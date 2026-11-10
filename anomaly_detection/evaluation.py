"""Offline evaluation; never used in the Spark hot path."""
from collections import Counter, defaultdict
import json
from pathlib import Path
from urllib.parse import unquote, urlparse
from .events import epoch_ms


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    index = (len(values) - 1) * q
    lower = int(index)
    return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (index - lower)


def read_scored(path):
    target = Path(path)
    if target.is_file():
        with target.open() as stream:
            for line in stream:
                row = json.loads(line)
                yield json.loads(row['payload']) if 'payload' in row else row
        return
    import pyarrow.parquet as pq
    metadata = target / '_spark_metadata'
    if not metadata.is_dir():
        raise ValueError('Expected a committed Spark file sink with _spark_metadata, or a JSONL file')
    logs = [p for p in metadata.iterdir() if p.name.split('.')[0].isdigit() and not p.name.startswith('.')]
    compact = [p for p in logs if p.name.endswith('.compact')]
    start = max((int(p.name.split('.')[0]) for p in compact), default=-1)
    logs = sorted([p for p in logs if (p.name == f'{start}.compact' or int(p.name.split('.')[0]) > start)],
                  key=lambda p: int(p.name.split('.')[0]))
    files = {}
    for log in logs:
        for line in log.read_text().splitlines()[1:]:
            entry = json.loads(line)
            if entry.get('action') == 'delete':
                files.pop(entry['path'], None)
            else:
                files[entry['path']] = entry
    for uri in sorted(files):
        parsed = urlparse(uri)
        if parsed.scheme not in ('', 'file'):
            raise ValueError('Offline reader currently supports local file sinks only')
        file = unquote(parsed.path) if parsed.scheme else uri
        for batch in pq.ParquetFile(file).iter_batches():
            for item in batch.to_pylist():
                row = json.loads(item['payload'])
                row['batch_started_ms'] = item['batch_started_ms']
                yield row


def window_metrics(rows, windows, detector):
    evaluated = [r for r in rows if r.get('record_status') == 'accepted']
    evaluated.sort(key=lambda r: epoch_ms(r['event_time']))
    if not evaluated:
        return dict(evaluated_records=0, eligible_windows=0, detected_windows=0,
                    missed_windows=0, false_alerts=0, window_recall=None,
                    false_alerts_per_1000=None, delay_median_seconds=None, delay_p95_seconds=None)
    start, end = epoch_ms(evaluated[0]['event_time']), epoch_ms(evaluated[-1]['event_time'])
    eligible = [(epoch_ms(a), epoch_ms(b)) for a, b in windows if start <= epoch_ms(a) <= epoch_ms(b) <= end]
    flags = [epoch_ms(r['event_time']) for r in evaluated if r.get(f'{detector}_anomaly') is True]
    delays = []
    for a, b in eligible:
        matching = [t for t in flags if a <= t <= b]
        if matching:
            delays.append((min(matching) - a) / 1000)
    # Alerts in boundary-overlapping windows are not false positives, but these
    # partial windows are excluded from recall/delay to avoid optimistic results.
    all_windows = [(epoch_ms(a), epoch_ms(b)) for a, b in windows]
    false = sum(not any(a <= t <= b for a, b in all_windows) for t in flags)
    return dict(evaluated_records=len(evaluated), eligible_windows=len(eligible),
                detected_windows=len(delays), missed_windows=len(eligible) - len(delays),
                false_alerts=false, window_recall=len(delays) / len(eligible) if eligible else None,
                false_alerts_per_1000=1000 * false / len(evaluated),
                delay_median_seconds=percentile(delays, .5), delay_p95_seconds=percentile(delays, .95),
                undecided_records=sum(r.get(f'{detector}_anomaly') is None for r in evaluated))


def evaluate(rows, windows):
    by_metric = defaultdict(list)
    statuses = Counter()
    reasons = Counter()
    seen = set()
    duplicate_ids = []
    for row in rows:
        statuses[row.get('record_status', 'unknown')] += 1
        if row.get('reason'):
            reasons[row['reason']] += 1
        if row.get('record_status') not in ('accepted', 'calibration'):
            continue
        identity = row['event_id']
        if identity in seen:
            duplicate_ids.append(identity)
        seen.add(identity)
        by_metric[row['metric_id']].append(row)
    results = {}
    for metric, records in sorted(by_metric.items()):
        source = records[0].get('source_metric_id', metric)
        if source not in windows:
            raise ValueError(f'Missing labels for {source}; no-window series must have an explicit empty list')
        results[metric] = {d: window_metrics(records, windows[source], d) for d in ('static', 'rolling')}
    totals = {}
    for detector in ('static', 'rolling'):
        count_keys = ['evaluated_records', 'eligible_windows', 'detected_windows', 'missed_windows', 'false_alerts']
        total = {k: sum(r[detector][k] for r in results.values()) for k in count_keys}
        total['window_recall'] = total['detected_windows'] / total['eligible_windows'] if total['eligible_windows'] else None
        total['false_alerts_per_1000'] = 1000 * total['false_alerts'] / total['evaluated_records'] if total['evaluated_records'] else None
        totals[detector] = total
    return dict(status_counts=dict(statuses), rejection_reasons=dict(reasons), duplicate_event_ids=duplicate_ids,
                series=results, aggregate=totals,
                policy='No cooldown. Recall includes only windows fully contained in the common post-calibration interval; undecided points remain in the denominator.')


def performance(rows, progress_path):
    progress = [json.loads(line) for line in Path(progress_path).read_text().splitlines() if line.strip()]
    completed = {p['batch_started_ms']: epoch_ms(p['observed_completed_at'])
                 for p in progress if 'batch_started_ms' in p}
    latencies = []
    alerts = []
    for row in rows:
        if row.get('record_status') != 'accepted':
            continue
        finished = completed.get(row.get('batch_started_ms'))
        if finished is not None:
            delay = finished - epoch_ms(row['sent_at'])
            latencies.append(delay)
            if row.get('is_anomaly'):
                alerts.append(delay)
    batches = [dict(batch_id=p['batchId'], input_rows=p['numInputRows'],
                    input_rows_per_second=p['inputRowsPerSecond'], processed_rows_per_second=p['processedRowsPerSecond'],
                    duration_ms=p.get('durationMs', {}).get('triggerExecution'), state_operators=p.get('stateOperators', []),
                    event_time=p.get('eventTime', {}), sources=p.get('sources', [])) for p in progress]
    return dict(latency_kind='driver-observed batch completion proxy; not sink visibility latency',
                matched_records=len(latencies), p50_ms=percentile(latencies, .5), p95_ms=percentile(latencies, .95),
                alert_p50_ms=percentile(alerts, .5), alert_p95_ms=percentile(alerts, .95),
                negative_latency_count=sum(x < 0 for x in latencies), batches=batches,
                listener_dropped_events=max((p.get('listener_dropped_events', 0) for p in progress), default=0))

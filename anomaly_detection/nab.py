"""Adapter to the unmodified upstream NAB Sweeper; no custom NAB approximation."""
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from .events import epoch_ms


def export_and_score(rows, windows, data_root, nab_root, output, threshold=3.0, profile='standard'):
    root = Path(nab_root).resolve()
    if not (root / 'nab/sweeper.py').is_file():
        raise ValueError('--nab-root must point to an upstream numenta/NAB checkout')
    sys.path.insert(0, str(root))
    from nab.sweeper import Sweeper
    profiles = json.loads((root / 'config/profiles.json').read_text())
    costs = profiles[profile]['CostMatrix']
    target = Path(output)
    target.mkdir(parents=True, exist_ok=True)
    groups = {}
    for row in rows:
        if row.get('record_status') in ('accepted', 'calibration'):
            if row.get('zscore_threshold', threshold) != threshold:
                raise ValueError('NAB export threshold must match the detector threshold')
            if '#replica=' in row['metric_id']:
                raise ValueError('NAB quality scoring requires original metrics, not workload replicas')
            groups.setdefault(row['metric_id'], []).append(row)
    totals = {d: dict(raw_score=0., null_score=0., perfect_score=0.) for d in ('static', 'rolling')}
    results = {}
    for metric, records in sorted(groups.items()):
        source = Path(data_root) / metric
        with source.open(newline='') as stream:
            original = list(csv.DictReader(stream))
        records.sort(key=lambda r: r['sequence'])
        if [r['sequence'] for r in records] != list(range(len(original))):
            raise ValueError(f'{metric}: NAB export requires each original observation exactly once, including calibration')
        if any(epoch_ms(r['event_time']) != epoch_ms(o['timestamp']) or float(r['value']) != float(o['value'])
               for r, o in zip(records, original)):
            raise ValueError(f'{metric}: records do not match original NAB series')
        timestamps = [datetime.fromtimestamp(epoch_ms(r['event_time']) / 1000, timezone.utc).replace(tzinfo=None) for r in records]
        boundaries = [(datetime.fromtimestamp(epoch_ms(a) / 1000, timezone.utc).replace(tzinfo=None),
                       datetime.fromtimestamp(epoch_ms(b) / 1000, timezone.utc).replace(tzinfo=None)) for a, b in windows[metric]]
        sweeper = Sweeper(probationPercent=.15, costMatrix=costs)
        probation = min(int(.15 * len(records)), .15 * 5000)
        boundaries = [(a, b) for a, b in boundaries if timestamps.index(a) >= probation]
        perfect = [float(any(t == a for a, _ in boundaries)) for t in timestamps]
        null = [0.] * len(timestamps)
        def score(values):
            _, result = sweeper.scoreDataSet(timestamps, values, boundaries, metric, .5)
            if result is None:
                raise RuntimeError('Upstream scorer returned no threshold result; check NAB revision')
            return float(result.score)
        null_score, perfect_score = score(null), score(perfect)
        results[metric] = {}
        for detector in ('static', 'rolling'):
            scores = []
            for row in records:
                z = row.get(f'{detector}_zscore')
                flag = row.get(f'{detector}_anomaly')
                if row['record_status'] == 'calibration' or flag is None:
                    value = 0.
                elif z is None:
                    value = 1. if flag else 0.
                else:
                    value = abs(z) / (abs(z) + threshold)
                scores.append(value)
            export = target / detector / metric
            export.parent.mkdir(parents=True, exist_ok=True)
            with export.open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['timestamp', 'value', 'anomaly_score'])
                writer.writerows((t.isoformat(sep=' '), r['value'], s) for t, r, s in zip(timestamps, records, scores))
            raw = score(scores)
            results[metric][detector] = raw
            totals[detector]['raw_score'] += raw
            totals[detector]['null_score'] += null_score
            totals[detector]['perfect_score'] += perfect_score
    for item in totals.values():
        denominator = item['perfect_score'] - item['null_score']
        item['normalized_subset_score'] = 100 * (item['raw_score'] - item['null_score']) / denominator if denominator else None
    revision = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], capture_output=True, text=True, check=True).stdout.strip()
    report = dict(profile=profile, upstream_revision=revision, series=results, aggregate=totals,
                  scope='Selected original series only; not a full NAB leaderboard score',
                  mapping='abs(z)/(abs(z)+threshold); decision threshold 0.5; constant-baseline flag maps to 0 or 1; calibration/warmup maps to 0',
                  probation='Upstream Sweeper 15% capped at 750 observations; windows beginning inside probation omitted; calibration suppression may be longer')
    (target / 'nab_scores.json').write_text(json.dumps(report, indent=2))
    return report

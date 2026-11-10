import csv
import hashlib
import heapq
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from .events import epoch_ms, iso_time


def metric_events(path, data_root, run_id, copies=1, expected_sha256=None):
    path = Path(path)
    if expected_sha256 and hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256:
        raise ValueError(f'{path}: CSV differs from calibration source')
    metric = path.resolve().relative_to(Path(data_root).resolve()).as_posix()
    with path.open(newline='', encoding='utf-8') as stream:
        previous = -1
        for index, row in enumerate(csv.DictReader(stream)):
            event_time = iso_time(row['timestamp'])
            current = epoch_ms(event_time)
            if current < previous:
                raise ValueError(f'{path}: timestamps are not sorted')
            previous = current
            for replica in range(copies):
                identity = metric if copies == 1 else f'{metric}#replica={replica}'
                yield dict(run_id=run_id, metric_id=identity, source_metric_id=metric,
                           event_id=f'{run_id}:{identity}:{index}', sequence=index,
                           event_time=event_time, value=float(row['value']), is_control=False)


def replay_events(files, data_root, run_id, copies=1, shuffle_block=1, seed=42, calibration=None):
    if copies < 1 or shuffle_block < 1:
        raise ValueError('copies and shuffle_block must be positive')
    streams = []
    for path in files:
        metric = Path(path).resolve().relative_to(Path(data_root).resolve()).as_posix()
        expected = calibration['metrics'][metric]['source_sha256'] if calibration else None
        streams.append(metric_events(path, data_root, run_id, copies, expected))
    merged = heapq.merge(*streams, key=lambda e: (e['event_time'], e['metric_id'], e['sequence']))
    rng = random.Random(seed)
    block = []
    for event in merged:
        block.append(event)
        if len(block) == shuffle_block:
            if shuffle_block > 1:
                rng.shuffle(block)
            yield from block
            block.clear()
    if shuffle_block > 1:
        rng.shuffle(block)
    yield from block


def control_events(metrics, run_id, maximum_time, advance_seconds):
    # A global advance beyond every final data timestamp allows a finite stream to drain.
    advance = datetime.fromtimestamp(epoch_ms(maximum_time) / 1000, timezone.utc) + timedelta(seconds=advance_seconds)
    for metric, source in sorted(metrics.items()):
        yield dict(run_id=run_id, metric_id=metric, source_metric_id=source,
                   event_id=f'{run_id}:{metric}:end', sequence=2**63 - 1,
                   event_time=advance.isoformat(), value=None, is_control=True)

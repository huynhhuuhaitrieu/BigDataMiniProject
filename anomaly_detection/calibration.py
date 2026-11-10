import csv
import hashlib
import json
import math
from pathlib import Path
from statistics import mean, pstdev


def calibrate(files, data_root, fraction=0.15, absolute_tolerance=0.01):
    if not 0 < fraction < 1:
        raise ValueError('calibration fraction must be between 0 and 1')
    if not math.isfinite(absolute_tolerance) or absolute_tolerance <= 0:
        raise ValueError('absolute tolerance must be finite and positive')
    metrics = {}
    root = Path(data_root).resolve()
    for file in files:
        path = Path(file).resolve()
        metric = path.relative_to(root).as_posix()
        raw = path.read_bytes()
        with path.open(newline='', encoding='utf-8') as stream:
            rows = list(csv.DictReader(stream))
        count = max(2, int(len(rows) * fraction))
        if count >= len(rows):
            raise ValueError(f'{metric}: need calibration and evaluation records')
        values = [float(row['value']) for row in rows[:count]]
        if not all(math.isfinite(x) for x in values):
            raise ValueError(f'{metric}: non-finite calibration value')
        entry = dict(mean=mean(values), stddev=pstdev(values), sample_count=count,
                     end_timestamp=rows[count - 1]['timestamp'], source_sha256=hashlib.sha256(raw).hexdigest(),
                     absolute_tolerance=absolute_tolerance)
        entry['calibration_id'] = hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()[:16]
        metrics[metric] = entry
    return {'schema_version': 1, 'fraction': fraction, 'metrics': metrics}


def load_calibration(path):
    artifact = json.loads(Path(path).read_text())
    if artifact['schema_version'] != 1 or not artifact['metrics']:
        raise ValueError('Unsupported or empty calibration artifact')
    for metric, entry in artifact['metrics'].items():
        if entry['sample_count'] < 2 or not all(math.isfinite(entry[k]) for k in ('mean', 'stddev', 'absolute_tolerance')):
            raise ValueError(f'Invalid calibration for {metric}')
        if entry['stddev'] < 0 or entry['absolute_tolerance'] <= 0:
            raise ValueError(f'Invalid calibration for {metric}')
        content = {key: value for key, value in entry.items() if key != 'calibration_id'}
        expected = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()[:16]
        if entry.get('calibration_id') != expected:
            raise ValueError(f'Calibration content changed for {metric}; create a new artifact')
    return artifact

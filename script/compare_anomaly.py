"""Compare event coverage and detector results after a restart or reordered replay."""
import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.evaluation import read_scored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', required=True)
    parser.add_argument('--actual', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    def collect(path):
        rows = [r for r in read_scored(path) if r.get('record_status') in ('accepted', 'calibration')]
        counts = Counter(r['event_id'] for r in rows)
        return {r['event_id']: r for r in rows}, [key for key, count in counts.items() if count != 1]
    expected, expected_duplicates = collect(args.reference)
    actual, actual_duplicates = collect(args.actual)
    mismatches = []
    for key in expected.keys() & actual.keys():
        a, b = expected[key], actual[key]
        for field in ('value', 'sequence', 'record_status', 'static_anomaly', 'rolling_anomaly', 'rolling_sample_count', 'static_zscore', 'rolling_zscore'):
            left, right = a.get(field), b.get(field)
            if field.endswith('zscore') and left is not None and right is not None:
                same = math.isclose(left, right, rel_tol=1e-8, abs_tol=1e-8)
            else:
                same = left == right
            if not same:
                mismatches.append(dict(event_id=key, field=field, reference=left, actual=right))
    result = dict(reference_records=len(expected), actual_records=len(actual),
                  missing=sorted(expected.keys() - actual.keys()), extra=sorted(actual.keys() - expected.keys()),
                  reference_duplicates=expected_duplicates, actual_duplicates=actual_duplicates, mismatches=mismatches)
    result['passed'] = not any(result[k] for k in ('missing', 'extra', 'reference_duplicates', 'actual_duplicates', 'mismatches'))
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k not in ('mismatches', 'missing', 'extra')}, indent=2))
    if not result['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

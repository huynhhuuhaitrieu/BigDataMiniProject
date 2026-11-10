"""Create immutable per-metric calibration artifacts from chronological prefixes."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.calibration import calibrate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--files', nargs='+', required=True)
    parser.add_argument('--data-root', default='data')
    parser.add_argument('--fraction', type=float, default=0.15)
    parser.add_argument('--absolute-tolerance', type=float, default=0.01)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    artifact = calibrate(args.files, args.data_root, args.fraction, args.absolute_tolerance)
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('x', encoding='utf-8') as stream:
        json.dump(artifact, stream, indent=2, allow_nan=False)
    print(f'Calibrated {len(artifact["metrics"])} metrics: {target}')


if __name__ == '__main__':
    main()

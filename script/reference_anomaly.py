"""Score replay JSONL incrementally as an offline reference (no Spark/Kafka)."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.calibration import load_calibration
from anomaly_detection.config import DetectorConfig
from anomaly_detection.engine import MetricEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--calibration', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--history-size', type=int, default=100)
    parser.add_argument('--min-periods', type=int, default=20)
    parser.add_argument('--threshold', type=float, default=3)
    args = parser.parse_args()
    config = DetectorConfig(history_size=args.history_size, min_periods=args.min_periods, threshold=args.threshold)
    baselines = load_calibration(args.calibration)['metrics']
    engines = {}
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    with Path(args.input).open() as source, target.open('x') as stream:
        for line in source:
            event = json.loads(line)
            key = event['metric_id']
            if key not in engines:
                engines[key] = MetricEngine(config, baselines)
            engine = engines[key]
            for row in engine.process([event]):
                stream.write(json.dumps(row, allow_nan=False) + '\n')
    print(target)


if __name__ == '__main__':
    main()

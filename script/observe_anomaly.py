"""Poll committed alerts to measure sink availability from the producer clock."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.evaluation import read_scored
from anomaly_detection.events import epoch_ms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--poll-seconds', type=float, default=1.)
    parser.add_argument('--duration-seconds', type=float, default=300.)
    args = parser.parse_args()
    if min(args.poll_seconds, args.duration_seconds) <= 0:
        parser.error('poll and duration must be positive')
    seen = set()
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with target.open('x') as stream:
        while time.monotonic() - started < args.duration_seconds:
            if (Path(args.input) / '_spark_metadata').is_dir():
                for row in read_scored(args.input):
                    if row.get('record_status') == 'accepted' and row.get('is_anomaly') and row['event_id'] not in seen:
                        observed = datetime.now(timezone.utc).isoformat()
                        latency = epoch_ms(observed) - epoch_ms(row['sent_at'])
                        stream.write(json.dumps(dict(event_id=row['event_id'], run_id=row['run_id'],
                            observed_at=observed, latency_ms=latency, poll_seconds=args.poll_seconds)) + '\n')
                        stream.flush()
                        seen.add(row['event_id'])
            time.sleep(args.poll_seconds)


if __name__ == '__main__':
    main()

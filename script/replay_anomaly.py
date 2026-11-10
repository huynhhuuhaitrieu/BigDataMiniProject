"""Replay calibrated NAB series to a dedicated Kafka topic or JSON Lines file."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.calibration import load_calibration
from anomaly_detection.replay import control_events, replay_events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--calibration', required=True)
    parser.add_argument('--data-root', default='data')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--topic', default='anomaly_input')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--rate', type=float, default=100, help='Total data records/s across metrics; 0 is unthrottled')
    parser.add_argument('--copies', type=int, default=1)
    parser.add_argument('--shuffle-block', type=int, default=1, help='Bounded block shuffle for disorder experiments')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--drain-seconds', type=int, default=601, help='Must exceed detector watermark delay')
    parser.add_argument('--json-output', help='Write JSONL instead of contacting Kafka; does not measure Kafka capacity')
    parser.add_argument('--manifest', required=True)
    args = parser.parse_args()
    if args.rate < 0 or args.drain_seconds <= 0:
        parser.error('rate must be nonnegative and drain-seconds positive')
    artifact = load_calibration(args.calibration)
    files = [Path(args.data_root) / key for key in artifact['metrics']]
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = dict(arguments=vars(args), calibration=artifact, python=sys.version, platform=platform.platform(),
                    cpu_count=os.cpu_count(), started_at=datetime.now(timezone.utc).isoformat())
    # Reserve the run manifest before sending anything; never overwrite an earlier run.
    with manifest_path.open('x') as stream:
        json.dump(manifest, stream, indent=2)
    producer = None
    output = None
    sent = 0
    started = time.monotonic()
    metrics = {}
    maximum_time = None
    def send(event):
        event['sent_at'] = datetime.now(timezone.utc).isoformat()
        if output:
            output.write(json.dumps(event, allow_nan=False) + '\n')
        else:
            producer.send(args.topic, key=event['metric_id'].encode(), value=event)

    try:
        if args.json_output:
            target = Path(args.json_output)
            target.parent.mkdir(parents=True, exist_ok=True)
            output = target.open('x')
        else:
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers.split(','),
                value_serializer=lambda e: json.dumps(e, allow_nan=False).encode(),
                acks='all', retries=3, max_in_flight_requests_per_connection=1)
        started = time.monotonic()
        for event in replay_events(files, args.data_root, args.run_id, args.copies, args.shuffle_block, args.seed, artifact):
            send(event)
            sent += 1
            metrics[event['metric_id']] = event['source_metric_id']
            maximum_time = max(maximum_time or event['event_time'], event['event_time'])
            if args.rate:
                time.sleep(max(0, sent / args.rate - (time.monotonic() - started)))
        if maximum_time:
            # Flush data acknowledgments before emitting global watermark control events.
            if producer:
                producer.flush(timeout=60)
            for event in control_events(metrics, args.run_id, maximum_time, args.drain_seconds):
                send(event)
        if producer:
            producer.flush(timeout=60)
        manifest['status'] = 'completed'
    except BaseException as error:
        manifest.update(status='failed', error=str(error))
        raise
    finally:
        elapsed = time.monotonic() - started
        manifest.update(data_records=sent, elapsed_seconds=elapsed, achieved_records_per_second=sent / elapsed if elapsed else 0,
                        finished_at=datetime.now(timezone.utc).isoformat())
        manifest_path.write_text(json.dumps(manifest, indent=2))
        if output:
            output.close()
        if producer:
            producer.close(timeout=10)
    print(f'Sent {sent} data records; manifest: {manifest_path}')


if __name__ == '__main__':
    main()

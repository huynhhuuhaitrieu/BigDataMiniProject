import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FILE = REPO_ROOT / "data" / "realKnownCause" / "machine_temperature_system_failure.csv"


def parse_args():
    parser = argparse.ArgumentParser(description="Send NAB CSV rows to Kafka.")
    parser.add_argument("--file", type=Path, default=DEFAULT_FILE, help="NAB CSV path")
    parser.add_argument("--topic", default="nab_raw", help="Kafka topic")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to wait between messages; 0 sends as fast as possible",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.delay < 0:
        raise ValueError("--delay must be zero or greater")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda message: json.dumps(message).encode("utf-8"),
    )

    sent = 0
    try:
        with args.file.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            if not reader.fieldnames or not {"timestamp", "value"}.issubset(reader.fieldnames):
                raise ValueError("CSV must contain 'timestamp' and 'value' columns")

            for row in reader:
                message = {
                    "timestamp": row["timestamp"],
                    "value": float(row["value"]),
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.send(args.topic, key=message["timestamp"].encode("utf-8"), value=message)
                sent += 1
                if args.delay:
                    time.sleep(args.delay)

        producer.flush()
        print(f"Sent {sent} messages to topic '{args.topic}' from {args.file}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

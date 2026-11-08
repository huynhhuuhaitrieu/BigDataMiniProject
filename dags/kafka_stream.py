import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


DEFAULT_NAB_FILE = "/opt/airflow/data/realKnownCause/machine_temperature_system_failure.csv"


def stream_nab(**context):
    run_conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    file_path = Path(run_conf.get("file", os.environ.get("NAB_FILE", DEFAULT_NAB_FILE)))
    topic = run_conf.get("topic", os.environ.get("NAB_TOPIC", "nab_raw"))
    delay = float(run_conf.get("delay_seconds", os.environ.get("NAB_DELAY_SECONDS", "0")))
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092").split(",")

    if delay < 0:
        raise ValueError("delay_seconds must be zero or greater")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda message: json.dumps(message).encode("utf-8"),
    )
    sent = 0
    try:
        with file_path.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            if not reader.fieldnames or not {"timestamp", "value"}.issubset(reader.fieldnames):
                raise ValueError("NAB CSV must contain 'timestamp' and 'value' columns")

            for row in reader:
                message = {
                    "timestamp": row["timestamp"],
                    "value": float(row["value"]),
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.send(topic, key=message["timestamp"].encode("utf-8"), value=message)
                sent += 1
                if delay:
                    time.sleep(delay)

        producer.flush()
        logging.info("Sent %s NAB records from %s to Kafka topic %s", sent, file_path, topic)
    finally:
        producer.close()


with DAG(
    dag_id="nab_streaming",
    start_date=datetime(2026, 11, 7),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="send_nab_to_kafka",
        python_callable=stream_nab,
    )

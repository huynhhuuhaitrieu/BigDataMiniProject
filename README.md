# NAB Kafka-Spark Streaming

This guide explains how to run the NAB streaming pipeline on macOS, Linux, or WSL2. The default pipeline is:

```text
NAB CSV -> Kafka topic `nab_raw` -> Spark Structured Streaming -> `output/nab/*.json`
```

You can send data with the standalone Python producer or trigger the Airflow DAG. Use only one producer method per test run.

## 1. Prepare the environment

Open a terminal in the repository directory.

If the virtual environment or dependencies are not installed:

```bash
uv python install 3.12
uv sync --python 3.12
```

The project pins a supported Python range and installs both PySpark and `kafka-python` from `pyproject.toml`.

On macOS, install Java 17 if needed, then select it in the current terminal before running Spark:

```bash
brew install openjdk@17
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Confirm Java prints version 17. PySpark 3.5.1 uses Hadoop libraries that fail during startup on newer Java versions.

## 2. Start the Docker services

```bash
docker compose up -d zookeeper broker
```

Wait until `broker` is healthy with `docker compose ps`.

Create the Kafka topic. This command is safe to run more than once:

```bash
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists --topic nab_raw --partitions 1 --replication-factor 1
```

## 3. Start the NAB consumer (Terminal 1)

Create local output and checkpoint directories:

```bash
mkdir -p output/nab checkpoints/nab
```

Run the PySpark driver in local mode from the same host as the broker. This avoids requiring Docker Spark workers to connect back to a host driver. Set `SPARK_DRIVER_HOST` only when using a remote Spark cluster.

```bash
export SPARK_MASTER_URL=local[*]
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export NAB_TOPIC=nab_raw
export NAB_OUTPUT_PATH="$PWD/output/nab"
export NAB_CHECKPOINT_PATH="$PWD/checkpoints/nab"
.venv/bin/python spark_stream.py
```

Keep this terminal open while the stream runs; press `Ctrl+C` to stop the Spark application. The Spark application UI is usually at [http://localhost:4040](http://localhost:4040); Spark may select another port if 4040 is already occupied.

**Start Spark before the producer.** Spark continuously watches Kafka and writes each batch it reads as JSON Lines files in `output/nab`. If you already sent records, this script is configured with `startingOffsets=earliest`, so a new checkpoint can read retained records from the beginning.

Clients running on the host use `localhost:9092`; clients in Docker use `broker:29092`.

## 4. Send NAB data to Kafka

### Option A: Run the standalone producer

With Spark still running in Terminal 1, open a second terminal (Terminal 2) and send the CSV:

```bash
.venv/bin/python script/stream_nab.py --file data/realKnownCause/machine_temperature_system_failure.csv --topic nab_raw --delay 0.1 --bootstrap-servers localhost:9092
```

Use `--delay 0` to send as fast as possible, or `--delay 1` to wait one second between messages. When it finishes, the producer prints the number of messages sent. The CSV must contain `timestamp` and `value` columns.

### Option B: Send data through Airflow

Open [http://localhost:8080](http://localhost:8080), sign in with `admin` / `admin`, select the `nab_streaming` DAG, click **Trigger DAG**, and enter this JSON configuration:

```json
{
  "file": "/opt/airflow/data/realKnownCause/machine_temperature_system_failure.csv",
  "delay_seconds": 0.1,
  "topic": "nab_raw"
}
```

The DAG runs once per trigger. The repository's `data/` directory is mounted in the container at `/opt/airflow/data`. Check the Airflow task log to confirm that `send_nab_to_kafka` succeeded.

## 5. Verify the data

### Check messages in Kafka

The following command reads up to five messages from the beginning of the topic, then exits:

```bash
docker compose exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic nab_raw --from-beginning --max-messages 5
```

Each message should be JSON containing `timestamp`, `value`, and `sent_at`.

Inspect the topic:

```bash
docker compose exec broker kafka-topics --bootstrap-server broker:29092 --describe --topic nab_raw
```

### Check Spark output

```bash
find output/nab -maxdepth 1 -type f -name 'part-*.json' -print
wc -l output/nab/part-*.json
head -n 5 output/nab/part-*.json
```

The output uses JSON Lines format, with one record per line. Spark may create multiple `part-*.json` files because each micro-batch appends a new file. `_spark_metadata` and `.crc` files are normal Spark metadata and checksums.

If the producer prints a message count and the Kafka consumer can read JSON messages, Kafka has received the data. If Spark is running and those records appear in `output/nab`, the Kafka-to-Spark-to-file pipeline is working.

## 6. Local URLs

- Airflow UI: [http://localhost:8080](http://localhost:8080)
- Spark application UI: [http://localhost:4040](http://localhost:4040), available while the PySpark driver is running. Spark may use the next port if 4040 is occupied.
- Kafka Control Center (optional): start it with `docker compose up -d schema-registry control-center`, then open [http://localhost:9021](http://localhost:9021).
- Spark Master UI: [http://localhost:9090](http://localhost:9090), available while `spark-master` is running.

Kafka has no web UI enabled by default. Use the `kafka-topics` and `kafka-console-consumer` commands above to inspect it.

## 7. Stop the services

Stop the Spark application with `Ctrl+C`, then stop the Docker containers:

```bash
docker compose stop
```

## Restart notes

Spark uses a checkpoint to remember Kafka offsets. Keep the same `NAB_CHECKPOINT_PATH` to continue from the previous stream position. To replay from the beginning, use a separate topic, checkpoint, and output directory for the new test. Do not remove a checkpoint while Spark is running.

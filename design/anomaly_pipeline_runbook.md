# Running the enhanced anomaly pipeline

This is a separate application. The existing `spark_stream.py`, `script/stream_nab.py`, Airflow DAG, and standalone `detection.py` continue to serve their original purposes.

Run commands from the repository root. Generated run files go under `anomaly_runs/`, which ignores generated contents in Git. Choose a new run directory and Kafka topic for each independent experiment. Use the same directory/topic only to resume an interrupted run.

## 1. Create a supported environment

Use Python 3.11 or 3.12 and Java 17. The current repository virtual environment may use another Python version, so create a separate one:

```bash
uv venv --python 3.11 .venv-anomaly
uv pip install --python .venv-anomaly/bin/python -r requirements-anomaly.txt
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/.venv-anomaly/bin/python"
```

The Java selection above is for macOS/Homebrew; on Linux select your installed Java 17 path. Spark workers require the same Python environment and compatible dependency versions. The entry point distributes the new Python package to executors as a ZIP archive.

## 2. Calibrate the selected metrics

Start with CPU, a no-window CPU series, and a network metric:

```bash
.venv-anomaly/bin/python script/calibrate_anomaly.py \
  --files data/realAWSCloudwatch/ec2_cpu_utilization_24ae8d.csv \
          data/realAWSCloudwatch/ec2_cpu_utilization_c6585a.csv \
          data/realAWSCloudwatch/ec2_network_in_257a54.csv \
  --fraction 0.15 \
  --absolute-tolerance 0.01 \
  --output anomaly_runs/cloudwatch-001/calibration.json
```

The artifact records each file's hash, prefix length, mean, standard deviation, absolute tolerance and calibration identity. Existing artifacts are never overwritten. The prefix initializes rolling history and is excluded from ordinary window-quality metrics. It is assumed representative; it is not certified anomaly-free. Select tolerances and thresholds on development data before final evaluation. To use different absolute tolerances for different metrics, calibrate those files separately and combine the `metrics` dictionaries into one artifact before starting a run; retain each entry's provenance and do not edit statistics after evaluation.

## 3. Run locally without Kafka

This path verifies scoring and Spark state before starting services. It reads finite JSONL files through Spark Structured Streaming; it is not a Kafka capacity benchmark.

```bash
mkdir -p anomaly_runs/cloudwatch-001/input
.venv-anomaly/bin/python script/replay_anomaly.py \
  --calibration anomaly_runs/cloudwatch-001/calibration.json \
  --run-id cloudwatch-001 --rate 0 \
  --json-output anomaly_runs/cloudwatch-001/input/events.json \
  --manifest anomaly_runs/cloudwatch-001/replay-manifest.json

.venv-anomaly/bin/python script/reference_anomaly.py \
  --input anomaly_runs/cloudwatch-001/input/events.json \
  --calibration anomaly_runs/cloudwatch-001/calibration.json \
  --output anomaly_runs/cloudwatch-001/reference.jsonl

.venv-anomaly/bin/python script/run_detection.py \
  --calibration anomaly_runs/cloudwatch-001/calibration.json \
  --json-input anomaly_runs/cloudwatch-001/input \
  --output anomaly_runs/cloudwatch-001/scores \
  --checkpoint anomaly_runs/cloudwatch-001/checkpoint \
  --progress anomaly_runs/cloudwatch-001/progress.jsonl \
  --available-now

.venv-anomaly/bin/python script/compare_anomaly.py \
  --reference anomaly_runs/cloudwatch-001/reference.jsonl \
  --actual anomaly_runs/cloudwatch-001/scores \
  --output anomaly_runs/cloudwatch-001/parity.json
```

`--available-now` drains available source files and terminates. Without it, Spark stays running until Ctrl+C. To exercise multiple batches with file input, place several JSONL files in the input directory; `--max-files-per-trigger 1` is the default. Keep files immutable after Spark discovers them.

The score sink is Parquet with `run_id`, `metric_id`, `event_id`, `record_status`, `batch_started_ms`, and a `payload` JSON string containing the full detector result. `script/evaluate_anomaly.py` decodes that payload and reads only files listed in Spark's commit metadata. Each record includes independent static/rolling flags, prior rolling statistics, and reason fields. Calibration and warmup decisions are explicit. A flat-baseline jump uses absolute tolerance and a null Z-score.

## 4. Run through Kafka

Unpause Docker Desktop if it is paused, then start the existing broker services:

```bash
docker compose up -d zookeeper broker
docker compose exec broker kafka-topics --bootstrap-server broker:29092 \
  --create --if-not-exists --topic anomaly_input_001 --partitions 4 --replication-factor 1
```

In terminal 1, start the separate detector:

```bash
.venv-anomaly/bin/python script/run_detection.py \
  --calibration anomaly_runs/cloudwatch-001/calibration.json \
  --topic anomaly_input_001 --bootstrap-servers localhost:9092 \
  --master 'local[2]' --shuffle-partitions 4 --max-offsets 5000 \
  --trigger-seconds 1 --history-size 100 --min-periods 20 --threshold 3 \
  --output anomaly_runs/kafka-001/scores \
  --checkpoint anomaly_runs/kafka-001/checkpoint \
  --progress anomaly_runs/kafka-001/progress.jsonl
```

In terminal 2, replay the calibrated files:

```bash
.venv-anomaly/bin/python script/replay_anomaly.py \
  --calibration anomaly_runs/cloudwatch-001/calibration.json \
  --run-id kafka-001 --topic anomaly_input_001 \
  --rate 100 --manifest anomaly_runs/kafka-001/replay-manifest.json
```

The rate is the total target across all selected metrics. `--rate 0` sends as quickly as acknowledgments/backpressure allow. Messages are keyed by metric, use acknowledgments from all in-sync replicas, and have one in-flight request per producer connection to preserve ordering during retries. Application-level sequence checks also reject duplicate or stale observations. The manifest records achieved producer rate; a target rate alone is not measured capacity.

Spark remains running after replay finishes. Wait for all data and control records to commit before stopping it with Ctrl+C. The replay manifest reports the count of data records; control records are additional. Spark's output status counts and Kafka consumer lag help confirm completion.

## 5. Evaluate and create charts

```bash
.venv-anomaly/bin/python script/evaluate_anomaly.py \
  --input anomaly_runs/cloudwatch-001/scores \
  --progress anomaly_runs/cloudwatch-001/progress.jsonl \
  --output anomaly_runs/cloudwatch-001/report
```

Outputs include `metrics.json`, `quality.png`, and `streaming.png` when progress is available. Window recall counts only windows fully contained in the common post-calibration interval. Alerts inside boundary-overlapping labeled windows are not false alerts. There is no alert cooldown. False alerts per 1,000 observations are not a false-positive rate over normal observations. A no-window series has null recall, not zero recall.

Quality evaluation loads committed results into driver memory; this offline reporting utility targets the NAB-sized experiments, not unbounded streams. Performance results include Spark progress, state memory, Kafka offsets when supplied by Spark, and a driver-observed batch completion latency proxy. This proxy maps output `batch_started_ms` to progress through the checkpoint offset log's persisted batch timestamp, and can include callback delay. File replay generated before Spark starts includes the intentional waiting time in latency; do not present it as a live transport result.

For official NAB scoring, obtain the public upstream source outside this repository:

```bash
git clone https://github.com/numenta/NAB.git /tmp/nab-scoring
.venv-anomaly/bin/python script/evaluate_anomaly.py \
  --input anomaly_runs/cloudwatch-001/scores \
  --progress anomaly_runs/cloudwatch-001/progress.jsonl \
  --nab-root /tmp/nab-scoring \
  --output anomaly_runs/cloudwatch-001/official-report
```

This invokes upstream `nab.sweeper.Sweeper` with the standard profile and records its Git revision. It exports continuous scores, raw scores, a null baseline, and a perfect baseline computed for the same selected series. The resulting normalized subset score must not be presented as a full NAB leaderboard score. NAB's probation rules differ from a simple uncapped 15% calibration prefix; the report records both. Export requires all original observations exactly once, including calibration, and rejects workload replicas. Use `--threshold` here only if the detector used a different threshold; the default is 3 in both commands.

## 6. Recovery and delayed events

For a recovery demonstration, stop the detector while replay is running, then run the exact same detector command with its existing checkpoint and output directory. Kafka must retain the unprocessed records. Do not restart the producer from the beginning as a substitute for resuming Spark. The detector refuses incompatible checkpoint configuration/calibration changes.

To compare a completed resumed run with an uninterrupted run, use `compare_anomaly.py`. The event IDs must match, so feed the same recorded replay JSONL/run identity to two separate file-input experiments, one uninterrupted and one interrupted. The comparison checks coverage, duplicates, flags and numerical scores with a documented numeric tolerance. Automated integration tests already cover a clean stop/restart with new input files; a process-kill crash experiment is a separate check.

Ordered mode is the default and suits chronological replay, even if selected metrics span different historical ranges. Spark sorts all input chunks per metric within a batch and rejects stale observations arriving after later data has already been scored.

For bounded disorder, use a fresh experiment and restrict calibration to series with overlapping timestamp ranges (one series is sufficient initially):

```text
Detector additions: --mode buffered --lateness-seconds 600 --max-pending 10000
Producer additions: --shuffle-block 4 --seed 42 --drain-seconds 601
```

The producer sends final tagged control events to advance event time beyond the last data point plus allowed lateness. Spark releases pending events in timestamp order after the global watermark passes them, including through timeout callbacks with no new input. Control events never enter detector history or quality metrics. Increasing lateness increases buffering/alert delay. An idle stream cannot promise a wall-clock flush. Historical series with disjoint ranges must not be combined in buffered mode because one global watermark can make an older series late.

Use a pending cap large enough for the arrival rate and allowed disorder. Overflow is explicitly rejected and counted. Records Spark drops before the state function are visible through engine progress counters and input audit, not application rejection output. The raw Kafka topic is the input audit source. For a fixed NAB experiment total state is bounded by the finite metric set; the application does not implement automatic eviction for unlimited metric identities.

## 7. Performance experiments

Run independent topics/checkpoints and save a manifest per configuration. Start with the proposal's matrix: rates 100/1,000/5,000 records per second; 1/2/4 local cores; 1/4/8 topic partitions; triggers 1/5 seconds; and increasing metric copies. Use three repetitions and a declared warmup/measurement interval. Record actual hardware and active workloads.

`replay_anomaly.py --copies 10` creates ten independent metric identities per original series. This increases processing workload without adding independent anomaly-quality evidence. Static calibration remains tied to each replica's original source metric. A single hot metric still requires sequential state updates, so extra partitions alone cannot parallelize it.

Use Kafka lag over time together with processed rate to identify sustainable loads. Spark reports the offsets available for each trigger in progress JSON; sum per-partition lag against broker log-end offsets for the same observation time. A finite drained batch's instantaneous processed rate is not sustainable throughput. Repeat long enough to observe lag growth. Multi-core local mode demonstrates local parallelism; a multi-host claim additionally requires shared output/checkpoint storage and a verified cluster deployment. This entry point currently uses local/shared filesystem paths, not HDFS/S3 URIs.

To measure first observed alert availability, start the observer before replay:

```bash
.venv-anomaly/bin/python script/observe_anomaly.py \
  --input anomaly_runs/kafka-001/scores \
  --output anomaly_runs/kafka-001/alert-visibility.jsonl \
  --poll-seconds 1 --duration-seconds 300
```

The observer polls committed files and records its first sight of each alert. Polling adds up to an interval plus file-reading time; clock synchronization is required across hosts. This utility rescans committed files and is intended for finite demo experiments, not a production notification service.

## 8. Verification commands

```bash
.venv-anomaly/bin/python -m unittest discover -s tests -v
RUN_SPARK_TESTS=1 .venv-anomaly/bin/python -m unittest discover -s tests -v
```

The ordinary tests cover numerical stability, eviction, flat baselines, per-metric separation, serialization/restart, duplicate/late/invalid input rejection, and window evaluation. The opt-in Spark tests exercise real Arrow serialization, committed file output, restart without duplicate outputs, and event-time timeout draining. Local Spark needs permission to bind localhost sockets.

See `design/results/anomaly_validation.md` for the validation performed during implementation and its limitations.

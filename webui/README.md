# Demo website

A small local website for following the anomaly pipeline step by step and showing that each feature behaves as designed. It is a thin layer over the existing code: every score it shows comes from `anomaly_detection/` or from the scripts in `script/`. It adds no dependencies; it needs the `.venv-anomaly` environment from the [runbook](../design/anomaly_pipeline_runbook.md).

## Start

From the repository root:

```bash
.venv-anomaly/bin/python webui/server.py
```

Open <http://127.0.0.1:8790>. Use `--port` (or the `PORT` variable) to pick another port. The server binds to localhost only. Stop it with `Ctrl+C`.

Spark steps need Java 17. The server finds it from `JAVA_HOME` (if it points to a 17 JDK), `/usr/libexec/java_home -v 17`, Homebrew `openjdk@17`, or `/usr/lib/jvm/*17*`, and passes it to the jobs it starts. The header shows which prerequisites were found, including whether Docker is running.

## Pages

| Page | What it shows | Feature demonstrated |
| --- | --- | --- |
| **Pipeline runs** | Each folder in `anomaly_runs/` as a row of stages: Calibrate → (Kafka topic) → Replay → Offline reference → Spark detect → Parity → Evaluate. Each stage shows its output files and their current content: calibration statistics, replay manifest, checkpoint offsets/commits, committed Parquet files, last micro-batch progress, parity result, and quality report. | The whole data flow, and where each artifact comes from |
| **Scored series** | One metric of a run: value, static and rolling z-scores, alerts, NAB labelled windows, calibration prefix. Hover a point for its baseline (prior mean, standard deviation, sample count). Same Vietnamese call-outs and summary as the playground. | Static vs rolling detection; alerts against ground-truth windows |
| **Detector playground** | Runs `MetricEngine` in-process on any NAB file with editable parameters. Also replays the file in micro-batches with a JSON state round trip and compares that with a single pass. Numbered call-outs on the chart, a parameter guide, a column guide and a plain-language summary (in Vietnamese) explain how to read the result. | Parameter effects; state surviving batch boundaries |
| **Correctness checks** | Ten scenarios with hand-made events and the rows the engine emitted. Buttons run `tests/` (and the opt-in Spark tests). | Calibration prefix, warmup, score-before-update, flat baseline, rejections, ordering, isolation, checkpoint state, watermark buffering, checkpoint contract |

On both chart pages, the **Ẩn chú thích / Hiện chú thích** button hides or shows the call-outs and guides without reloading data. The choice is remembered per page in this browser.

## Running a pipeline from the page

1. Open **New run**, give it a name, keep **JSONL files (no Kafka)**, choose datasets (three CloudWatch series are preselected) and detector settings.
2. Press **Run** on each stage in order. A stage is enabled only once its inputs exist. The job log appears under the stages and is also saved to `anomaly_runs/<run>/webui/<step>.log`.
3. When **Spark detect** is done, the **Parity** stage should report `PASSED` (Spark output equals the offline reference). **Evaluate** then shows window recall, false alerts and the generated charts.

The steps start exactly the commands from the runbook, with `--rate 0` replay into `input/events.json` and `--available-now` detection. The files they write are never overwritten; to repeat an experiment, create a new run.

**Kafka mode** requires the broker from `docker compose up -d zookeeper broker`. The extra **Kafka topic** stage creates a dedicated topic. **Spark detect** then runs continuously; start it, then start **Replay** (paced by the chosen records/s rate), watch batches arrive in the stage details, and press **Stop** once all records are committed. The offline reference and parity stages do not apply to Kafka runs, because the replay goes to the topic instead of a file.

Job history is kept in server memory. Stage status is read from the files on disk, so it survives a server restart, and runs created from the command line appear too (read-only, since the page does not know their original settings).

## Suggested demo script (about 10 minutes)

1. **Correctness checks**: open *Score before update*, *Flat baseline* and *Buffered mode + watermark*, and explain the rows.
2. **Pipeline runs**: create `demo-001` and run all six stages (Spark takes roughly 20–40 s). Point to checkpoint offsets/commits and the Parity result.
3. **Scored series**: open `ec2_cpu_utilization_24ae8d.csv`. Both detectors hit the two labelled windows, while rolling raises more alerts outside them.
4. **Detector playground**: change the threshold from 3 to 4, or history N from 100 to 300, and compare false alerts. Then try `ec2_cpu_utilization_c6585a.csv`, which has no labelled windows, so every alert there is false.
5. Return to **Evaluate** for the aggregate table and the limits in the [validation report](../design/results/anomaly_validation.md).

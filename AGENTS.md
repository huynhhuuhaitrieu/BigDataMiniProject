# Project context for AI coding assistants

This file is for any AI assistant working in this repository: Claude Code (through `CLAUDE.md`), OpenAI Codex or ChatGPT, Cursor, and similar tools. Read it before changing code or reports.

## What this project is

This is a CO5135 Big Data mini-project by group 261_05: **Scalable Real-Time Anomaly Detection on Operational Time-Series Data Using Apache Spark Structured Streaming**.

- **Goal** (from slides `design/brief.pdf`, pages 2–3): run NAB CSV → Python replay → Kafka → Spark Structured Streaming → anomaly alerts, with **Static** and **Rolling Z-score** detectors.
- **Evaluation targets:**
  - NAB score and false-alarm rate
  - throughput and alert latency
  - scalability across input rates and Spark configurations
- **Dataset scope:** the whole NAB `realAWSCloudwatch` subset, 17 files in `data/realAWSCloudwatch/`. The brief says "including CPU utilization", so CPU is one example, not a limit. Labelled anomaly windows are in `labels/combined_windows.json`.
- **Status:** all 20 targets are implemented and verified with measured evidence in `assess/README.md`, which is written in Vietnamese.

The team speaks Vietnamese. Answer in the language the user writes in.

## Repository map

| Path | What it is | Notes |
| --- | --- | --- |
| `anomaly_detection/` | **Main detector package.** `engine.py` (`MetricEngine`, pure Python, also used inside Spark), `statistics.py` (`RollingHistory`), `calibration.py`, `stateful.py` (Spark `applyInPandasWithState`), `evaluation.py`, `nab.py` (adapter to upstream NAB scorer), `replay.py`, `events.py`, `config.py`, `progress.py` | Core logic. Changing it affects every result. |
| `script/` | Entry points: `calibrate_anomaly.py`, `replay_anomaly.py`, `run_detection.py`, `reference_anomaly.py` (sequential reference), `compare_anomaly.py` (parity), `evaluate_anomaly.py`, `observe_anomaly.py`; legacy `stream_nab.py`, `entrypoint.sh` | All CLI flags are documented in the runbook |
| `tests/` | `test_anomaly_detection.py` (7 unit tests), `test_spark_anomaly.py` (2 real Spark tests, run only with `RUN_SPARK_TESTS=1`) | Must stay 9/9 passing |
| `webui/` | Local demo website, stdlib `ThreadingHTTPServer` and hand-drawn SVG, no extra dependencies. Port 8790 (`PORT` overrides) | UI text is partly Vietnamese |
| `assess/` | `run_assessment.py` (evidence collector), `README.md` (Vietnamese completion report), `results/` (JSON and PNG) | See "Assessment" below |
| `design/` | `brief.pdf` (slides = targets), `anomaly_detection_enhancement_proposal.md`, `anomaly_pipeline_runbook.md`, `results/anomaly_validation.md` (dated 10 Nov 2026, with a 15 Nov follow-up section) | |
| `spark_stream.py`, `dags/`, `detection.py`, `script/stream_nab.py` | **Original pipeline** (README sections 1–8): Kafka topic `nab_raw` → JSON files, an Airflow DAG, and a standalone CSV detector | Leave unchanged unless asked |
| `anomaly_runs/` | Raw run folders (git-ignored). `anomaly_runs/assess/` holds assessment runs and the NAB scorer clone `nab-upstream/` | Safe to delete and regenerate |
| `output/`, `checkpoints/` | Output of the original pipeline | |

## Environment (the usual cause of failures)

- **Two virtualenvs.** `.venv` (Python 3.12, `pyproject.toml`, `uv sync`) is for the original pipeline. **`.venv-anomaly`** (Python 3.11, `requirements-anomaly.txt`) is for everything under `anomaly_detection/`, `script/*_anomaly.py`, `webui/` and `assess/`. Use `.venv-anomaly/bin/python` for these.
- **Java 17 is required.** PySpark 3.5.1 fails at startup on newer JDKs. The machine's default is JDK 23. On macOS, `/usr/libexec/java_home -v 17` can silently return 23, so use the Homebrew path:
  ```bash
  export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
  export PATH="$JAVA_HOME/bin:$PATH"
  ```
  `webui/server.py` and `assess/run_assessment.py` already find Java 17 by themselves.
- **Kafka:** `docker compose up -d zookeeper broker` starts Confluent 7.4 at `localhost:9092`; inside the containers the address is `broker:29092`. If the broker exits with ZooKeeper `NodeExistsException`, wait a few seconds and run `docker compose up -d broker` again.
- The Spark Kafka connector jar comes through `--packages` and is cached in `~/.ivy2`. The first run needs internet access.

## Common commands

```bash
.venv-anomaly/bin/python -m unittest discover -s tests -v                      # 7 unit tests, 2 skipped
RUN_SPARK_TESTS=1 .venv-anomaly/bin/python -m unittest discover -s tests -v    # all 9, needs Java 17
.venv-anomaly/bin/python webui/server.py                                       # http://127.0.0.1:8790
.venv-anomaly/bin/python assess/run_assessment.py tests quality nab tuning sensitivity
.venv-anomaly/bin/python assess/run_assessment.py live recovery capacity       # needs Docker, about 30 min
.venv-anomaly/bin/python assess/run_assessment.py charts
```

Pipeline commands step by step (calibrate → replay → detect → reference → compare → evaluate) are in `design/anomaly_pipeline_runbook.md`.

## Detector rules that must not break

- **Static Z-score:** per-metric mean and standard deviation from the first 15% of each series (calibration). Calibration points are not scored.
- **Rolling Z-score:** compares each value with the **previous** N values (default N=100, `min_periods`=20, threshold |z| ≥ 3). The current value is scored *before* it enters the history. A flat history (stddev ≈ 0) uses an absolute tolerance (default 0.01) instead of dividing by zero.
- **Parity:** Spark output must match `script/reference_anomaly.py` record by record, with 0 missing, 0 extra, 0 duplicate and 0 mismatched records (z tolerance 1e-8). `compare_anomaly.py` checks this. Any engine change must keep parity and the 9 tests green.
- **State:** per-metric state is stored in the Spark checkpoint. The checkpoint also stores a config/calibration contract (`checkpoint/anomaly-contract.json`); reusing a checkpoint with a different config is rejected on purpose.
- Duplicate, late, invalid and unknown-metric events are rejected and must not change the history. Buffered mode reorders events with a watermark.
- **Defaults stay at threshold 3, N=100.** Tuning chose threshold 6 and N=50 (`assess/results/tuning.json`), but every other measurement uses the defaults. Do not change the defaults without re-running the whole assessment.

## Assessment and reporting conventions

- `assess/README.md` is **Vietnamese**, written for non-experts. Lead with a short conclusion and use checklist tables with ✅ and evidence. Numbers use Vietnamese formatting: `.` for thousands and `,` for decimals, for example 12.843 bản ghi/s and 1,1 s.
- Every number in it must come from `assess/results/*.json` produced by `assess/run_assessment.py`. Do not hand-edit numbers; re-run the stage, then update the text.
- The team decided **not to mention** these topics in assessment reports: alerts being written only to files or the lack of a separate alert channel, deployment on a multi-machine cluster, and rolling median/MAD. Treat them as out of scope.
- Key measured results as of 15 Nov 2026, so they can be quoted without re-running:
  - Parity: 67,740/67,740 records (17 series, file source), 36,288/36,288 (Kafka), 12,096/12,096 after a SIGKILL crash and restart.
  - Quality with defaults on 17 series: static recall 26/30 with 91.5 false alerts per 1,000; rolling recall 28/30 with 14.6 per 1,000.
  - Official NAB score on 17 series (upstream `ea702d7`, standard profile): static −806.9, rolling −58.0. Tuned rolling (threshold 6, N=50) gets −1.1 on the held-out 8 series and +15.9 on all 17.
  - Live Kafka latency p50 (median of 3 runs): 0.99 s at 1k/s, 1.01 s at 5k/s, 1.11 s at 10k/s.
  - Backlog drain: 8,623 records/s with 1 core, 13,005 with 4 cores, 13,067 with 8 cores. Kafka partitions 1/4/8 make little difference.
- NAB scores are subset scores (17 series). Do not compare them with the full 58-file NAB leaderboard.

## Editing conventions

- `README.md` has mixed CRLF/LF line endings. Keep the existing endings when editing, or the diff will show every line as changed.
- Keep `webui/` dependency-free (stdlib and vanilla JS) and its design plain and utilitarian.
- Commit messages follow `type(scope): summary`, for example `feat(detection): ...` or `feat(webui): ...`. Work goes on feature branches named like `feat/<name>/<topic>` and is merged by PR into `main`.
- Do not commit `anomaly_runs/`, `.venv*`, checkpoints or output folders.

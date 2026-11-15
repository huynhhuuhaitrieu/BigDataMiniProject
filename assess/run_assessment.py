"""Collect evidence for the project assessment (assess/README.md).

Each stage runs the project's own scripts and saves a small JSON summary in
assess/results/. Raw run folders go to anomaly_runs/assess/ (git-ignored) and
are recreated on every run of a stage.

    .venv-anomaly/bin/python assess/run_assessment.py tests quality nab tuning sensitivity
    .venv-anomaly/bin/python assess/run_assessment.py live capacity recovery   # need Docker
    .venv-anomaly/bin/python assess/run_assessment.py charts
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from anomaly_detection.calibration import calibrate  # noqa: E402
from anomaly_detection.config import DetectorConfig  # noqa: E402
from anomaly_detection.engine import MetricEngine  # noqa: E402
from anomaly_detection.evaluation import evaluate, percentile, read_scored  # noqa: E402
from anomaly_detection.events import epoch_ms  # noqa: E402
from anomaly_detection.replay import metric_events  # noqa: E402

PY = sys.executable
RESULTS = ROOT / 'assess' / 'results'
RUNS = ROOT / 'anomaly_runs' / 'assess'
DATA = ROOT / 'data'
CLOUDWATCH = sorted(p.relative_to(ROOT).as_posix() for p in (DATA / 'realAWSCloudwatch').glob('*.csv'))
THREE = ['data/realAWSCloudwatch/ec2_cpu_utilization_24ae8d.csv', 'data/realAWSCloudwatch/ec2_cpu_utilization_c6585a.csv',
         'data/realAWSCloudwatch/ec2_network_in_257a54.csv']
WINDOWS = json.loads((ROOT / 'labels/combined_windows.json').read_text())
NAB_URL = 'https://github.com/numenta/NAB.git'
NAB_REVISION = 'ea702d75cc2258d9d7dd35ca8e5e2539d71f3140'  # same revision as design/results/
REPEATS = 3
# Tuning/test split for parameter selection: alternate files in name order, so both halves mix metric types.
TUNE = CLOUDWATCH[0::2]
TEST = CLOUDWATCH[1::2]


# ------------------------------------------------------------------ helpers

def java17():
    home = os.environ.get('JAVA_HOME', '')
    if '17' in home:
        return home
    for prefix in ('/opt/homebrew/opt/openjdk@17', '/usr/local/opt/openjdk@17'):
        candidate = Path(prefix) / 'libexec/openjdk.jdk/Contents/Home'
        if candidate.is_dir():
            return str(candidate)
    found = sorted(Path('/usr/lib/jvm').glob('*17*')) if Path('/usr/lib/jvm').is_dir() else []
    if found:
        return str(found[0])
    raise SystemExit('Java 17 not found; set JAVA_HOME')


def env():
    home = java17()
    return dict(os.environ, JAVA_HOME=home, PATH=f'{home}/bin:' + os.environ['PATH'],
                PYSPARK_PYTHON=PY, PYTHONUNBUFFERED='1')


def log(message):
    print(f'[{datetime.now():%H:%M:%S}] {message}', flush=True)


def run(argv, logfile, timeout=1800, extra_env=None):
    logfile.parent.mkdir(parents=True, exist_ok=True)
    with logfile.open('a') as stream:
        stream.write('$ ' + ' '.join(map(str, argv)) + '\n')
        stream.flush()
        result = subprocess.run(list(map(str, argv)), cwd=ROOT, env=dict(env(), **(extra_env or {})),
                                stdout=stream, stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{argv[1] if len(argv) > 1 else argv[0]} failed; see {logfile.relative_to(ROOT)}')


def start(argv, logfile):
    logfile.parent.mkdir(parents=True, exist_ok=True)
    stream = logfile.open('a')
    stream.write('$ ' + ' '.join(map(str, argv)) + '\n')
    stream.flush()
    # Own process group so a crash test can kill the Python driver and its JVM together.
    return subprocess.Popen(list(map(str, argv)), cwd=ROOT, env=env(), stdout=stream, stderr=subprocess.STDOUT,
                            start_new_session=True)


def stop(process, timeout=90):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def fresh(name):
    target = RUNS / name
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


def save(name, value):
    RESULTS.mkdir(parents=True, exist_ok=True)
    value = dict(value, generated_at=datetime.now(timezone.utc).isoformat(timespec='seconds'))
    (RESULTS / f'{name}.json').write_text(json.dumps(value, indent=2, allow_nan=False, default=str))
    log(f'saved assess/results/{name}.json')


def progress(path):
    if not Path(path).exists():
        return []
    rows = []
    for line in Path(path).read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            pass  # a crash can leave one partial line
    return rows


def numbered(directory):
    return sorted(int(p.name) for p in Path(directory).iterdir() if p.name.isdigit()) if Path(directory).is_dir() else []


def detector_args(calibration, topic, out, master='local[2]', partitions=4, max_offsets=5000, trigger=1):
    return [PY, 'script/run_detection.py', '--calibration', calibration, '--topic', topic,
            '--bootstrap-servers', 'localhost:9092', '--master', master, '--shuffle-partitions', partitions,
            '--max-offsets', max_offsets, '--trigger-seconds', trigger,
            '--output', out / 'scores', '--checkpoint', out / 'checkpoint', '--progress', out / 'progress.jsonl']


def consumed(out):
    return sum(p.get('numInputRows', 0) for p in progress(out / 'progress.jsonl'))


def wait_consumed(out, total, process, timeout):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if process.poll() is not None:
            raise RuntimeError(f'detector exited early; see {out.relative_to(ROOT)}/detector.log')
        ckpt = out / 'checkpoint'
        if consumed(out) >= total and numbered(ckpt / 'commits') == numbered(ckpt / 'offsets'):
            return time.monotonic()
        time.sleep(0.5)
    raise TimeoutError(f'only {consumed(out)} of {total} records consumed')


def lag_series(rows):
    """Records Kafka still held after each batch: latest offset minus end offset, summed over partitions."""
    series = []
    for p in rows:
        source = (p.get('sources') or [{}])[0]
        latest, end = source.get('latestOffset'), source.get('endOffset')
        if isinstance(latest, str):
            latest, end = json.loads(latest), json.loads(end)
        if isinstance(latest, dict) and isinstance(end, dict):
            lag = sum(int(v) - int(end.get(topic, {}).get(part, 0))
                      for topic, parts in latest.items() for part, v in parts.items())
            series.append(dict(batch=p['batchId'], lag=lag, rows=p.get('numInputRows', 0),
                               at=p.get('timestamp'), ms=(p.get('durationMs') or {}).get('triggerExecution')))
    return series


# ------------------------------------------------------------------- broker

def compose(*args, timeout=180):
    return subprocess.run(['docker', 'compose', *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout)


def ensure_broker():
    if subprocess.run(['docker', 'info'], capture_output=True).returncode:
        raise SystemExit('Docker is not running')
    compose('up', '-d', 'zookeeper', 'broker', timeout=600)
    for _ in range(90):
        if compose('exec', '-T', 'broker', 'kafka-topics', '--bootstrap-server', 'broker:29092', '--list').returncode == 0:
            return
        time.sleep(2)
    raise SystemExit('Kafka broker did not become ready')


def create_topic(name, partitions):
    result = compose('exec', '-T', 'broker', 'kafka-topics', '--bootstrap-server', 'broker:29092', '--create',
                     '--topic', name, '--partitions', str(partitions), '--replication-factor', '1')
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)


def topic_name(prefix):
    return f'{prefix}_{datetime.now():%Y%m%d_%H%M%S}'


def shared_calibration(name, files):
    """Calibration for a fixed file list, created once and reused by Kafka stages."""
    path = RUNS / 'calibration' / f'{name}.json'
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        run([PY, 'script/calibrate_anomaly.py', '--files', *files, '--output', path], path.with_suffix('.log'))
    return path


def nab_root():
    """Upstream NAB scorer, pinned. Uses $NAB_ROOT if set, else a clone under anomaly_runs/assess/."""
    root = Path(os.environ.get('NAB_ROOT') or RUNS / 'nab-upstream')
    if not (root / 'nab/sweeper.py').is_file():
        log(f'cloning {NAB_URL} into {root}')
        subprocess.run(['git', 'clone', '-q', NAB_URL, str(root)], check=True, timeout=600)
    subprocess.run(['git', '-C', str(root), 'checkout', '-q', NAB_REVISION], check=True)
    return root


def nab_score(rows, threshold, out):
    from anomaly_detection.nab import export_and_score
    return export_and_score(rows, WINDOWS, DATA, nab_root(), out, threshold)


def median_run(runs, keys):
    """Median of each numeric key over repeated runs, plus the per-run values."""
    merged = dict(runs[0])
    for key in keys:
        values = [r[key] for r in runs if r.get(key) is not None]
        merged[key] = statistics.median(values) if values else None
        merged[f'{key}_runs'] = [r.get(key) for r in runs]
    return merged


def replay(calibration, run_id, out, topic=None, rate=0, copies=1, json_output=None, name='replay'):
    argv = [PY, 'script/replay_anomaly.py', '--calibration', calibration, '--run-id', run_id,
            '--rate', rate, '--copies', copies, '--manifest', out / f'{name}-manifest.json']
    argv += ['--json-output', json_output] if json_output else ['--topic', topic]
    run(argv, out / f'{name}.log', timeout=3600)
    return json.loads((out / f'{name}-manifest.json').read_text())


# ------------------------------------------------------------------- stages

def stage_tests():
    out = fresh('tests')
    result = subprocess.run([PY, '-m', 'unittest', 'discover', '-s', 'tests', '-v'], cwd=ROOT,
                            env=dict(env(), RUN_SPARK_TESTS='1'), capture_output=True, text=True, timeout=1800)
    (out / 'tests.log').write_text(result.stdout + result.stderr)
    text = result.stderr
    # Spark log lines can sit between a test name and its status, so read each test's whole segment.
    heads = list(re.finditer(r'^(test_\w+) \((\S+)\)', text, re.M))
    tests = []
    for i, head in enumerate(heads):
        segment = text[head.end():heads[i + 1].start() if i + 1 < len(heads) else len(text)]
        status = re.search(r'(?:\.\.\. |^)(ok|FAIL|ERROR|skipped)\b', segment, re.M)
        tests.append((head.group(1), head.group(2), status.group(1) if status else 'unknown'))
    ran = re.search(r'Ran (\d+) tests', text)
    save('tests', dict(passed=result.returncode == 0, ran=int(ran.group(1)) if ran else None,
                       tests=[dict(name=n, suite=s.split('.')[0], status=st) for n, s, st in tests],
                       summary=text.strip().splitlines()[-1] if text.strip() else ''))


def stage_quality():
    """All 17 CloudWatch series through Spark (file source), checked against the offline reference."""
    out = fresh('quality')
    run([PY, 'script/calibrate_anomaly.py', '--files', *CLOUDWATCH, '--output', out / 'calibration.json'], out / 'steps.log')
    replay(out / 'calibration.json', 'assess-quality', out, json_output=out / 'input/events.json')
    run([PY, 'script/reference_anomaly.py', '--input', out / 'input/events.json', '--calibration', out / 'calibration.json',
         '--output', out / 'reference.jsonl'], out / 'steps.log')
    started = time.monotonic()
    run([PY, 'script/run_detection.py', '--calibration', out / 'calibration.json', '--json-input', out / 'input',
         '--master', 'local[4]', '--output', out / 'scores', '--checkpoint', out / 'checkpoint',
         '--progress', out / 'progress.jsonl', '--available-now'], out / 'steps.log')
    spark_seconds = time.monotonic() - started
    run([PY, 'script/compare_anomaly.py', '--reference', out / 'reference.jsonl', '--actual', out / 'scores',
         '--output', out / 'parity.json'], out / 'steps.log')
    run([PY, 'script/evaluate_anomaly.py', '--input', out / 'scores', '--output', out / 'report'], out / 'steps.log')
    metrics = json.loads((out / 'report/metrics.json').read_text())
    parity = json.loads((out / 'parity.json').read_text())
    save('quality', dict(files=len(CLOUDWATCH), spark_wall_seconds=round(spark_seconds, 1),
                         parity={k: (len(v) if isinstance(v, list) else v) for k, v in parity.items()},
                         status_counts=metrics['status_counts'], aggregate=metrics['aggregate'], series=metrics['series'],
                         policy=metrics['policy']))


def cloudwatch_events():
    artifact = calibrate([ROOT / f for f in CLOUDWATCH], DATA)
    events = {}
    for f in CLOUDWATCH:
        metric = Path(f).relative_to('data').as_posix()
        events[metric] = [dict(e, sent_at='2026-01-01T00:00:00+00:00') for e in metric_events(ROOT / f, DATA, 'sweep')]
    return artifact, events


def stage_nab():
    """Official NAB scorer (upstream Sweeper, standard profile) on the Spark output of all 17 series."""
    out = RUNS / 'quality'
    if not (out / 'scores').is_dir():
        raise SystemExit('run the quality stage first')
    run([PY, 'script/evaluate_anomaly.py', '--input', out / 'scores', '--nab-root', nab_root(),
         '--output', out / 'official-report'], out / 'steps.log')
    report = json.loads((out / 'official-report/nab/nab_scores.json').read_text())
    save('nab', dict(files=len(report['series']), **{k: report[k] for k in
                     ('profile', 'upstream_revision', 'aggregate', 'series', 'scope', 'mapping', 'probation')}))


def stage_tuning():
    """Pick threshold / history on the tuning half by official NAB score, freeze it, report the untouched test half."""
    out = fresh('tuning')
    artifact, events = cloudwatch_events()
    split = dict(tune=[Path(f).relative_to('data').as_posix() for f in TUNE],
                 test=[Path(f).relative_to('data').as_posix() for f in TEST])
    split['all'] = split['tune'] + split['test']

    def measure(rows, threshold, name):
        result = {}
        for part, metrics in split.items():
            chosen = [r for r in rows if r['metric_id'] in metrics]
            quality = evaluate(chosen, WINDOWS)['aggregate']
            nab = nab_score(chosen, threshold, out / name / part)['aggregate']
            result[part] = {d: dict(nab=nab[d]['normalized_subset_score'], recall=quality[d]['window_recall'],
                                    detected=quality[d]['detected_windows'], windows=quality[d]['eligible_windows'],
                                    false_alerts=quality[d]['false_alerts'],
                                    false_alerts_per_1000=quality[d]['false_alerts_per_1000'])
                            for d in ('static', 'rolling')}
        return result

    grid = []
    for threshold in (3, 4, 5, 6, 8, 10, 15):
        for history in (50, 100, 300):
            config = DetectorConfig(history_size=history, min_periods=20, threshold=threshold)
            rows = []
            for items in events.values():
                rows.extend(MetricEngine(config, artifact['metrics']).process(items))
            grid.append(dict(threshold=threshold, history=history, **measure(rows, threshold, f't{threshold}-n{history}')))
            log(f'tuning threshold={threshold} history={history}: tune NAB rolling '
                f'{grid[-1]["tune"]["rolling"]["nab"]:.1f} static {grid[-1]["tune"]["static"]["nab"]:.1f}')
    # Static ignores history, so its choice is over thresholds only (history 100 row).
    best_static = max((g for g in grid if g['history'] == 100), key=lambda g: g['tune']['static']['nab'])
    best_rolling = max(grid, key=lambda g: g['tune']['rolling']['nab'])
    default = next(g for g in grid if g['threshold'] == 3 and g['history'] == 100)
    save('tuning', dict(
        rule='Choose the configuration with the highest official NAB score (standard profile) on the tuning half only; '
             'then report it once on the test half, which played no part in the choice.',
        split={k: [m.split('/')[-1] for m in v] for k, v in split.items() if k != 'all'},
        windows={part: default[part]['rolling']['windows'] for part in ('tune', 'test', 'all')},
        default=dict(threshold=3, history=100),
        chosen=dict(static=dict(threshold=best_static['threshold']),
                    rolling=dict(threshold=best_rolling['threshold'], history=best_rolling['history'])),
        compare={detector: dict(default={p: default[p][detector] for p in ('tune', 'test', 'all')},
                                chosen={p: best[p][detector] for p in ('tune', 'test', 'all')})
                 for detector, best in (('static', best_static), ('rolling', best_rolling))},
        grid=grid, min_periods=20, calibration_fraction=0.15))


def stage_sensitivity():
    """Offline sweep with the same engine: how threshold and history length trade misses for false alerts."""
    artifact, events = cloudwatch_events()
    grid = []
    for threshold in (2.5, 3, 4, 5):
        for history in (50, 100, 300):
            config = DetectorConfig(history_size=history, min_periods=20, threshold=threshold)
            rows = []
            for metric, items in events.items():
                rows.extend(MetricEngine(config, artifact['metrics']).process(items))
            agg = evaluate(rows, WINDOWS)['aggregate']
            grid.append(dict(threshold=threshold, history=history,
                             **{f'{d}_{k}': agg[d][k] for d in ('static', 'rolling')
                                for k in ('detected_windows', 'eligible_windows', 'window_recall', 'false_alerts', 'false_alerts_per_1000')}))
            log(f'sweep threshold={threshold} history={history} done')
    save('sensitivity', dict(files=len(CLOUDWATCH), min_periods=20, calibration_fraction=0.15, grid=grid))


def live_once(calibration, rate, copies, rep):
    name = f'live-{rate}-r{rep}'
    out = fresh(name)
    topic = topic_name(f'assess_live_{rate}_r{rep}')
    create_topic(topic, 4)
    detector = start(detector_args(calibration, topic, out, master='local[4]', partitions=4, max_offsets=50000),
                     out / 'detector.log')
    observer = start([PY, 'script/observe_anomaly.py', '--input', out / 'scores', '--output', out / 'alert-visibility.jsonl',
                      '--poll-seconds', '1', '--duration-seconds', '900'], out / 'observer.log')
    time.sleep(20)  # Spark start-up (JVM, Kafka package) before load starts
    log(f'live rate={rate} run {rep}: replaying {copies} copies')
    replay_started = time.monotonic()
    manifest = replay(calibration, f'assess-{name}', out, topic=topic, rate=rate, copies=copies)
    replay_finished = time.monotonic()
    total = manifest['data_records'] + 3 * copies
    try:
        done = wait_consumed(out, total, detector, timeout=900)
    finally:
        time.sleep(3)
        stop(observer, 10)
        stop(detector)
    run([PY, 'script/evaluate_anomaly.py', '--input', out / 'scores', '--progress', out / 'progress.jsonl',
         '--output', out / 'report'], out / 'steps.log')
    perf = json.loads((out / 'report/metrics.json').read_text())['performance']
    rows = progress(out / 'progress.jsonl')
    lags = lag_series(rows)
    busy = [p for p in rows if p.get('numInputRows')]
    visible = [json.loads(l)['latency_ms'] for l in (out / 'alert-visibility.jsonl').read_text().splitlines() if l.strip()] \
        if (out / 'alert-visibility.jsonl').exists() else []
    load = [l['lag'] for l in lags if l['rows']]
    log(f'live rate={rate} run {rep}: p50={perf["p50_ms"]} ms p95={perf["p95_ms"]} ms')
    return dict(
        target_rate=rate, copies=copies, metrics=3 * copies, data_records=manifest['data_records'],
        achieved_rate=round(manifest['achieved_records_per_second']), replay_seconds=round(replay_finished - replay_started, 1),
        drain_after_replay_seconds=round(done - replay_finished, 1),
        processed_rows_per_second_median=round(statistics.median(p['processedRowsPerSecond'] for p in busy)) if busy else None,
        batches=len(busy), latency_p50_ms=perf['p50_ms'], latency_p95_ms=perf['p95_ms'],
        alert_latency_p50_ms=perf['alert_p50_ms'], alert_latency_p95_ms=perf['alert_p95_ms'],
        visible_alerts=len(visible), visibility_p50_ms=percentile(visible, .5), visibility_p95_ms=percentile(visible, .95),
        max_lag=max(load, default=0), lag=lags, latency_kind=perf['latency_kind'])


LIVE_KEYS = ('achieved_rate', 'drain_after_replay_seconds', 'processed_rows_per_second_median', 'latency_p50_ms',
             'latency_p95_ms', 'alert_latency_p50_ms', 'alert_latency_p95_ms', 'visibility_p50_ms', 'visibility_p95_ms', 'max_lag')


def stage_live():
    """Kafka end to end at fixed producer rates, REPEATS times each: latency, lag and parity with the offline reference."""
    ensure_broker()
    calibration = shared_calibration('cloudwatch3', THREE)
    results = []
    for rate, copies in ((1000, 3), (5000, 12), (10000, 25)):
        runs = [live_once(calibration, rate, copies, rep) for rep in range(1, REPEATS + 1)]
        merged = median_run(runs, LIVE_KEYS)
        # Keep the batch-time trace of the run whose p50 latency is the median one.
        merged['lag'] = sorted(runs, key=lambda r: r['latency_p50_ms'])[len(runs) // 2]['lag']
        results.append(merged)
    # Parity: one 1000/s run against an offline reference of the same events (same run_id, same event IDs).
    out = RUNS / 'live-1000-r1'
    replay(calibration, 'assess-live-1000-r1', out, json_output=out / 'reference-input.json', copies=3, name='reference-replay')
    run([PY, 'script/reference_anomaly.py', '--input', out / 'reference-input.json', '--calibration', calibration,
         '--output', out / 'reference.jsonl'], out / 'steps.log')
    run([PY, 'script/compare_anomaly.py', '--reference', out / 'reference.jsonl', '--actual', out / 'scores',
         '--output', out / 'parity.json'], out / 'steps.log')
    parity = json.loads((out / 'parity.json').read_text())
    save('live', dict(runs=results, repeats=REPEATS,
                      parity_1000={k: (len(v) if isinstance(v, list) else v) for k, v in parity.items()},
                      setup='3 CloudWatch series x copies, Kafka topic with 4 partitions, local[4], 4 shuffle partitions, '
                            f'trigger 1 s, maxOffsetsPerTrigger 50000; each rate run {REPEATS} times, median reported'))


def stage_capacity():
    """Backlog drain: how fast Spark processes a full topic with different cores and partition counts."""
    ensure_broker()
    calibration = shared_calibration('cloudwatch3', THREE)
    copies = 10
    topics = {}
    out_root = fresh('capacity')
    for partitions in (1, 4, 8):
        topics[partitions] = topic_name(f'assess_cap_p{partitions}')
        create_topic(topics[partitions], partitions)
        manifest = replay(calibration, f'assess-cap-p{partitions}', out_root, topic=topics[partitions], rate=0,
                          copies=copies, name=f'preload-p{partitions}')
        log(f'preloaded {manifest["data_records"]} records into {partitions} partition(s)')
    total = manifest['data_records'] + 3 * copies
    results = []
    for cores, partitions in ((1, 4), (2, 4), (4, 4), (8, 4), (4, 1), (4, 8)):
        runs = []
        for rep in range(1, REPEATS + 1):
            out = out_root / f'cores{cores}-p{partitions}-r{rep}'
            out.mkdir()
            started = time.monotonic()
            # Each run has its own checkpoint, so it reads the same preloaded topic from the start.
            detector = start(detector_args(calibration, topics[partitions], out, master=f'local[{cores}]',
                                           partitions=max(cores, partitions), max_offsets=20000), out / 'detector.log')
            try:
                done = wait_consumed(out, total, detector, timeout=1200)
            finally:
                stop(detector)
            rows = [p for p in progress(out / 'progress.jsonl') if p.get('numInputRows')]
            first = min(datetime.fromisoformat(p['timestamp'].replace('Z', '+00:00')) for p in rows)
            last_done = max(datetime.fromisoformat(p['observed_completed_at']) for p in rows)
            processing = (last_done - first).total_seconds()
            runs.append(dict(cores=cores, kafka_partitions=partitions, records=total, metrics=3 * copies,
                             batches=len(rows), processing_seconds=round(processing, 1),
                             records_per_second=round(total / processing) if processing else None,
                             startup_plus_processing_seconds=round(done - started, 1),
                             state_memory_bytes=max(sum(o.get('memoryUsedBytes', 0) for o in p.get('stateOperators', []))
                                                    for p in rows)))
            log(f'capacity cores={cores} partitions={partitions} run {rep}: {runs[-1]["records_per_second"]} records/s')
        results.append(median_run(runs, ('records_per_second', 'processing_seconds', 'startup_plus_processing_seconds',
                                         'state_memory_bytes')))
    save('capacity', dict(runs=results, repeats=REPEATS, machine=dict(cpu_count=os.cpu_count(), platform=platform.platform()),
                          note=f'Topic preloaded unthrottled before Spark starts; maxOffsetsPerTrigger 20000; trigger 1 s; '
                               f'each configuration run {REPEATS} times, median reported. '
                               'Measures how fast Spark empties a full topic on this machine.'))


def stage_recovery():
    """Crash the detector mid-stream (SIGKILL of driver and JVM), restart, and check for missing/duplicate output."""
    ensure_broker()
    calibration = shared_calibration('cloudwatch3', THREE)
    out = fresh('recovery')
    topic = topic_name('assess_recovery')
    create_topic(topic, 4)
    run_id = 'assess-recovery'
    manifest = replay(calibration, run_id, out, topic=topic, rate=0)
    replay(calibration, run_id, out, json_output=out / 'reference-input.json', name='reference-replay')
    run([PY, 'script/reference_anomaly.py', '--input', out / 'reference-input.json', '--calibration', calibration,
         '--output', out / 'reference.jsonl'], out / 'steps.log')
    total = manifest['data_records'] + 3
    argv = detector_args(calibration, topic, out, max_offsets=400)
    detector = start(argv, out / 'detector.log')
    while len(numbered(out / 'checkpoint/commits')) < 8:
        if detector.poll() is not None:
            raise RuntimeError('detector exited before the crash point')
        time.sleep(0.2)
    time.sleep(0.4)  # land inside a later batch, not exactly at a boundary
    commits_before = len(numbered(out / 'checkpoint/commits'))
    offsets_before = len(numbered(out / 'checkpoint/offsets'))
    os.killpg(detector.pid, signal.SIGKILL)
    detector.wait()
    killed_at = time.monotonic()
    log(f'recovery: killed after {commits_before} committed batches ({offsets_before} planned)')
    detector = start(argv, out / 'detector.log')
    while len(numbered(out / 'checkpoint/commits')) <= commits_before:
        if detector.poll() is not None:
            raise RuntimeError('restarted detector exited')
        time.sleep(0.2)
    first_new_commit = time.monotonic() - killed_at
    try:
        wait_consumed(out, total, detector, timeout=900)
    finally:
        stop(detector)
    run([PY, 'script/compare_anomaly.py', '--reference', out / 'reference.jsonl', '--actual', out / 'scores',
         '--output', out / 'parity.json'], out / 'steps.log')
    parity = json.loads((out / 'parity.json').read_text())
    save('recovery', dict(records=total, killed_after_committed_batches=commits_before, planned_batches_at_kill=offsets_before,
                          seconds_from_kill_to_first_new_commit=round(first_new_commit, 1),
                          total_batches=len(numbered(out / 'checkpoint/commits')),
                          parity={k: (len(v) if isinstance(v, list) else v) for k, v in parity.items()},
                          method='SIGKILL to the detector process group (Python driver and JVM) during the stream, '
                                 'then the identical command with the same checkpoint and output.'))


# ------------------------------------------------------------------- charts

def stage_charts():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
    orange, blue, grey = '#d9480f', '#1864ab', '#868e96'

    def load(name):
        path = RESULTS / f'{name}.json'
        return json.loads(path.read_text()) if path.exists() else None

    def spread(runs, key, scale=1):
        """Error bars from the per-run values that median_run() keeps next to the median."""
        if not all(f'{key}_runs' in r for r in runs):
            return None
        return [[(r[key] - min(r[f'{key}_runs'])) / scale for r in runs], [(max(r[f'{key}_runs']) - r[key]) / scale for r in runs]]

    tuning = load('tuning')
    if tuning:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.3))
        for history, style in ((50, ':'), (100, '-'), (300, '--')):
            rows = sorted((g for g in tuning['grid'] if g['history'] == history), key=lambda g: g['threshold'])
            axes[0].plot([g['threshold'] for g in rows], [g['tune']['rolling']['nab'] for g in rows], 's' + style, color=blue,
                         label=f'rolling N={history}')
        chosen = tuning['chosen']['rolling']
        axes[0].annotate('được chọn', (chosen['threshold'], tuning['compare']['rolling']['chosen']['tune']['nab']),
                         textcoords='offset points', xytext=(25, 8), fontsize=9, color=blue,
                         arrowprops=dict(arrowstyle='->', color=blue))
        axes[0].axhline(0, color=grey, linewidth=1)
        axes[0].set_xlabel('Ngưỡng |z|')
        axes[0].set_ylabel('Điểm NAB trên tập chỉnh')
        axes[0].set_title('Bước 1: chọn tham số trên tập chỉnh (9 series)')
        axes[0].legend()
        labels = ['Điểm NAB', 'Recall × 100', 'Báo nhầm / 1.000']
        test = tuning['compare']['rolling']
        values = lambda m: [m['nab'], 100 * m['recall'], m['false_alerts_per_1000']]
        x = range(len(labels))
        for shift, key, color, label in ((-.2, 'default', grey, 'mặc định (ngưỡng 3, N=100)'),
                                         (.2, 'chosen', blue, f'đã chọn (ngưỡng {chosen["threshold"]:g}, N={chosen["history"]})')):
            vals = values(test[key]['test'])
            axes[1].bar([i + shift for i in x], vals, .38, color=color, label=label)
            for i, v in zip(x, vals):
                axes[1].text(i + shift, v, f'{v:.1f}'.replace('.', ','), ha='center', va='bottom' if v >= 0 else 'top', fontsize=8)
        axes[1].axhline(0, color=grey, linewidth=1)
        axes[1].set_xticks(list(x), labels)
        axes[1].set_title('Bước 2: kiểm trên tập đánh giá chưa dùng để chọn (8 series)')
        axes[1].legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(RESULTS / 'tuning.png', dpi=150)
        plt.close(fig)

    quality = load('quality')
    if quality:
        series = quality['series']
        names = [m.split('/')[-1].replace('.csv', '') for m in series]
        fig, axes = plt.subplots(1, 2, figsize=(12, 5.5), sharey=True)
        ypos = range(len(names))
        for axis, key, title in ((axes[0], 'false_alerts_per_1000', 'Báo nhầm / 1.000 điểm (thấp hơn là tốt)'),
                                 (axes[1], 'window_recall', 'Recall: tỷ lệ sự cố bắt được (cao hơn là tốt)')):
            for offset, det, color in ((-.2, 'static', orange), (.2, 'rolling', blue)):
                values = [series[m][det][key] for m in series]
                axis.barh([y + offset for y in ypos], [v if v is not None else 0 for v in values], height=.38,
                          color=color, label=det)
                for y, v in zip(ypos, values):
                    if v is None:
                        axis.text(0.01, y + offset, 'không có sự cố', va='center', fontsize=7, color=grey)
                    elif v == 0 and key == 'window_recall':
                        axis.text(0.01, y + offset, '0', va='center', fontsize=7, color=color)
            axis.set_title(title)
        axes[0].set_yticks(list(ypos), names, fontsize=8)
        axes[0].invert_yaxis()
        axes[0].legend(loc='center right')
        fig.tight_layout()
        fig.savefig(RESULTS / 'quality_by_series.png', dpi=150)
        plt.close(fig)

    sweep = load('sensitivity')
    if sweep:
        fig, axis = plt.subplots(figsize=(7, 4.5))
        static = {g['threshold']: g for g in sweep['grid'] if g['history'] == 100}
        xs = sorted(static)
        axis.plot([static[t]['static_false_alerts_per_1000'] for t in xs], [static[t]['static_window_recall'] for t in xs],
                  'o-', color=orange, label='static')
        for t in xs:
            axis.annotate(f'ngưỡng {t}', (static[t]['static_false_alerts_per_1000'], static[t]['static_window_recall']),
                          textcoords='offset points', xytext=(6, -10), fontsize=8, color=orange)
        for history, style in ((50, ':'), (100, '-'), (300, '--')):
            rows = sorted((g for g in sweep['grid'] if g['history'] == history), key=lambda g: g['threshold'])
            axis.plot([g['rolling_false_alerts_per_1000'] for g in rows], [g['rolling_window_recall'] for g in rows],
                      's' + style, color=blue, label=f'rolling N={history}')
        axis.set_xlabel('Báo nhầm / 1.000 điểm (trái là tốt)')
        axis.set_ylabel('Recall (trên là tốt)')
        axis.set_title('Đánh đổi: bắt được nhiều sự cố hơn ↔ báo nhầm nhiều hơn')
        axis.legend()
        fig.tight_layout()
        fig.savefig(RESULTS / 'sensitivity_tradeoff.png', dpi=150)
        plt.close(fig)

    live = load('live')
    if live:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
        def rate_label(r):
            return f"{r['target_rate']:,}/s".replace(',', '.') if r['achieved_rate'] >= .98 * r['target_rate'] else \
                f"đặt {r['target_rate']:,} → đạt {r['achieved_rate']:,}/s".replace(',', '.')
        for r, color in zip(live['runs'], (blue, orange, '#2b8a3e')):
            batches = [l for l in r['lag'] if l['rows'] and l['ms']]
            axes[0].plot(range(len(batches)), [l['ms'] / 1000 for l in batches], color=color, label=rate_label(r))
        axes[0].axhline(1, color=grey, linestyle='--', linewidth=1)
        axes[0].text(0, 1.05, 'chu kỳ trigger 1 giây', color=grey, fontsize=8)
        axes[0].set_title('Thời gian xử lý mỗi batch (trên 1 giây = không theo kịp)')
        axes[0].set_xlabel('Batch')
        axes[0].set_ylabel('Giây')
        axes[0].legend(title='Tốc độ gửi')
        labels = [rate_label(r) for r in live['runs']]
        x = range(len(labels))
        for shift, key, color, label in ((-.2, 'latency_p50_ms', blue, 'p50'), (.2, 'latency_p95_ms', orange, 'p95')):
            mids = [r[key] / 1000 for r in live['runs']]
            axes[1].bar([i + shift for i in x], mids, .38, color=color, label=label,
                        yerr=spread(live['runs'], key, 1000), capsize=3, ecolor=grey)
        axes[1].text(0, -0.16, f'cột = trung vị {live.get("repeats", 1)} lần chạy, vạch = thấp nhất–cao nhất',
                     transform=axes[1].transAxes, fontsize=8, color=grey)
        axes[1].set_xticks(list(x), labels)
        axes[1].set_title('Độ trễ: gửi → Spark xử lý xong (giây)')
        axes[1].legend()
        fig.tight_layout()
        fig.savefig(RESULTS / 'live_lag_latency.png', dpi=150)
        plt.close(fig)

    capacity = load('capacity')
    if capacity:
        fig, axis = plt.subplots(figsize=(7, 4))
        runs = capacity['runs']
        labels = [f'{r["cores"]} core\n{r["kafka_partitions"]} partition' for r in runs]
        axis.bar(range(len(runs)), [r['records_per_second'] for r in runs], color=[blue if r['kafka_partitions'] == 4 else grey for r in runs],
                 yerr=spread(runs, 'records_per_second'), capsize=4, ecolor='#495057')
        for i, r in enumerate(runs):
            axis.text(i, r['records_per_second'], f'{r["records_per_second"]:,}'.replace(',', '.'), ha='center', va='bottom', fontsize=8)
        axis.set_xticks(range(len(runs)), labels, fontsize=8)
        axis.set_ylabel('Bản ghi / giây')
        axis.set_title(f'Tốc độ xử lý tối đa của Spark (trung vị {capacity.get("repeats", 1)} lần, vạch = thấp nhất–cao nhất)', fontsize=10)
        fig.tight_layout()
        fig.savefig(RESULTS / 'capacity.png', dpi=150)
        plt.close(fig)
    log('charts written to assess/results/')


STAGES = dict(tests=stage_tests, quality=stage_quality, nab=stage_nab, tuning=stage_tuning, sensitivity=stage_sensitivity, live=stage_live,
              capacity=stage_capacity, recovery=stage_recovery, charts=stage_charts)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('stages', nargs='+', choices=list(STAGES))
    args = parser.parse_args()
    if sys.version_info[:2] not in ((3, 11), (3, 12)):
        raise SystemExit('Use .venv-anomaly (Python 3.11 or 3.12)')
    for name in args.stages:
        log(f'== {name}')
        STAGES[name]()


if __name__ == '__main__':
    main()

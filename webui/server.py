"""Local demo/tracking website for the NAB anomaly pipeline.

Run from the repository root with the anomaly environment:

    .venv-anomaly/bin/python webui/server.py            # http://127.0.0.1:8790

Standard library only (plus the project's own modules). The server binds to
localhost, reads run directories under anomaly_runs/, and can launch the
existing pipeline scripts with fixed, validated arguments. It never changes the
detector itself: every score shown comes from anomaly_detection.* code.
"""
import argparse
from collections import Counter
from dataclasses import asdict
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from anomaly_detection.calibration import calibrate, load_calibration  # noqa: E402
from anomaly_detection.config import DetectorConfig  # noqa: E402
from anomaly_detection.engine import MetricEngine  # noqa: E402
from anomaly_detection.evaluation import read_scored, window_metrics  # noqa: E402
from anomaly_detection.events import epoch_ms  # noqa: E402
from anomaly_detection.replay import metric_events  # noqa: E402

DATA = ROOT / 'data'
RUNS = ROOT / 'anomaly_runs'
STATIC = Path(__file__).resolve().parent / 'static'
WINDOWS = json.loads((ROOT / 'labels/combined_windows.json').read_text())
NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,59}$')
STEPS = ('calibrate', 'topic', 'replay', 'reference', 'detect', 'compare', 'evaluate')


# ---------------------------------------------------------------- environment

def java17_home():
    current = os.environ.get('JAVA_HOME')
    if current and '17' in current:
        return current
    try:
        found = subprocess.run(['/usr/libexec/java_home', '-v', '17'], capture_output=True, text=True, timeout=5)
        # java_home may fall back to a newer JDK; Spark 3.5.1 needs 17 exactly.
        if found.returncode == 0 and '17' in Path(found.stdout.strip()).name:
            return found.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        pass
    for prefix in ('/opt/homebrew/opt/openjdk@17', '/usr/local/opt/openjdk@17'):
        home = Path(prefix) / 'libexec/openjdk.jdk/Contents/Home'
        if home.is_dir():
            return str(home)
    for candidate in Path('/usr/lib/jvm').glob('*17*') if Path('/usr/lib/jvm').is_dir() else []:
        return str(candidate)
    return None


def child_env():
    env = dict(os.environ, PYSPARK_PYTHON=sys.executable, PYTHONUNBUFFERED='1')
    home = java17_home()
    if home:
        env['JAVA_HOME'] = home
        env['PATH'] = f'{home}/bin:' + env.get('PATH', '')
    return env


def docker_ready():
    if not shutil.which('docker'):
        return False
    try:
        return subprocess.run(['docker', 'info'], capture_output=True, timeout=4).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def environment():
    try:
        import pyspark
        spark = pyspark.__version__
    except ImportError:
        spark = None
    try:
        import pyarrow  # noqa: F401
        arrow = True
    except ImportError:
        arrow = False
    return dict(python=sys.version.split()[0], python_ok=sys.version_info[:2] in ((3, 11), (3, 12)),
                executable=sys.executable, pyspark=spark, pyarrow=arrow, java17=java17_home(),
                docker=docker_ready(), root=str(ROOT))


# ------------------------------------------------------------------- datasets

def datasets():
    result = []
    for path in sorted(DATA.glob('*/*.csv')):
        key = path.relative_to(DATA).as_posix()
        result.append(dict(metric=key, group=key.split('/')[0], windows=len(WINDOWS.get(key, [])),
                           labeled=key in WINDOWS))
    return result


# ----------------------------------------------------------------- run status

def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


def count_lines(path):
    try:
        with Path(path).open('rb') as stream:
            return sum(1 for _ in stream)
    except OSError:
        return None


def numbered(directory):
    if not directory.is_dir():
        return []
    return sorted(int(p.name) for p in directory.iterdir() if p.name.isdigit())


def committed_files(scores):
    meta = scores / '_spark_metadata'
    if not meta.is_dir():
        return 0
    files = set()
    for log in sorted((p for p in meta.iterdir() if p.name.split('.')[0].isdigit()), key=lambda p: int(p.name.split('.')[0])):
        for line in log.read_text().splitlines()[1:]:
            entry = json.loads(line)
            (files.discard if entry.get('action') == 'delete' else files.add)(entry['path'])
    return len(files)


def run_settings(run):
    return read_json(run / 'webui' / 'run.json') or {}


def run_status(name):
    run = RUNS / name
    settings = run_settings(run)
    kafka = settings.get('mode') == 'kafka'
    calibration = read_json(run / 'calibration.json')
    contract = read_json(run / 'checkpoint' / 'anomaly-contract.json')
    if calibration is None and contract:
        calibration = contract['calibration']  # run calibrated outside this directory
    manifest_path = next((p for p in (run / 'replay-manifest.json', run / 'manifest.json') if p.exists()), None)
    manifest = read_json(manifest_path) if manifest_path else None
    events = run / 'input' / 'events.json'
    checkpoint = run / 'checkpoint'
    progress_lines = []
    if (run / 'progress.jsonl').exists():
        progress_lines = [json.loads(l) for l in (run / 'progress.jsonl').read_text().splitlines() if l.strip()]
    parity = read_json(run / 'parity.json')
    report = read_json(run / 'report' / 'metrics.json')
    job = JOBS.latest(name)
    running = {j['step'] for j in JOBS.list(name) if j['state'] == 'running'}

    def stage(key, done, detail, applicable=True):
        state = 'running' if key in running else ('done' if done else 'pending')
        last = JOBS.last_for(name, key)
        if state == 'pending' and last and last['state'] == 'failed':
            state = 'failed'
        return dict(key=key, state=state if applicable else 'skipped', detail=detail)

    stages = [
        stage('calibrate', calibration is not None, dict(
            fraction=calibration and calibration.get('fraction'),
            metrics={k: {f: v[f] for f in ('mean', 'stddev', 'sample_count', 'end_timestamp', 'absolute_tolerance', 'calibration_id')}
                     for k, v in (calibration or {}).get('metrics', {}).items()})),
        stage('topic', bool(settings.get('topic_created')), dict(topic=settings.get('topic')), kafka),
        stage('replay', bool(manifest and manifest.get('status') == 'completed'), manifest and dict(
            status=manifest.get('status'), data_records=manifest.get('data_records'),
            target=(manifest.get('arguments') or {}).get('json_output') or (manifest.get('arguments') or {}).get('topic'),
            achieved_records_per_second=manifest.get('achieved_records_per_second'),
            started_at=manifest.get('started_at'), file=manifest_path.name, input_lines=count_lines(events))),
        stage('reference', (run / 'reference.jsonl').exists(), dict(rows=count_lines(run / 'reference.jsonl')), not kafka),
        stage('detect', committed_files(run / 'scores') > 0, dict(
            source=contract and ('Kafka topic ' + contract['input']['topic'] if isinstance(contract['input'], dict) else 'JSONL files'),
            config=contract and contract['config'], offsets=len(numbered(checkpoint / 'offsets')),
            commits=len(numbered(checkpoint / 'commits')), committed_files=committed_files(run / 'scores'),
            batches=len(progress_lines), input_rows=sum(p.get('numInputRows', 0) for p in progress_lines),
            state_rows=(progress_lines[-1].get('stateOperators') or [{}])[0].get('numRowsTotal') if progress_lines else None,
            last_batch=progress_lines[-1] if progress_lines else None)),
        stage('compare', parity is not None, parity and {k: (len(v) if isinstance(v, list) else v) for k, v in parity.items()}, not kafka),
        stage('evaluate', report is not None, report and dict(
            status_counts=report.get('status_counts'), rejection_reasons=report.get('rejection_reasons'),
            aggregate=report.get('aggregate'), series=report.get('series'),
            performance=report.get('performance') and {k: v for k, v in report['performance'].items() if k != 'batches'},
            images=[p.name for p in (run / 'report').glob('*.png')])),
    ]
    return dict(name=name, mode='kafka' if kafka else 'file', settings=settings, stages=stages,
                metrics=list((calibration or {}).get('metrics', {})), last_job=job)


def list_runs():
    if not RUNS.is_dir():
        return []
    runs = []
    for run in sorted(RUNS.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if run.is_dir() and NAME.match(run.name):
            status = run_status(run.name)
            runs.append(dict(name=run.name, mode=status['mode'],
                             done=sum(s['state'] == 'done' for s in status['stages']),
                             total=sum(s['state'] != 'skipped' for s in status['stages'])))
    return runs


# --------------------------------------------------------------------- series

def compact(rows, metric, calibration_count=None):
    rows = sorted((r for r in rows if r.get('metric_id') == metric and r.get('record_status') in ('accepted', 'calibration')),
                  key=lambda r: r['sequence'])
    source = rows[0].get('source_metric_id', metric) if rows else metric
    windows = [[epoch_ms(a), epoch_ms(b)] for a, b in WINDOWS.get(source, [])]
    return dict(metric=metric, windows=windows,
                t=[epoch_ms(r['event_time']) for r in rows], v=[r['value'] for r in rows],
                cal=[r['record_status'] == 'calibration' for r in rows],
                sz=[r.get('static_zscore') for r in rows], sa=[r.get('static_anomaly') for r in rows],
                rz=[r.get('rolling_zscore') for r in rows], ra=[r.get('rolling_anomaly') for r in rows],
                rm=[r.get('rolling_mean') for r in rows], rs=[r.get('rolling_stddev') for r in rows],
                rr=[r.get('rolling_reason') for r in rows], n=[r.get('rolling_sample_count') for r in rows],
                threshold=rows[0].get('zscore_threshold') if rows else None)


def run_series(name, metric, source):
    run = RUNS / name
    path = run / ('reference.jsonl' if source == 'reference' else 'scores')
    if not path.exists():
        raise LookupError(f'{path.relative_to(ROOT)} does not exist yet')
    rows = list(read_scored(path))
    result = compact(rows, metric)
    windows = WINDOWS.get(metric, [])
    accepted = [r for r in rows if r.get('metric_id') == metric]
    result['quality'] = {d: window_metrics(accepted, windows, d) for d in ('static', 'rolling')}
    result['statuses'] = dict(Counter(r.get('record_status') for r in rows if r.get('metric_id') == metric))
    # Calibration and detector settings let the page explain the chart in words.
    contract = read_json(run / 'checkpoint' / 'anomaly-contract.json') or {}
    calibration = read_json(run / 'calibration.json') or contract.get('calibration') or {}
    result['calibration'] = calibration.get('metrics', {}).get(metric)
    result['config'] = contract.get('config') or {k: v for k, v in run_settings(run).items()
                                                  if k in ('history_size', 'min_periods', 'threshold')} or None
    return result


# ----------------------------------------------------------------- playground

def make_config(body, mode='ordered'):
    return DetectorConfig(history_size=int(body.get('history_size', 100)), min_periods=int(body.get('min_periods', 20)),
                          threshold=float(body.get('threshold', 3)), absolute_tolerance=float(body.get('tolerance', .01)),
                          mode=mode)


def playground(body):
    metric = body.get('metric')
    if metric not in {d['metric'] for d in datasets()}:
        raise ValueError('Unknown dataset')
    path = DATA / metric
    config = make_config(body)
    fraction = float(body.get('fraction', .15))
    tolerance = float(body.get('tolerance', .01))
    batch = max(1, int(body.get('batch_size', 500)))
    started = time.perf_counter()
    artifact = calibrate([path], DATA, fraction, tolerance)
    events = [dict(e, sent_at='2026-01-01T00:00:00+00:00') for e in metric_events(path, DATA, 'playground')]
    # Single pass versus micro-batches with a JSON state round trip, exactly as Spark stores it.
    single = MetricEngine(config, artifact['metrics']).process(events)
    engine, batched = MetricEngine(config, artifact['metrics']), []
    for start in range(0, len(events), batch):
        batched.extend(engine.process(events[start:start + batch]))
        engine = MetricEngine(config, artifact['metrics'], json.loads(engine.dumps()))
    mismatches = 0
    for a, b in zip(single, batched):
        for field in ('event_id', 'record_status', 'static_anomaly', 'rolling_anomaly'):
            mismatches += a.get(field) != b.get(field)
        for field in ('static_zscore', 'rolling_zscore'):
            x, y = a.get(field), b.get(field)
            mismatches += (x is None) != (y is None) or (x is not None and not math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9))
    result = compact(single, metric)
    windows = WINDOWS.get(metric, [])
    result.update(
        quality={d: window_metrics(single, windows, d) for d in ('static', 'rolling')},
        calibration=artifact['metrics'][metric], config=asdict(config),
        batch_check=dict(batch_size=batch, batches=math.ceil(len(events) / batch), records=len(events),
                         mismatches=mismatches, single_rows=len(single), batched_rows=len(batched)),
        elapsed_ms=round(1000 * (time.perf_counter() - started), 1), labeled=metric in WINDOWS)
    return result


# ------------------------------------------------------------ contract checks

BASE = {'m': dict(mean=10., stddev=1., sample_count=3, calibration_id='demo', absolute_tolerance=.1)}


def ev(i, value=10., metric='m', **extra):
    return dict(run_id='demo', metric_id=metric, event_id=f'{metric}:{i}', sequence=i,
                event_time=f'2020-01-01T00:{i // 60:02d}:{i % 60:02d}Z', sent_at='2026-01-01T00:00:00Z',
                value=value, is_control=False, **extra)


def brief(row):
    keys = ('event_id', 'record_status', 'reason', 'static_zscore', 'static_anomaly', 'static_reason',
            'rolling_mean', 'rolling_sample_count', 'rolling_zscore', 'rolling_anomaly', 'rolling_reason')
    return {k: row[k] for k in keys if k in row}


def contract_checks():
    checks = []

    def check(title, explanation, run):
        try:
            rows, passed, expected = run()
            checks.append(dict(title=title, explanation=explanation, expected=expected, passed=bool(passed),
                               rows=[brief(r) for r in rows]))
        except Exception as error:  # surfaced in the UI rather than hidden
            checks.append(dict(title=title, explanation=explanation, expected='', passed=False, rows=[], error=repr(error)))

    cfg = DetectorConfig(history_size=5, min_periods=3, absolute_tolerance=.1)

    def calibration_prefix():
        rows = MetricEngine(cfg, BASE).process([ev(i, 10 + i % 2) for i in range(5)])
        return rows, [r['record_status'] for r in rows] == ['calibration'] * 3 + ['accepted'] * 2, \
            'sequence 0-2 are calibration (sample_count=3); later rows are accepted and statically scored'
    check('Calibration prefix', 'Rows inside the calibration prefix build rolling history but are not evaluated.', calibration_prefix)

    def warmup():
        rows = MetricEngine(cfg, BASE).process([ev(i, 10 + i % 2) for i in range(5)])
        reasons = [r['rolling_reason'] for r in rows]
        return rows, reasons[:3] == ['warmup'] * 3 and rows[3]['rolling_zscore'] is not None, \
            'first min_periods=3 rows: rolling_reason=warmup, no rolling decision'
    check('Rolling warmup', 'Rolling Z-score needs min_periods prior observations.', warmup)

    def score_before_update():
        rows = MetricEngine(DetectorConfig(history_size=5, min_periods=3), BASE).process(
            [ev(0, 9), ev(1, 11), ev(2, 9), ev(3, 11), ev(4, 30)])
        last = rows[-1]
        return rows, last['rolling_mean'] == 10 and last['rolling_sample_count'] == 4 and last['rolling_anomaly'] is True, \
            'spike 30 is scored against mean 10 of the 4 PRIOR values (itself excluded) -> anomaly'
    check('Score before update', 'The current value never contributes to its own baseline.', score_before_update)

    def constant_baseline():
        flat = [ev(i, 10) for i in range(4)]
        a = MetricEngine(cfg, BASE).process(flat + [ev(4, 10.05)])[-1]
        b = MetricEngine(cfg, BASE).process(flat + [ev(4, 15)])[-1]
        rows = [a, b]
        ok = a['rolling_reason'] == b['rolling_reason'] == 'constant_baseline' and a['rolling_anomaly'] is False \
            and b['rolling_anomaly'] is True and b['rolling_zscore'] is None
        return rows, ok, 'flat history: |10.05-10|<=0.1 normal; |15-10|>0.1 anomaly; z-score stays null'
    check('Flat baseline', 'Standard deviation 0 uses absolute tolerance instead of dividing by zero.', constant_baseline)

    def rejections():
        engine = MetricEngine(cfg, BASE)
        engine.process([ev(0), ev(2)])
        before = list(engine.state['history'])
        bad = ev(5, 10)
        bad.pop('value')
        rows = engine.process([ev(2, 900), ev(1, 900), bad, ev(6, 900, metric='unknown')])
        reasons = [r.get('reason', '').split(':')[0] for r in rows]
        ok = reasons == ['duplicate_or_stale_sequence', 'duplicate_or_stale_sequence', 'invalid_event', 'unknown_metric'] \
            and engine.state['history'] == before
        return rows, ok, 'duplicate, stale, invalid and unknown-metric events are rejected and history stays [10, 10]'
    check('Rejected input', 'Bad events are emitted as rejected records and never pollute the baseline.', rejections)

    def out_of_order():
        engine = MetricEngine(cfg, BASE)
        first = engine.process([ev(0), ev(1)])
        late = ev(2, 10)
        late['event_time'] = '2019-12-31T23:59:00Z'
        rows = first + engine.process([late])
        return rows, rows[-1].get('reason') == 'out_of_order', 'event with a newer sequence but older event_time -> out_of_order'
    check('Out-of-order in ordered mode', 'Ordered mode rejects events older than the last accepted (event_time, sequence).', out_of_order)

    def isolation():
        ordered = MetricEngine(cfg, {'a': dict(BASE['m']), 'b': dict(BASE['m'])})
        a = MetricEngine(cfg, {'a': dict(BASE['m']), 'b': dict(BASE['m'])})
        b = MetricEngine(cfg, {'a': dict(BASE['m']), 'b': dict(BASE['m'])})
        rows_a = a.process([ev(i, 10 + i % 2, metric='a') for i in range(5)])
        rows_b = b.process([ev(i, 1000 * (i + 1), metric='b') for i in range(5)])
        mixed = ordered.process([ev(0, 10, metric='a')])
        try:
            ordered.process([ev(1, 10, metric='b')])
            raised = False
        except ValueError:
            raised = True
        return rows_a + rows_b + mixed, raised and a.state['history'] != b.state['history'], \
            'each metric has its own state; feeding metric b into metric a state raises an error'
    check('Per-metric isolation', 'Spark groups by metric_id; one engine state never mixes metrics.', isolation)

    def batches():
        config = DetectorConfig(history_size=20, min_periods=3)
        data = [ev(i, 10 + math.sin(i) + (8 if i == 60 else 0)) for i in range(90)]
        expected = MetricEngine(config, BASE).process(data)
        engine, actual = MetricEngine(config, BASE), []
        for start in range(0, len(data), 7):
            actual.extend(engine.process(data[start:start + 7]))
            engine = MetricEngine(config, BASE, json.loads(engine.dumps()))
        same = all(x['event_id'] == y['event_id'] and x['rolling_anomaly'] == y['rolling_anomaly'] and
                   (x['rolling_zscore'] is None or math.isclose(x['rolling_zscore'], y['rolling_zscore'], rel_tol=1e-9))
                   for x, y in zip(expected, actual))
        return [r for r in actual if r.get('rolling_anomaly')], same and len(actual) == 90, \
            '90 events in micro-batches of 7 with JSON state restore == one single pass'
    check('State across micro-batches', 'History survives batch boundaries and a serialize/restore (checkpoint).', batches)

    def buffered():
        config = DetectorConfig(history_size=10, min_periods=2, mode='buffered')
        engine = MetricEngine(config, BASE)
        r1 = engine.process([ev(2, 2.), ev(0, 0.), ev(3, 3.)])
        r2 = engine.process([ev(1, 1.)])
        released = engine.process([], watermark_ms=epoch_ms('2020-01-01T00:00:02Z'))
        still_pending = [e['sequence'] for e in engine.state['pending']]
        late = engine.process([ev(9, 9.)], watermark_ms=epoch_ms('2020-01-01T00:00:10Z'))
        ok = r1 == [] and r2 == [] and [r['sequence'] for r in released] == [0, 1, 2] and still_pending == [3] and \
            late[0].get('reason') == 'beyond_watermark' and [r.get('sequence') for r in late[1:]] == [3]
        return released + late, ok, 'shuffled events wait in state; watermark 00:02 releases 0,1,2 in order; seq 3 stays pending; ' \
            'watermark 00:10 rejects late seq 9 (beyond_watermark) and releases seq 3'
    check('Buffered mode + watermark', 'Bounded disorder: events are buffered and released in event-time order.', buffered)

    def config_change():
        engine = MetricEngine(cfg, BASE)
        engine.process([ev(0)])
        state = json.loads(engine.dumps())
        try:
            MetricEngine(DetectorConfig(history_size=5, min_periods=3, threshold=4), BASE, state)
            return [], False, ''
        except ValueError as error:
            return [dict(event_id='-', record_status='refused', reason=str(error))], True, \
                'restoring a checkpoint with another threshold is refused'
    check('Checkpoint contract', 'A changed detector/calibration cannot silently reuse old state.', config_change)
    return checks


# ----------------------------------------------------------------------- jobs

class Jobs:
    def __init__(self):
        self.lock = threading.Lock()
        self.items = []

    def list(self, run=None):
        with self.lock:
            return [self.view(j) for j in self.items if run is None or j['run'] == run]

    def latest(self, run):
        items = self.list(run)
        return items[-1] if items else None

    def last_for(self, run, step):
        items = [j for j in self.list(run) if j['step'] == step]
        return items[-1] if items else None

    def view(self, job):
        return {k: v for k, v in job.items() if k not in ('process',)}

    def start(self, run, step, argv, log, on_success=None):
        with self.lock:
            if any(j['state'] == 'running' and j['run'] == run and j['step'] == step for j in self.items):
                raise ValueError(f'{step} is already running for {run}')
            log.parent.mkdir(parents=True, exist_ok=True)
            stream = log.open('w')
            stream.write('$ ' + ' '.join(argv) + '\n\n')
            stream.flush()
            process = subprocess.Popen(argv, cwd=ROOT, env=child_env(), stdout=stream, stderr=subprocess.STDOUT)
            job = dict(id=len(self.items) + 1, run=run, step=step, argv=argv, log=str(log.relative_to(ROOT)),
                       state='running', started=time.time(), finished=None, returncode=None, process=process)
            self.items.append(job)

        def wait():
            code = process.wait()
            stream.close()
            with self.lock:
                job.update(state='done' if code == 0 else ('stopped' if job.get('stopping') else 'failed'),
                           returncode=code, finished=time.time())
            if code == 0 and on_success:
                on_success()
        threading.Thread(target=wait, daemon=True).start()
        return self.view(job)

    def stop(self, job_id):
        with self.lock:
            for job in self.items:
                if job['id'] == job_id and job['state'] == 'running':
                    job['stopping'] = True
                    job['process'].send_signal(2)  # SIGINT: run_detection.py stops its query cleanly
                    return True
        return False

    def tail(self, job_id, size=6000):
        with self.lock:
            job = next((j for j in self.items if j['id'] == job_id), None)
        if not job:
            raise LookupError('Unknown job')
        data = (ROOT / job['log']).read_bytes()
        return dict(self.view(job), output=data[-size:].decode('utf-8', 'replace'))


JOBS = Jobs()


def save_settings(run, **values):
    path = run / 'webui' / 'run.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    settings = read_json(path) or {}
    settings.update(values)
    path.write_text(json.dumps(settings, indent=2))


def create_run(body):
    name = body.get('name', '')
    if not NAME.match(name):
        raise ValueError('Run name: letters, digits, dot, dash, underscore (max 60)')
    run = RUNS / name
    if run.exists():
        raise ValueError(f'anomaly_runs/{name} already exists; choose a new name')
    mode = body.get('mode', 'file')
    topic = body.get('topic') or f'anomaly_{name}'.replace('.', '_')
    if mode not in ('file', 'kafka') or not NAME.match(topic):
        raise ValueError('Invalid mode or topic')
    files = body.get('files') or []
    known = {d['metric'] for d in datasets()}
    if not files or any(f not in known for f in files):
        raise ValueError('Select at least one dataset')
    fraction, tolerance = float(body.get('fraction', .15)), float(body.get('tolerance', .01))
    detector = {k: body.get(k) for k in ('history_size', 'min_periods', 'threshold') if body.get(k) is not None}
    make_config(detector)  # validate early
    run.mkdir(parents=True)
    save_settings(run, mode=mode, topic=topic, files=files, fraction=fraction, tolerance=tolerance,
                  rate=float(body.get('rate', 100)), partitions=int(body.get('partitions', 4)), **detector)
    return run_status(name)


def start_step(name, step):
    run = RUNS / name
    if not run.is_dir() or not NAME.match(name):
        raise LookupError('Unknown run')
    if step not in STEPS:
        raise ValueError('Unknown step')
    s = run_settings(run)
    py, rel = sys.executable, (lambda p: str((run / p).relative_to(ROOT)))
    detector = ['--history-size', str(s.get('history_size', 100)), '--min-periods', str(s.get('min_periods', 20)),
                '--threshold', str(s.get('threshold', 3))]
    kafka = s.get('mode') == 'kafka'
    on_success = None
    if step == 'calibrate':
        if not s.get('files'):
            raise ValueError('This run was not created from the web UI; calibrate it with the CLI')
        argv = [py, 'script/calibrate_anomaly.py', '--files', *[f'data/{f}' for f in s['files']],
                '--fraction', str(s['fraction']), '--absolute-tolerance', str(s['tolerance']), '--output', rel('calibration.json')]
    elif step == 'topic':
        argv = ['docker', 'compose', 'exec', '-T', 'broker', 'kafka-topics', '--bootstrap-server', 'broker:29092',
                '--create', '--if-not-exists', '--topic', s['topic'], '--partitions', str(s.get('partitions', 4)),
                '--replication-factor', '1']
        on_success = lambda: save_settings(run, topic_created=True)  # noqa: E731
    elif step == 'replay':
        argv = [py, 'script/replay_anomaly.py', '--calibration', rel('calibration.json'), '--run-id', name,
                '--manifest', rel('replay-manifest.json')]
        argv += ['--topic', s['topic'], '--rate', str(s.get('rate', 100))] if kafka else \
            ['--rate', '0', '--json-output', rel('input/events.json')]
    elif step == 'reference':
        argv = [py, 'script/reference_anomaly.py', '--input', rel('input/events.json'),
                '--calibration', rel('calibration.json'), '--output', rel('reference.jsonl'), *detector]
    elif step == 'detect':
        argv = [py, 'script/run_detection.py', '--calibration', rel('calibration.json'), *detector,
                '--output', rel('scores'), '--checkpoint', rel('checkpoint'), '--progress', rel('progress.jsonl')]
        argv += ['--topic', s['topic'], '--bootstrap-servers', 'localhost:9092', '--master', 'local[2]',
                 '--shuffle-partitions', str(s.get('partitions', 4)), '--trigger-seconds', '1'] if kafka else \
            ['--json-input', rel('input'), '--available-now']
    elif step == 'compare':
        argv = [py, 'script/compare_anomaly.py', '--reference', rel('reference.jsonl'), '--actual', rel('scores'),
                '--output', rel('parity.json')]
    else:
        argv = [py, 'script/evaluate_anomaly.py', '--input', rel('scores'), '--output', rel('report')]
        if (run / 'progress.jsonl').exists():
            argv += ['--progress', rel('progress.jsonl')]
    return JOBS.start(name, step, argv, run / 'webui' / f'{step}.log', on_success)


def run_tests(spark):
    argv = [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v']
    log = RUNS / '.webui' / ('tests-spark.log' if spark else 'tests.log')
    if spark:
        argv = ['env', 'RUN_SPARK_TESTS=1', *argv]
    return JOBS.start('(tests)', 'tests-spark' if spark else 'tests', argv, log)


# --------------------------------------------------------------------- server

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC), **kwargs)

    def log_message(self, fmt, *args):
        if '/api/' in (args[0] if args else '') and ' 200 ' not in fmt % args:
            sys.stderr.write(fmt % args + '\n')

    def send_json(self, value, status=HTTPStatus.OK):
        data = json.dumps(value, allow_nan=False, default=str).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(data)

    def handle_api(self, method):
        url = urlparse(self.path)
        parts = [p for p in url.path.split('/') if p][1:]
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        body = {}
        if method == 'POST':
            length = int(self.headers.get('Content-Length') or 0)
            body = json.loads(self.rfile.read(length) or b'{}') if length else {}
        try:
            match method, parts:
                case 'GET', ['env']:
                    return self.send_json(environment())
                case 'GET', ['datasets']:
                    return self.send_json(datasets())
                case 'GET', ['runs']:
                    return self.send_json(list_runs())
                case 'POST', ['runs']:
                    return self.send_json(create_run(body))
                case 'GET', ['runs', name] if NAME.match(name) and (RUNS / name).is_dir():
                    return self.send_json(run_status(name))
                case 'GET', ['runs', name, 'series'] if NAME.match(name):
                    return self.send_json(run_series(name, query.get('metric', ''), query.get('source', 'scores')))
                case 'GET', ['runs', name, 'image', image] if NAME.match(name) and re.match(r'^\w+\.png$', image):
                    data = (RUNS / name / 'report' / image).read_bytes()
                    self.send_response(200)
                    self.send_header('Content-Type', 'image/png')
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    return self.wfile.write(data)
                case 'POST', ['runs', name, 'steps', step]:
                    return self.send_json(start_step(name, step))
                case 'GET', ['jobs']:
                    return self.send_json(JOBS.list(query.get('run')))
                case 'GET', ['jobs', job_id] if job_id.isdigit():
                    return self.send_json(JOBS.tail(int(job_id)))
                case 'POST', ['jobs', job_id, 'stop'] if job_id.isdigit():
                    return self.send_json(dict(stopped=JOBS.stop(int(job_id))))
                case 'POST', ['tests']:
                    return self.send_json(run_tests(bool(body.get('spark'))))
                case 'POST', ['playground']:
                    return self.send_json(playground(body))
                case 'GET', ['checks']:
                    return self.send_json(contract_checks())
            return self.send_json(dict(error='Not found'), HTTPStatus.NOT_FOUND)
        except (LookupError, FileNotFoundError) as error:
            return self.send_json(dict(error=str(error)), HTTPStatus.NOT_FOUND)
        except (ValueError, TypeError, KeyError) as error:
            return self.send_json(dict(error=str(error)), HTTPStatus.BAD_REQUEST)

    def do_GET(self):
        if self.path.startswith('/api/'):
            return self.handle_api('GET')
        return super().do_GET()

    def do_POST(self):
        if self.path.startswith('/api/'):
            return self.handle_api('POST')
        self.send_error(HTTPStatus.NOT_FOUND)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--port', type=int, default=int(os.environ.get('PORT', 8790)))
    args = parser.parse_args()
    RUNS.mkdir(exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    print(f'Anomaly demo UI: http://127.0.0.1:{args.port}  (Ctrl+C to stop)')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()

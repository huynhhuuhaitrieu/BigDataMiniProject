'use strict';
const $ = (s, el = document) => el.querySelector(s);
const esc = v => String(v ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const fmt = (v, d = 3) => v === null || v === undefined ? '—' : typeof v === 'number' ? (Number.isInteger(v) ? v.toLocaleString() : v.toFixed(d)) : typeof v === 'boolean' ? String(v) : esc(v);
const time = ms => new Date(ms).toISOString().replace('T', ' ').slice(0, 16);

async function api(path, body) {
  const res = await fetch('/api/' + path, body === undefined ? {} :
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

/* ------------------------------------------------------------- navigation */
function route() {
  const page = (location.hash || '#pipeline').slice(1);
  document.querySelectorAll('.page').forEach(p => p.classList.toggle('active', p.id === page));
  document.querySelectorAll('nav a').forEach(a => a.classList.toggle('active', a.hash === '#' + page));
  if (page === 'checks' && !$('#checks-out').innerHTML) loadChecks();
  if (page === 'series') fillSeriesRuns();
}
window.addEventListener('hashchange', route);

/* ------------------------------------------------------------ environment */
async function loadEnv() {
  const e = await api('env');
  const item = (ok, label) => `<span class="${ok ? 'ok' : 'bad'}">${ok ? '✓' : '✗'} ${esc(label)}</span>`;
  $('#env').innerHTML = [
    item(e.python_ok, 'Python ' + e.python),
    item(e.pyspark, 'PySpark ' + (e.pyspark || 'missing')),
    item(e.pyarrow, 'PyArrow'),
    item(e.java17, e.java17 ? 'Java 17' : 'Java 17 not found (Spark steps will fail)'),
    item(e.docker, e.docker ? 'Docker running' : 'Docker not running (Kafka mode unavailable)'),
  ].join('');
  state.env = e;
}

/* --------------------------------------------------------------- datasets */
const state = { runs: [], run: null, status: null, stage: 'detect', job: null, datasets: [], env: null, timer: null };

async function loadDatasets() {
  state.datasets = await api('datasets');
  const groups = {};
  state.datasets.forEach(d => (groups[d.group] ??= []).push(d));
  const defaults = ['realAWSCloudwatch/ec2_cpu_utilization_24ae8d.csv', 'realAWSCloudwatch/ec2_cpu_utilization_c6585a.csv',
    'realAWSCloudwatch/ec2_network_in_257a54.csv'];
  $('#dataset-picker').innerHTML = Object.entries(groups).map(([g, items]) => `<b>${esc(g)}</b>` + items.map(d =>
    `<label><input type="checkbox" name="files" value="${esc(d.metric)}" ${defaults.includes(d.metric) ? 'checked' : ''}>
     ${esc(d.metric.split('/')[1])} <span class="muted">(${d.windows} window${d.windows === 1 ? '' : 's'})</span></label>`).join('')).join('');
  $('#play-form [name=metric]').innerHTML = Object.entries(groups).map(([g, items]) => `<optgroup label="${esc(g)}">` +
    items.map(d => `<option value="${esc(d.metric)}" ${d.metric === defaults[0] ? 'selected' : ''}>${esc(d.metric.split('/')[1])} · ${d.windows} windows</option>`).join('') + '</optgroup>').join('');
}

/* ------------------------------------------------------------------- runs */
const STAGES = {
  calibrate: ['Calibrate', 'script/calibrate_anomaly.py', 'calibration.json',
    'Computes a static baseline (mean, population stddev) per metric from the first chronological fraction of each CSV. The artifact is hashed and immutable.'],
  topic: ['Kafka topic', 'docker compose exec broker kafka-topics', 'topic on broker',
    'Creates a dedicated topic for this run so offsets and checkpoints never mix with other experiments.'],
  replay: ['Replay', 'script/replay_anomaly.py', 'input/events.json or Kafka topic',
    'Turns CSV rows into events {run_id, metric_id, event_id, sequence, event_time, value, sent_at}, merged by event time, keyed by metric. Ends with control events.'],
  reference: ['Offline reference', 'script/reference_anomaly.py', 'reference.jsonl',
    'Scores the same events sequentially with MetricEngine, without Spark. Used as ground truth for parity.'],
  detect: ['Spark detect', 'script/run_detection.py', 'scores/ (Parquet), checkpoint/, progress.jsonl',
    'Spark Structured Streaming: groupBy(metric_id).applyInPandasWithState. Keeps rolling history per metric in checkpointed state and writes static + rolling decisions.'],
  compare: ['Parity', 'script/compare_anomaly.py', 'parity.json',
    'Checks that Spark output equals the offline reference: same event IDs, no duplicates, same flags and z-scores (1e-8 tolerance).'],
  evaluate: ['Evaluate', 'script/evaluate_anomaly.py', 'report/metrics.json, quality.png, streaming.png',
    'Window recall, false alerts per 1,000 records and detection delay against NAB labelled windows; Spark progress (throughput, state size, latency proxy).'],
};

async function loadRuns(select) {
  state.runs = await api('runs');
  $('#runs').innerHTML = state.runs.length ? state.runs.map(r =>
    `<li data-run="${esc(r.name)}" class="${r.name === state.run ? 'sel' : ''}"><span>${esc(r.name)}</span>
     <span class="muted small">${r.mode} · ${r.done}/${r.total}</span></li>`).join('') : '<li class="muted">No runs yet</li>';
  if (select) openRun(select);
  else if (!state.run && state.runs.length) openRun(state.runs[0].name);
}

$('#runs').addEventListener('click', e => {
  const li = e.target.closest('li[data-run]');
  if (li) openRun(li.dataset.run);
});

async function openRun(name) {
  if (state.run !== name) state.job = null;
  state.run = name;
  document.querySelectorAll('#runs li').forEach(li => li.classList.toggle('sel', li.dataset.run === name));
  await refreshRun();
}

function prerequisitesMet(status, key) {
  const st = Object.fromEntries(status.stages.map(s => [s.key, s.state]));
  const kafka = status.mode === 'kafka';
  const need = {
    calibrate: [], topic: [], replay: kafka ? ['calibrate', 'topic'] : ['calibrate'], reference: ['replay'],
    detect: kafka ? ['calibrate', 'topic'] : ['replay'], compare: ['reference', 'detect'], evaluate: ['detect'],
  }[key];
  if (key === 'topic' && !state.env?.docker) return false;
  if (kafka && (key === 'replay' || key === 'detect') && !state.env?.docker) return false;
  return need.every(k => st[k] === 'done');
}

async function refreshRun() {
  if (!state.run) return;
  let status;
  try { status = await api('runs/' + encodeURIComponent(state.run)); } catch (e) { $('#run-view').innerHTML = `<p class="error">${esc(e.message)}</p>`; return; }
  state.status = status;
  const jobs = await api('jobs?run=' + encodeURIComponent(state.run));
  const running = Object.fromEntries(jobs.filter(j => j.state === 'running').map(j => [j.step, j]));
  if (!state.job && jobs.length) state.job = jobs[jobs.length - 1].id;
  const order = status.stages.filter(s => s.state !== 'skipped' || status.mode === 'kafka');
  let n = 0;
  const flow = order.map(s => {
    const [title] = STAGES[s.key];
    const label = s.state === 'skipped' ? 'not used' : s.state;
    const job = running[s.key];
    const btn = job ? `<button data-stop="${job.id}">Stop</button>` :
      s.state === 'skipped' ? '' :
      `<button data-step="${s.key}" ${prerequisitesMet(status, s.key) && s.state !== 'done' ? '' : 'disabled'}>${s.state === 'failed' ? 'Retry' : 'Run'}</button>`;
    return `<div class="stage ${s.state} ${s.key === state.stage ? 'sel' : ''}" data-stage="${s.key}">
      <div class="n">${s.state === 'skipped' ? '' : ++n}</div><div class="t">${title}</div><div class="s">${label}</div>${btn}</div>`;
  }).join('');
  const selected = status.stages.find(s => s.key === state.stage) || status.stages[0];
  const [title, script, writes, text] = STAGES[selected.key];
  $('#run-view').innerHTML = `
    <h2>${esc(status.name)} <span class="muted small">· ${status.mode === 'kafka' ? 'Kafka input, topic ' + esc(status.settings.topic) : 'JSONL file input (no Kafka)'}</span></h2>
    ${status.settings.files ? '' : '<p class="muted small">Created outside the web UI: stages are read-only here.</p>'}
    <div class="flow">${flow}</div>
    <p class="muted small">Click a stage for details. Steps run the project scripts with the run's settings; logs go to <code>anomaly_runs/${esc(status.name)}/webui/</code>.</p>
    <h3>${title}</h3>
    <div class="explain">${esc(text)}<br><code>${esc(script)}</code> → <code>${esc(writes)}</code></div>
    <div id="stage-detail">${stageDetail(selected, status)}</div>
    <h3>Job log</h3>
    <div class="toolbar">${jobs.length ? `<select id="job-pick">${jobs.map(j => `<option value="${j.id}" ${j.id === state.job ? 'selected' : ''}>#${j.id} ${j.step} — ${j.state}</option>`).join('')}</select>` : '<span class="muted">No jobs started from this page yet.</span>'}</div>
    <pre id="job-log" class="log ${jobs.length ? '' : 'hidden'}"></pre>`;
  if (state.job) showJob(state.job);
  schedule(Object.keys(running).length ? 1500 : 6000);
}

function schedule(ms) {
  clearTimeout(state.timer);
  state.timer = setTimeout(() => { if (location.hash === '' || location.hash === '#pipeline') refreshRun(); else schedule(ms); }, ms);
}

async function showJob(id) {
  try {
    const job = await api('jobs/' + id);
    const log = $('#job-log');
    if (!log) return;
    const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 20;
    log.textContent = job.output + (job.state === 'running' ? '\n… running' : `\n[${job.state}, exit ${job.returncode}]`);
    if (atBottom) log.scrollTop = log.scrollHeight;
  } catch (e) { /* job list is memory-only; ignore after server restart */ }
}

$('#run-view').addEventListener('click', async e => {
  const step = e.target.dataset.step, stop = e.target.dataset.stop;
  if (step) {
    e.target.disabled = true;
    try { const job = await api(`runs/${encodeURIComponent(state.run)}/steps/${step}`, {}); state.job = job.id; state.stage = step; }
    catch (err) { alert(err.message); }
    return refreshRun();
  }
  if (stop) { await api(`jobs/${stop}/stop`, {}); return refreshRun(); }
  const box = e.target.closest('.stage');
  if (box) { state.stage = box.dataset.stage; refreshRun(); }
});
$('#run-view').addEventListener('change', e => { if (e.target.id === 'job-pick') { state.job = +e.target.value; showJob(state.job); } });

function kv(obj) {
  return `<table class="kv">${Object.entries(obj).filter(([, v]) => v !== undefined).map(([k, v]) =>
    `<tr><th>${esc(k)}</th><td>${v !== null && typeof v === 'object' ? `<code>${esc(JSON.stringify(v))}</code>` : fmt(v)}</td></tr>`).join('')}</table>`;
}

function stageDetail(s, status) {
  const d = s.detail;
  if (!d || (s.key !== 'calibrate' && s.state === 'pending' && !Object.values(d).some(v => v))) return '<p class="muted">No output yet.</p>';
  switch (s.key) {
    case 'calibrate': {
      const rows = Object.entries(d.metrics || {});
      if (!rows.length) return '<p class="muted">No calibration yet.</p>';
      return `<p>Fraction ${fmt(d.fraction, 2)}</p><div class="scroll"><table><tr><th>Metric</th><th class="num">Samples</th><th class="num">Mean</th><th class="num">Stddev</th><th>Prefix ends</th><th class="num">Abs. tol.</th><th>ID</th></tr>
        ${rows.map(([m, c]) => `<tr><td>${esc(m)}</td><td class="num">${fmt(c.sample_count)}</td><td class="num">${fmt(c.mean, 4)}</td><td class="num">${fmt(c.stddev, 4)}</td><td>${esc(c.end_timestamp)}</td><td class="num">${fmt(c.absolute_tolerance)}</td><td><code>${esc(c.calibration_id)}</code></td></tr>`).join('')}</table></div>`;
    }
    case 'detect': {
      const last = d.last_batch;
      return kv({ source: d.source, 'checkpoint offsets (planned batches)': d.offsets, 'checkpoint commits (finished batches)': d.commits,
        'committed Parquet files': d.committed_files, 'progress events': d.batches, 'input rows (sum)': d.input_rows,
        'state rows (last batch)': d.state_rows, detector: d.config && `N=${d.config.history_size}, min_periods=${d.config.min_periods}, |z|≥${d.config.threshold}, mode=${d.config.mode}` })
        + (last ? `<h3>Last micro-batch</h3>` + kv({ batchId: last.batchId, numInputRows: last.numInputRows, inputRowsPerSecond: last.inputRowsPerSecond,
          processedRowsPerSecond: last.processedRowsPerSecond, 'triggerExecution ms': last.durationMs?.triggerExecution,
          'state memory bytes': (last.stateOperators || []).reduce((a, o) => a + (o.memoryUsedBytes || 0), 0), timestamp: last.timestamp }) : '')
        + (s.state === 'done' ? `<p><a href="#series" data-goto="${esc(status.name)}">Open scored series →</a></p>` : '');
    }
    case 'compare':
      return `<p class="${d.passed ? 'ok' : 'bad'}"><b>${d.passed ? 'PASSED' : 'FAILED'}</b> — Spark output ${d.passed ? 'matches' : 'differs from'} the offline reference.</p>` +
        kv({ 'reference records': d.reference_records, 'Spark records': d.actual_records, missing: d.missing, extra: d.extra,
          'duplicates (reference)': d.reference_duplicates, 'duplicates (Spark)': d.actual_duplicates, 'field mismatches': d.mismatches });
    case 'evaluate': return evaluateDetail(d, status);
    default: return kv(d);
  }
}

function qualityTable(series, aggregate) {
  const rows = Object.entries(series || {});
  const row = (name, q) => ['static', 'rolling'].map((det, i) => `<tr>${i ? '' : `<td rowspan="2">${esc(name)}</td>`}<td>${det}</td>
    <td class="num">${fmt(q[det].evaluated_records)}</td><td class="num">${fmt(q[det].eligible_windows)}</td><td class="num">${fmt(q[det].detected_windows)}</td>
    <td class="num">${fmt(q[det].window_recall)}</td><td class="num">${fmt(q[det].false_alerts)}</td><td class="num">${fmt(q[det].false_alerts_per_1000)}</td>
    <td class="num">${q[det].delay_median_seconds == null ? '—' : (q[det].delay_median_seconds / 60).toFixed(0) + ' min'}</td></tr>`).join('');
  return `<div class="scroll"><table><tr><th>Series</th><th>Detector</th><th class="num">Evaluated</th><th class="num">Windows</th><th class="num">Hit</th><th class="num">Recall</th><th class="num">False alerts</th><th class="num">per 1,000</th><th class="num">Median delay</th></tr>
    ${rows.map(([m, q]) => row(m.split('/').pop(), q)).join('')}${aggregate ? row('TOTAL', aggregate) : ''}</table></div>`;
}

function evaluateDetail(d, status) {
  const p = d.performance;
  return `<h3>Record status</h3>${kv(d.status_counts || {})}${Object.keys(d.rejection_reasons || {}).length ? '<h3>Rejection reasons</h3>' + kv(d.rejection_reasons) : ''}
    <h3>Detection quality (NAB windows)</h3>${qualityTable(d.series, d.aggregate)}
    <p class="muted small">Recall counts only windows fully inside the post-calibration range. No alert cooldown. A series without windows has recall “—”, not 0.</p>
    ${p ? '<h3>Streaming (driver-observed proxy)</h3>' + kv({ 'matched records': p.matched_records, 'latency p50 ms': p.p50_ms, 'latency p95 ms': p.p95_ms,
      'alert latency p50 ms': p.alert_p50_ms, 'alert latency p95 ms': p.alert_p95_ms, kind: p.latency_kind }) : ''}
    ${(d.images || []).map(i => `<p><img class="report" src="/api/runs/${encodeURIComponent(status.name)}/image/${i}" alt="${i}"></p>`).join('')}`;
}

document.addEventListener('click', e => {
  const goto = e.target.dataset?.goto;
  if (goto) { state.seriesRun = goto; }
});

/* --------------------------------------------------------------- new run */
const newRun = $('#new-run');
const syncMode = () => newRun.querySelector('.kafka-only').classList.toggle('hidden', newRun.mode.value !== 'kafka');
newRun.addEventListener('change', syncMode);
newRun.addEventListener('submit', async e => {
  e.preventDefault();
  const f = new FormData(newRun);
  const body = { name: f.get('name'), mode: f.get('mode'), files: f.getAll('files') };
  for (const k of ['fraction', 'tolerance', 'history_size', 'min_periods', 'threshold', 'rate', 'partitions']) body[k] = Number(f.get(k));
  if (f.get('topic')) body.topic = f.get('topic');
  try {
    $('#new-run-error').textContent = '';
    await api('runs', body);
    $('#new-run-box').open = false;
    state.stage = 'calibrate';
    await loadRuns(body.name);
  } catch (err) { $('#new-run-error').textContent = err.message; }
});

/* ------------------------------------------------------------------ chart */
// Numbered call-outs drawn on the chart; each returns the legend text shown under it.
function chartNotes(d, geo) {
  const { x, y, yz, L, R, W, T, H1, top2, thr } = geo;
  const inWin = t => d.windows.some(([a, b]) => a <= t && t <= b);
  const alert = i => (d.sa[i] || d.ra[i]) && !d.cal[i];
  const who = i => d.sa[i] && d.ra[i] ? 'cả static lẫn rolling' : d.sa[i] ? 'static' : 'rolling';
  const notes = [];
  const add = (bx, by, text, ring) => notes.push({ bx, by, text, ring });
  const calEnd = d.cal.lastIndexOf(true);
  if (calEnd >= 0) {
    const c = d.calibration;
    add((L + x(d.t[calEnd])) / 2, T + H1 * .45, `<b>Vùng xám = đoạn "học" (calibration).</b> ${fmt(calEnd + 1)} điểm đầu, đến ${time(d.t[calEnd])}.` +
      (c ? ` Static học được mức bình thường: trung bình ${fmt(c.mean, 4)}, độ lệch chuẩn ${fmt(c.stddev, 4)}.` : '') + ' Báo động ở đây không được tính.');
  }
  const win = d.windows.find(([a, b]) => b >= d.t[0] && a <= d.t[d.t.length - 1]);
  if (win) add(x((Math.max(win[0], d.t[0]) + Math.min(win[1], d.t[d.t.length - 1])) / 2), T + H1 * .55,
    `<b>Vùng hồng = sự cố thật</b> (đáp án NAB gán sẵn). File này có ${d.windows.length} vùng. Hệ thống cần báo động <i>bên trong</i> vùng hồng.`);
  else add(L + 60, T + H1 - 16, '<b>File này không có vùng sự cố nào</b> (không có vùng hồng), nên mọi báo động ngoài vùng xám đều là báo nhầm.');
  const first = d.t.findIndex((t, i) => alert(i));
  if (first >= 0) add(x(d.t[first]) - 16, T + 9, '<b>Hai hàng vạch trên cùng = các lần báo động.</b> Hàng trên màu cam: static báo. Hàng dưới màu xanh: rolling báo. Vạch mờ nằm trong vùng xám, không tính.');
  const sorted = d.v.filter(Number.isFinite).sort((a, b) => a - b), median = sorted[sorted.length >> 1];
  add(L + (W - L - R) * .62, y(median) - 16, `<b>Mức bình thường.</b> Phần lớn thời gian giá trị quanh ${fmt(median, 3)} (trung vị). Đường đen là giá trị thật theo thời gian.`);
  // Prefer a false alert both detectors raise, so it is not the same point as the rolling-only example.
  let fa = d.t.findIndex((t, i) => alert(i) && d.sa[i] && !inWin(t));
  if (fa < 0) fa = d.t.findIndex((t, i) => alert(i) && !inWin(t));
  if (fa >= 0) add(x(d.t[fa]) + 18, y(d.v[fa]) + 22, `<b>Báo nhầm.</b> Lúc ${time(d.t[fa])} giá trị ${fmt(d.v[fa], 3)}, ${who(fa)} báo động nhưng điểm này nằm ngoài mọi vùng hồng. Những gai lặp lại đều đặn thường là việc chạy định kỳ, bình thường với hệ thống.`, [x(d.t[fa]), y(d.v[fa])]);
  // Point at the largest alerted value inside a window: usually the incident itself.
  let hit = -1;
  d.t.forEach((t, i) => { if (alert(i) && inWin(t) && (hit < 0 || d.v[i] > d.v[hit])) hit = i; });
  if (hit >= 0) add(x(d.t[hit]) - 24, y(d.v[hit]) + 28, `<b>Bắt đúng.</b> Lúc ${time(d.t[hit])} giá trị ${fmt(d.v[hit], 3)}, ${who(hit)} báo động bên trong vùng hồng, tức là phát hiện được sự cố.`, [x(d.t[hit]), y(d.v[hit])]);
  const ro = d.t.findIndex((t, i) => i !== fa && alert(i) && d.ra[i] && !d.sa[i] && !inWin(t));
  if (ro >= 0) add(x(d.t[ro]) - 18, y(d.v[ro]) - 22, `<b>Chỉ rolling báo.</b> Lúc ${time(d.t[ro])} giá trị chỉ ${fmt(d.v[ro], 3)}, static thấy bình thường. Rolling so với ${d.config?.history_size ?? 'N'} điểm ngay trước, đoạn đó rất phẳng nên một nhích nhỏ cũng thành "lạ": rolling nhạy hơn nhưng dễ báo nhầm hơn.`, [x(d.t[ro]), y(d.v[ro])]);
  add(L + 14, top2 + 14, '<b>Khung dưới = mức độ lạ (z-score).</b> Cho biết mỗi điểm lệch khỏi bình thường bao nhiêu lần độ lệch chuẩn. Cam: static (so với vùng xám). Xanh: rolling (so với các điểm ngay trước). Trục bị cắt ở ±10.');
  add(W - R - 14, yz(thr) - 12, `<b>Đường đứt = ngưỡng ±${thr}.</b> Mức lạ vượt ngưỡng thì báo động, mỗi lần vượt sinh một vạch ở hàng trên cùng.`);
  return notes;
}

function chart(el, d, showNotes = false) {
  const W = Math.max(320, el.clientWidth || 900), H1 = 220, H2 = 120, L = 48, R = 10, T = 8, gap = 26;
  const H = T + H1 + gap + H2 + 22;
  const t0 = d.t[0], t1 = d.t[d.t.length - 1] || t0 + 1;
  const x = t => L + (t - t0) / (t1 - t0 || 1) * (W - L - R);
  const vals = d.v.filter(Number.isFinite);
  let vmin = Math.min(...vals), vmax = Math.max(...vals);
  if (vmin === vmax) { vmin -= 1; vmax += 1; }
  const pad = (vmax - vmin) * .05;
  const y = v => T + H1 - (v - vmin + pad) / (vmax - vmin + 2 * pad) * H1;
  const Z = 10, top2 = T + H1 + gap;
  const yz = z => top2 + H2 / 2 - Math.max(-Z, Math.min(Z, z)) / Z * (H2 / 2);
  const thr = d.threshold || 3;
  const line = (arr, fy) => {
    let s = '', pen = false;
    arr.forEach((v, i) => { if (v === null || v === undefined || !Number.isFinite(v)) { pen = false; return; } s += (pen ? 'L' : 'M') + x(d.t[i]).toFixed(1) + ' ' + fy(v).toFixed(1); pen = true; });
    return s;
  };
  const calEnd = d.cal.lastIndexOf(true);
  let svg = '';
  if (calEnd >= 0) svg += `<rect x="${L}" y="${T}" width="${x(d.t[calEnd]) - L}" height="${H1 + gap + H2}" fill="var(--cal)"/>`;
  d.windows.forEach(([a, b]) => { if (b >= t0 && a <= t1) svg += `<rect x="${x(Math.max(a, t0))}" y="${T}" width="${Math.max(2, x(Math.min(b, t1)) - x(Math.max(a, t0)))}" height="${H1 + gap + H2}" fill="var(--win)"/>`; });
  // axes and labels
  for (let k = 0; k <= 4; k++) {
    const v = vmin + (vmax - vmin) * k / 4;
    svg += `<line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}" stroke="var(--line)" stroke-width=".5"/><text x="${L - 4}" y="${y(v) + 3}" text-anchor="end">${+v.toPrecision(3)}</text>`;
  }
  [-thr, 0, thr].forEach(z => svg += `<line x1="${L}" x2="${W - R}" y1="${yz(z)}" y2="${yz(z)}" stroke="var(--line)" ${z ? 'stroke-dasharray="4 3"' : ''}/><text x="${L - 4}" y="${yz(z) + 3}" text-anchor="end">${z}</text>`);
  svg += `<text x="${L}" y="${top2 - 6}">z-score (clipped at ±${Z}); dashed = ±threshold</text>`;
  for (let k = 0; k <= 4; k++) { const t = t0 + (t1 - t0) * k / 4; svg += `<text x="${x(t)}" y="${H - 6}" text-anchor="${k === 0 ? 'start' : k === 4 ? 'end' : 'middle'}">${time(t).slice(0, 10)}</text>`; }
  svg += `<path d="${line(d.v, y)}" fill="none" stroke="var(--series)" stroke-width="1"/>`;
  svg += `<path d="${line(d.sz, yz)}" fill="none" stroke="var(--static)" stroke-width="1" opacity=".85"/>`;
  svg += `<path d="${line(d.rz, yz)}" fill="none" stroke="var(--rolling)" stroke-width="1" opacity=".85"/>`;
  d.t.forEach((t, i) => {
    // Rolling flags inside the calibration prefix are emitted but never evaluated: draw them faded.
    const fade = d.cal[i] ? ' opacity=".35"' : '';
    if (d.sa[i]) svg += `<rect x="${x(t) - 2}" y="${T + 1}" width="4" height="7" fill="var(--static)"/>`;
    if (d.ra[i]) svg += `<rect x="${x(t) - 2}" y="${T + 10}" width="4" height="7" fill="var(--rolling)"${fade}/>`;
    if (d.sa[i] || d.ra[i]) svg += `<circle cx="${x(t)}" cy="${y(d.v[i])}" r="2.5" fill="none" stroke="${d.ra[i] && !d.sa[i] ? 'var(--rolling)' : 'var(--static)'}"/>`;
  });
  const notes = showNotes ? chartNotes(d, { x, y, yz, L, R, W, T, H1, top2, thr }) : [];
  notes.forEach((n, k) => {
    const bx = Math.max(L + 10, Math.min(W - R - 10, n.bx)), by = Math.max(T + 10, Math.min(H - 30, n.by));
    if (n.ring) svg += `<circle cx="${n.ring[0]}" cy="${n.ring[1]}" r="9" fill="none" stroke="var(--note)" stroke-width="2.5"/>
      <line x1="${n.ring[0]}" y1="${n.ring[1]}" x2="${bx}" y2="${by}" stroke="var(--note)" stroke-width="1.5"/>`;
    svg += `<circle cx="${bx}" cy="${by}" r="10" fill="var(--note)" stroke="var(--bg)" stroke-width="2"/>
      <text x="${bx}" y="${by + 4}" text-anchor="middle" style="fill:#fff;font-size:12px;font-weight:700">${k + 1}</text>`;
  });
  svg += `<line id="hover" x1="0" x2="0" y1="${T}" y2="${top2 + H2}" stroke="var(--fg)" stroke-width=".5" visibility="hidden"/>`;
  el.innerHTML = `<div class="legend"><span><i style="background:var(--series)"></i>value</span><span><i style="background:var(--static)"></i>static z / alert</span>
    <span><i style="background:var(--rolling)"></i>rolling z / alert</span><span><i style="background:var(--win)"></i>NAB labelled window</span><span><i style="background:var(--cal)"></i>calibration prefix</span>
    <span class="muted">Top ticks: row 1 static alerts, row 2 rolling alerts (faded = inside calibration, not evaluated)</span></div>
    <div class="chart"><svg viewBox="0 0 ${W} ${H}" height="${H}">${svg}</svg></div>
    ${notes.length ? `<h3>Cách đọc biểu đồ (số xanh lá trên hình)</h3><ol class="notes">${notes.map((n, k) => `<li><span class="badge">${k + 1}</span><span>${n.text}</span></li>`).join('')}</ol>` : ''}`;
  const svgEl = el.querySelector('svg'), hover = el.querySelector('#hover'), tip = $('#tip');
  svgEl.addEventListener('mousemove', ev => {
    const r = svgEl.getBoundingClientRect(), px = (ev.clientX - r.left) * W / r.width;
    const target = t0 + (px - L) / (W - L - R) * (t1 - t0);
    let lo = 0, hi = d.t.length - 1;
    while (lo < hi) { const m = (lo + hi) >> 1; if (d.t[m] < target) lo = m + 1; else hi = m; }
    const i = lo > 0 && Math.abs(d.t[lo - 1] - target) < Math.abs(d.t[lo] - target) ? lo - 1 : lo;
    hover.setAttribute('x1', x(d.t[i])); hover.setAttribute('x2', x(d.t[i])); hover.setAttribute('visibility', 'visible');
    const inWin = d.windows.some(([a, b]) => a <= d.t[i] && d.t[i] <= b);
    tip.textContent = `#${i}  ${time(d.t[i])}${d.cal[i] ? '  [calibration]' : ''}${inWin ? '  [in window]' : ''}
value      ${fmt(d.v[i], 4)}
static     z=${fmt(d.sz[i], 2)}  anomaly=${fmt(d.sa[i])}
rolling    z=${fmt(d.rz[i], 2)}  anomaly=${fmt(d.ra[i])}  (${d.rr[i]})
baseline   mean=${fmt(d.rm[i], 4)} sd=${fmt(d.rs[i], 4)} n=${fmt(d.n[i])}`;
    tip.classList.remove('hidden');
    tip.style.left = Math.min(ev.clientX + 14, innerWidth - tip.offsetWidth - 8) + 'px';
    tip.style.top = (ev.clientY + 14) + 'px';
  });
  svgEl.addEventListener('mouseleave', () => { hover.setAttribute('visibility', 'hidden'); tip.classList.add('hidden'); });
}

function alertList(d) {
  const rows = [];
  d.t.forEach((t, i) => { if (d.sa[i] || d.ra[i]) rows.push(i); });
  if (!rows.length) return '<p class="muted">No alerts.</p>';
  return `<details><summary>${rows.length} alert rows</summary><div class="scroll"><table><tr><th>#</th><th>Time</th><th class="num">Value</th><th class="num">Static z</th><th>Static</th><th class="num">Rolling z</th><th>Rolling</th><th>Reason</th><th>In window</th></tr>
    ${rows.map(i => `<tr><td>${i}</td><td>${time(d.t[i])}</td><td class="num">${fmt(d.v[i], 4)}</td><td class="num">${fmt(d.sz[i], 2)}</td><td>${d.sa[i] ? '<b>alert</b>' : ''}</td>
    <td class="num">${fmt(d.rz[i], 2)}</td><td>${d.ra[i] ? '<b>alert</b>' : ''}</td><td>${esc(d.rr[i])}</td><td>${d.windows.some(([a, b]) => a <= d.t[i] && d.t[i] <= b) ? 'yes' : '<span class="bad">no</span>'}</td></tr>`).join('')}</table></div></details>`;
}

/* ----------------------------------------------------------------- series */
async function fillSeriesRuns() {
  const form = $('#series-form');
  const runs = await api('runs');
  const want = state.seriesRun || form.run.value || state.run;
  form.run.innerHTML = runs.map(r => `<option ${r.name === want ? 'selected' : ''}>${esc(r.name)}</option>`).join('');
  await fillSeriesMetrics();
  if (state.seriesRun) { state.seriesRun = null; loadSeries(); }
}
async function fillSeriesMetrics() {
  const form = $('#series-form');
  if (!form.run.value) return;
  const status = await api('runs/' + encodeURIComponent(form.run.value));
  form.metric.innerHTML = status.metrics.map(m => `<option>${esc(m)}</option>`).join('');
}
$('#series-form').run.addEventListener('change', fillSeriesMetrics);
$('#series-form').addEventListener('submit', e => { e.preventDefault(); loadSeries(); });

async function loadSeries() {
  const f = $('#series-form'), out = $('#series-out');
  out.innerHTML = '<p class="muted">Loading…</p>';
  try {
    const d = await api(`runs/${encodeURIComponent(f.run.value)}/series?metric=${encodeURIComponent(f.metric.value)}&source=${f.source.value}`);
    if (!d.t.length) { out.innerHTML = '<p class="muted">No scored rows for this metric.</p>'; return; }
    const st = d.statuses;
    out.innerHTML = `<p>${fmt(d.t.length)} rows · statuses ${esc(JSON.stringify(st))}</p>
      <p class="muted small annot">Nghĩa của dòng trên: ${fmt(st.calibration || 0)} điểm đầu dùng để "học" (calibration),
        ${fmt(st.accepted || 0)} điểm được chấm${st.control ? `, ${st.control} event đánh dấu hết luồng (không phải dữ liệu)` : ''}${st.rejected ? `, ${st.rejected} event bị loại` : ''}.
        Đây là kết quả ${f.source.value === 'reference' ? 'tính tuần tự không qua Spark (dùng để đối chiếu)' : 'Spark đã ghi ra (Parquet)'}.</p>
      ${verdict(d)}
      <div id="series-chart"></div>
      <h3>Quality for this series</h3>${qualityTable({ [d.metric]: d.quality })}${columnGuide()}${alertList(d)}`;
    state.series = d;
    chart($('#series-chart'), d, notesOn('series'));
  } catch (err) { out.innerHTML = `<p class="error">${esc(err.message)}</p>`; }
}

/* ------------------------------------------------------------- playground */
$('#play-form').addEventListener('submit', async e => {
  e.preventDefault();
  const f = new FormData(e.target), body = { metric: f.get('metric') };
  for (const k of ['fraction', 'tolerance', 'history_size', 'min_periods', 'threshold', 'batch_size']) body[k] = Number(f.get(k));
  const out = $('#play-out');
  out.innerHTML = '<p class="muted">Running…</p>';
  try {
    const d = await api('playground', body);
    const b = d.batch_check, c = d.calibration;
    out.innerHTML = `
      <p>${fmt(b.records)} events scored in ${d.elapsed_ms} ms (both passes). Static baseline: mean ${fmt(c.mean, 4)}, sd ${fmt(c.stddev, 4)} from first ${fmt(c.sample_count)} rows (to ${esc(c.end_timestamp)}).
      ${d.labeled ? '' : '<span class="bad">This file has no NAB labels.</span>'}</p>
      <p class="${b.mismatches ? 'bad' : 'ok'}"><b>${b.mismatches ? '✗' : '✓'} Micro-batch consistency:</b> ${b.batches} batches of ${b.batch_size} with JSON state restore between batches
        → ${b.mismatches} mismatching fields vs one pass (${fmt(b.batched_rows)} / ${fmt(b.single_rows)} rows).</p>
      ${verdict(d)}
      <div id="play-chart"></div><h3>Quality</h3>${qualityTable({ [d.metric]: d.quality })}${columnGuide()}${alertList(d)}`;
    state.play = d;
    chart($('#play-chart'), d, notesOn('playground'));
  } catch (err) { out.innerHTML = `<p class="error">${esc(err.message)}</p>`; }
});

function columnGuide() {
  return `<details class="help annot"><summary>Các cột trong bảng nghĩa là gì?</summary><table class="kv">
    <tr><th>Evaluated</th><td>Số điểm được chấm (đã bỏ vùng xám calibration).</td></tr>
    <tr><th>Windows</th><td>Số vùng sự cố thật (vùng hồng) nằm trọn trong đoạn được chấm.</td></tr>
    <tr><th>Hit</th><td>Số vùng hồng có ít nhất một báo động bên trong.</td></tr>
    <tr><th>Recall</th><td>Hit ÷ Windows: bắt được bao nhiêu phần sự cố (1 = 100%). “—” nghĩa là file không có vùng nào để bắt.</td></tr>
    <tr><th>False alerts</th><td>Số báo động nằm ngoài mọi vùng hồng, tức báo nhầm.</td></tr>
    <tr><th>per 1,000</th><td>Số báo nhầm trên mỗi 1,000 điểm được chấm, để so giữa các file có độ dài khác nhau.</td></tr>
    <tr><th>Median delay</th><td>Thời gian từ <i>mép trái</i> vùng hồng đến báo động đầu tiên. Vùng hồng của NAB vẽ rộng và bắt đầu sớm hơn sự cố, nên số này lớn không có nghĩa là phản ứng chậm.</td></tr>
  </table></details>`;
}

/* ------------------------------------------------- annotation on/off button */
// Per page; remembered in this browser only (storage may be unavailable).
const NOTE_KEY = 'webui-notes-';
function notesOn(page) {
  try { return localStorage.getItem(NOTE_KEY + page) !== 'off'; } catch { return true; }
}
function applyNotes(page) {
  const on = notesOn(page);
  $('#' + page).classList.toggle('notes-off', !on);
  const button = $(`.note-toggle[data-page="${page}"]`);
  button.textContent = on ? 'Ẩn chú thích' : 'Hiện chú thích';
  button.setAttribute('aria-pressed', String(on));
  const data = page === 'playground' ? state.play : state.series;
  const target = $(page === 'playground' ? '#play-chart' : '#series-chart');
  if (data && target) chart(target, data, on);
}
document.querySelectorAll('.note-toggle').forEach(button => {
  button.addEventListener('click', () => {
    const page = button.dataset.page;
    try { localStorage.setItem(NOTE_KEY + page, notesOn(page) ? 'off' : 'on'); } catch { /* keep default */ }
    applyNotes(page);
  });
  applyNotes(button.dataset.page);
});

// Plain-language reading of the quality numbers for this run.
function verdict(d) {
  const line = (name, q) => {
    if (!q.evaluated_records) return `<li><b>${name}</b>: không có điểm nào được chấm.</li>`;
    const caught = q.eligible_windows ? `bắt được <b>${q.detected_windows}/${q.eligible_windows}</b> sự cố` : 'không có sự cố nào để bắt';
    return `<li><b>${name}</b>: ${caught}, báo nhầm <b>${q.false_alerts}</b> lần (${fmt(q.false_alerts_per_1000, 2)} lần / 1,000 điểm).</li>`;
  };
  const s = d.quality.static, r = d.quality.rolling;
  let compare = '';
  if (s.false_alerts !== r.false_alerts)
    compare = `${s.false_alerts < r.false_alerts ? 'Static' : 'Rolling'} báo nhầm ít hơn với tham số này.`;
  if ((s.detected_windows ?? 0) !== (r.detected_windows ?? 0))
    compare += ` ${s.detected_windows > r.detected_windows ? 'Static' : 'Rolling'} bắt được nhiều sự cố hơn.`;
  return `<div class="summary"><b>Nhận xét nhanh</b><ul>${line('Static', s)}${line('Rolling', r)}</ul>${compare ? `<span>${compare.trim()}</span>` : ''}</div>`;
}

/* ----------------------------------------------------------------- checks */
async function loadChecks() {
  const out = $('#checks-out');
  out.innerHTML = '<p class="muted">Running…</p>';
  const checks = await api('checks');
  const passed = checks.filter(c => c.passed).length;
  const cols = ['event_id', 'record_status', 'reason', 'static_zscore', 'static_anomaly', 'rolling_mean', 'rolling_sample_count', 'rolling_zscore', 'rolling_anomaly', 'rolling_reason'];
  out.innerHTML = `<p class="${passed === checks.length ? 'ok' : 'bad'}"><b>${passed} / ${checks.length} scenarios behave as specified.</b></p>` +
    checks.map(c => `<details class="check"><summary><span class="${c.passed ? 'ok' : 'bad'}">${c.passed ? 'PASS' : 'FAIL'}</span> &nbsp;<b>${esc(c.title)}</b> — ${esc(c.explanation)}</summary><div>
      <p><b>Expected:</b> ${esc(c.expected)}</p>${c.error ? `<p class="error">${esc(c.error)}</p>` : ''}
      ${c.rows.length ? `<div class="scroll"><table><tr>${cols.map(k => `<th>${k}</th>`).join('')}</tr>${c.rows.map(r => `<tr>${cols.map(k => `<td>${fmt(r[k], 3)}</td>`).join('')}</tr>`).join('')}</table></div>` : ''}
    </div></details>`).join('');
}
$('#rerun-checks').addEventListener('click', loadChecks);

async function runTests(spark) {
  const out = $('#tests-out');
  out.classList.remove('hidden');
  out.textContent = 'Starting…';
  let job;
  try { job = await api('tests', { spark }); } catch (e) { out.textContent = e.message; return; }
  const poll = async () => {
    const j = await api('jobs/' + job.id);
    out.textContent = j.output + (j.state === 'running' ? '\n… running' : `\n[${j.state}, exit ${j.returncode}]`);
    out.scrollTop = out.scrollHeight;
    if (j.state === 'running') setTimeout(poll, 1000);
  };
  poll();
}
$('#unit-tests').addEventListener('click', () => runTests(false));
$('#spark-tests').addEventListener('click', () => runTests(true));

/* ------------------------------------------------------------------- boot */
syncMode();
route();
loadEnv().then(() => loadRuns());
loadDatasets();

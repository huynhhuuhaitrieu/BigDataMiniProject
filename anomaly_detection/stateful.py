"""Spark adapter: one stateful operator and one append file sink."""
import json
from .engine import MetricEngine
from .events import epoch_ms

OUTPUT_SCHEMA = 'payload string, run_id string, metric_id string, event_id string, record_status string, batch_started_ms long'


def build_detector(records, config, baselines):
    import pandas as pd
    from pyspark.sql.streaming.state import GroupStateTimeout

    def update(key, frames, state):
        engine = MetricEngine(config, baselines, json.loads(state.get[0]) if state.exists else None)
        watermark = state.getCurrentWatermarkMs() if config.mode == 'buffered' else -1
        started = state.getCurrentProcessingTimeMs()
        # Sorting spans all Pandas chunks in this invocation; never reset per chunk.
        def events():
            for frame in frames:
                for raw in frame['raw_json']:
                    try:
                        parsed = json.loads(raw)
                        yield parsed if isinstance(parsed, dict) else {}
                    except (TypeError, ValueError):
                        yield {}
        output = engine.process(events(), watermark)
        state.update((engine.dumps(),))
        if config.mode == 'buffered' and engine.state['pending']:
            next_time = min(epoch_ms(e['event_time']) for e in engine.state['pending'])
            state.setTimeoutTimestamp(max(next_time, watermark + 1))
        if output:
            yield pd.DataFrame([
                dict(payload=json.dumps(row, allow_nan=False), run_id=row.get('run_id'),
                     metric_id=row.get('metric_id'), event_id=row.get('event_id'),
                     record_status=row['record_status'], batch_started_ms=started)
                for row in output
            ], columns=['payload', 'run_id', 'metric_id', 'event_id', 'record_status', 'batch_started_ms'])

    if config.mode == 'buffered':
        records = records.withWatermark('watermark_time', f'{config.lateness_seconds} seconds')
    return records.groupBy('metric_key').applyInPandasWithState(
        update, outputStructType=OUTPUT_SCHEMA, stateStructType='engine_json string',
        outputMode='Append',
        timeoutConf=(GroupStateTimeout.EventTimeTimeout if config.mode == 'buffered'
                     else GroupStateTimeout.NoTimeout),
    )

"""Serializable per-metric state machine, independent of Spark runtime."""
import hashlib
import json
from .events import epoch_ms, validate_event
from .statistics import RollingHistory, score_event


class MetricEngine:
    def __init__(self, config, baselines, state=None):
        self.config = config
        self.baselines = baselines
        self.identity = config.version + ':' + hashlib.sha256(json.dumps(baselines, sort_keys=True).encode()).hexdigest()[:16]
        self.state = state or dict(identity=self.identity, run_id=None, metric_id=None,
                                   history=[], pending=[], last_key=None, last_sequence=-1)
        if self.state['identity'] != self.identity:
            raise ValueError('Detector or calibration changed: use a fresh checkpoint')
        self.history = RollingHistory(config.history_size, self.state['history'])

    def reject(self, event, reason):
        identity = {key: event.get(key) if isinstance(event.get(key), str) else None
                    for key in ('run_id', 'metric_id', 'event_id')}
        return dict(identity, record_status='rejected', reason=reason,
                    raw_event=repr(event), config_version=self.config.version)

    def process(self, events, watermark_ms=-1):
        output = []
        pending = self.state['pending']
        sequences = {e['sequence'] for e in pending}
        for raw in events:
            try:
                event = validate_event(raw)
            except (KeyError, ValueError, TypeError, OverflowError) as error:
                output.append(self.reject(raw if isinstance(raw, dict) else {}, f'invalid_event: {error}'))
                continue
            base_metric = event.get('source_metric_id', event['metric_id'])
            # Replicas are explicitly named; arbitrary aliases cannot borrow calibration.
            if event['metric_id'] != base_metric and not event['metric_id'].startswith(base_metric + '#replica='):
                output.append(self.reject(event, 'invalid_metric_alias'))
                continue
            if base_metric not in self.baselines:
                output.append(self.reject(event, 'unknown_metric'))
                continue
            if self.state['run_id'] is None:
                self.state.update(run_id=event['run_id'], metric_id=event['metric_id'])
            if event['run_id'] != self.state['run_id'] or event['metric_id'] != self.state['metric_id']:
                raise ValueError('Mixed run/metric identity in state: use a fresh checkpoint for each run')
            if event.get('is_control', False):
                output.append(dict(event, record_status='control'))
                continue
            key = [epoch_ms(event['event_time']), event['sequence']]
            if event['sequence'] <= self.state['last_sequence'] or event['sequence'] in sequences:
                output.append(self.reject(event, 'duplicate_or_stale_sequence'))
            elif self.state['last_key'] is not None and key <= self.state['last_key']:
                output.append(self.reject(event, 'out_of_order'))
            elif self.config.mode == 'buffered' and key[0] <= watermark_ms:
                output.append(self.reject(event, 'beyond_watermark'))
            elif self.config.mode == 'buffered' and len(pending) >= self.config.max_pending:
                # Backpressure must be configured so this is exceptional, not normal.
                output.append(self.reject(event, 'pending_overflow'))
            else:
                pending.append(dict(event))
                sequences.add(event['sequence'])
        pending.sort(key=lambda e: (epoch_ms(e['event_time']), e['sequence']))
        retained = []
        for event in pending:
            key = [epoch_ms(event['event_time']), event['sequence']]
            if self.config.mode == 'buffered' and key[0] > watermark_ms:
                retained.append(event)
                continue
            if event['sequence'] <= self.state['last_sequence'] or (self.state['last_key'] and key <= self.state['last_key']):
                output.append(self.reject(event, 'out_of_order'))
                continue
            baseline = self.baselines[event.get('source_metric_id', event['metric_id'])]
            output.append(score_event(event, self.history, baseline, self.config))
            self.state.update(last_key=key, last_sequence=event['sequence'])
        self.state.update(pending=retained, history=list(self.history.values))
        return output

    def dumps(self):
        return json.dumps(self.state, allow_nan=False, separators=(',', ':'))

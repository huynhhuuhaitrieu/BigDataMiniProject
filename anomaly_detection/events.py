"""Transport contract shared by replay, Spark and offline reference runs."""
from datetime import datetime, timezone
import math


def epoch_ms(value):
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def iso_time(value):
    return datetime.fromtimestamp(epoch_ms(value) / 1000, timezone.utc).isoformat()


def validate_event(event):
    for key in ('run_id', 'metric_id', 'event_id'):
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise ValueError(f'Missing {key}')
    if 'source_metric_id' in event and (not isinstance(event['source_metric_id'], str) or not event['source_metric_id']):
        raise ValueError('source_metric_id must be a nonempty string')
    if isinstance(event.get('sequence'), bool) or not isinstance(event.get('sequence'), int) or event['sequence'] < 0:
        raise ValueError('sequence must be a nonnegative integer')
    epoch_ms(event['event_time'])
    epoch_ms(event['sent_at'])
    if not isinstance(event.get('is_control', False), bool):
        raise ValueError('is_control must be boolean')
    if not event.get('is_control', False):
        if isinstance(event.get('value'), bool) or not math.isfinite(float(event['value'])):
            raise ValueError('value must be finite')
    return event

"""Incremental, numerically stable population statistics over prior values."""
from collections import deque
import math


class RollingHistory:
    def __init__(self, size, values=()):
        self.values = deque(maxlen=size)
        self.mean = 0.0
        self.m2 = 0.0
        self.updates = 0
        for value in values:
            self.append(float(value))

    def append(self, value):
        n = len(self.values)
        if n == self.values.maxlen:
            old = self.values.popleft()
            next_mean = self.mean + (self.mean - old) / (n - 1)
            self.m2 -= (old - self.mean) * (old - next_mean)
            self.mean = next_mean
            n -= 1
        delta = value - self.mean
        self.mean += delta / (n + 1)
        self.m2 += delta * (value - self.mean)
        self.values.append(value)
        self.updates += 1
        # Reconcile accumulated removal error periodically and whenever negative.
        if self.m2 < 0 or self.updates % 256 == 0:
            self.mean = math.fsum(self.values) / len(self.values)
            self.m2 = math.fsum((x - self.mean) ** 2 for x in self.values)

    @property
    def stddev(self):
        return math.sqrt(max(0.0, self.m2) / len(self.values)) if self.values else None


def decision(value, mean, sd, config, tolerance=None):
    if sd < config.epsilon:
        return None, abs(value - mean) > (config.absolute_tolerance if tolerance is None else tolerance), 'constant_baseline'
    z = (value - mean) / sd
    return z, abs(z) >= config.threshold, 'scored'


def score_event(event, history, baseline, config):
    """Score first; only then update this metric's rolling history."""
    value = float(event['value'])
    n = len(history.values)
    output = dict(event)
    output.update(rolling_mean=history.mean if n else None, rolling_stddev=history.stddev,
                  rolling_sample_count=n, rolling_zscore=None, rolling_anomaly=None,
                  rolling_reason='warmup', static_zscore=None, static_anomaly=None,
                  static_reason='missing_calibration', record_status='accepted',
                  config_version=config.version, calibration_id=None,
                  zscore_threshold=config.threshold)
    if n >= config.min_periods:
        z, flag, reason = decision(value, history.mean, history.stddev, config,
                                   baseline.get('absolute_tolerance') if baseline else None)
        output.update(rolling_zscore=z, rolling_anomaly=flag, rolling_reason=reason)
    if baseline:
        output['calibration_id'] = baseline['calibration_id']
        # All calibration records initialize rolling state, but are not evaluated.
        if int(event['sequence']) < baseline['sample_count']:
            output.update(record_status='calibration', static_reason='calibration')
        else:
            z, flag, reason = decision(value, baseline['mean'], baseline['stddev'], config,
                                       baseline.get('absolute_tolerance'))
            output.update(static_zscore=z, static_anomaly=flag, static_reason=reason)
    flags = [output['static_anomaly'], output['rolling_anomaly']]
    output['is_anomaly'] = any(x for x in flags if x is not None) if any(x is not None for x in flags) else None
    history.append(value)
    return output

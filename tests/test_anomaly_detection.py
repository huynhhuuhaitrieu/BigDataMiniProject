"""Contract tests cover failure modes beyond checking individual formulas."""
import json
import math
from pathlib import Path
import random
from statistics import mean, pstdev
import unittest

from anomaly_detection.config import DetectorConfig
from anomaly_detection.engine import MetricEngine
from anomaly_detection.evaluation import evaluate
from anomaly_detection.statistics import RollingHistory

BASELINES = {'metric': dict(mean=10., stddev=1., sample_count=2,
                            calibration_id='test', absolute_tolerance=.1)}


def event(index, value=10., metric='metric'):
    return dict(run_id='test', metric_id=metric, event_id=f'{metric}:{index}', sequence=index,
                event_time=f'2020-01-01T00:{index // 60:02d}:{index % 60:02d}Z',
                sent_at='2026-11-10T00:00:00Z', value=value, is_control=False)


class DetectionContracts(unittest.TestCase):
    def test_prior_history_and_flat_jump(self):
        config = DetectorConfig(history_size=5, min_periods=2, absolute_tolerance=.1)
        engine = MetricEngine(config, BASELINES)
        rows = engine.process([event(i, 10 if i < 4 else 15) for i in range(5)])
        self.assertIsNone(rows[0]['rolling_anomaly'])
        self.assertFalse(rows[2]['rolling_anomaly'])
        self.assertTrue(rows[4]['rolling_anomaly'])
        self.assertEqual(rows[4]['rolling_mean'], 10)
        self.assertEqual(rows[4]['rolling_sample_count'], 4)
        self.assertEqual(rows[4]['rolling_reason'], 'constant_baseline')

    def test_numerical_reference_with_eviction(self):
        rng = random.Random(3)
        values = [rng.gauss(1e6, 3) for _ in range(2000)]
        history = RollingHistory(100)
        for i, value in enumerate(values):
            history.append(value)
            expected = values[max(0, i - 99):i + 1]
            self.assertAlmostEqual(history.mean, mean(expected), places=7)
            self.assertAlmostEqual(history.stddev, pstdev(expected), places=7)

    def test_state_survives_batches_and_restart(self):
        config = DetectorConfig(history_size=20, min_periods=3)
        data = [event(i, 10 + math.sin(i)) for i in range(90)]
        expected = MetricEngine(config, BASELINES).process(data)
        engine = MetricEngine(config, BASELINES)
        actual = []
        for start in range(0, len(data), 7):
            actual.extend(engine.process(data[start:start + 7]))
            engine = MetricEngine(config, BASELINES, json.loads(engine.dumps()))
        for a, b in zip(expected, actual):
            self.assertEqual(a['event_id'], b['event_id'])
            self.assertEqual(a['rolling_anomaly'], b['rolling_anomaly'])
            if a['rolling_zscore'] is not None:
                self.assertAlmostEqual(a['rolling_zscore'], b['rolling_zscore'], places=10)

    def test_buffer_reorders_across_batches_and_drains(self):
        config = DetectorConfig(history_size=10, min_periods=2, mode='buffered')
        ordered = MetricEngine(DetectorConfig(history_size=10, min_periods=2), BASELINES).process([event(i, float(i)) for i in range(8)])
        engine = MetricEngine(config, BASELINES)
        self.assertEqual(engine.process([event(2, 2.), event(0, 0.), event(3, 3.)]), [])
        self.assertEqual(engine.process([event(1, 1.), event(7, 7.), event(5, 5.)]), [])
        self.assertEqual(engine.process([event(4, 4.), event(6, 6.)]), [])
        actual = engine.process([], watermark_ms=1577836808000)
        self.assertEqual([r['event_id'] for r in actual], [r['event_id'] for r in ordered])
        self.assertEqual([r['rolling_zscore'] for r in actual], [r['rolling_zscore'] for r in ordered])
        self.assertEqual(engine.state['pending'], [])

    def test_late_duplicate_invalid_and_unknown_do_not_pollute_history(self):
        config = DetectorConfig(history_size=5, min_periods=2)
        engine = MetricEngine(config, BASELINES)
        engine.process([event(0), event(2)])
        result = engine.process([event(2, 900), event(1, 900), event(3, float('nan')), event(4, 900, 'unknown')])
        self.assertTrue(all(r['record_status'] == 'rejected' for r in result))
        self.assertEqual(engine.state['history'], [10., 10.])

    def test_per_metric_isolation_and_config_guard(self):
        baselines = dict(BASELINES, other=BASELINES['metric'])
        a, b = MetricEngine(DetectorConfig(), baselines), MetricEngine(DetectorConfig(), baselines)
        a.process([event(0, 10)])
        b.process([event(0, 100, 'other')])
        self.assertEqual(a.state['history'], [10])
        self.assertEqual(b.state['history'], [100])
        with self.assertRaises(ValueError):
            MetricEngine(DetectorConfig(threshold=4), baselines, json.loads(a.dumps()))

    def test_window_evaluation_has_distinct_false_alert_and_hit_counts(self):
        records = [dict(event(i), record_status='accepted', static_anomaly=(i in [2, 3, 8]),
                        rolling_anomaly=False) for i in range(10)]
        windows = {'metric': [['2020-01-01T00:00:01Z', '2020-01-01T00:00:04Z']]}
        result = evaluate(records, windows)['aggregate']['static']
        self.assertEqual(result['detected_windows'], 1)
        self.assertEqual(result['false_alerts'], 1)
        self.assertEqual(result['window_recall'], 1)


if __name__ == '__main__':
    unittest.main()

"""Opt-in real Spark tests for Arrow serialization, file commits and checkpoints.

Run with Java 17, Python 3.11/3.12, dependencies installed, and RUN_SPARK_TESTS=1.
"""
import json
import os
from pathlib import Path
import tempfile
import unittest

from anomaly_detection.config import DetectorConfig
from anomaly_detection.engine import MetricEngine
from anomaly_detection.evaluation import performance, read_scored
from anomaly_detection.events import epoch_ms
from test_anomaly_detection import BASELINES, event


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1 for real Spark integration tests')
class SparkContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[2]').appName('anomaly-contract-tests')
            .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
            .config('spark.sql.shuffle.partitions', '2').config('spark.sql.session.timeZone', 'UTC').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def consume(self, root, config, baselines=BASELINES):
        from pyspark.sql import functions as F
        from anomaly_detection.stateful import build_detector
        from anomaly_detection.progress import progress_listener
        listener = progress_listener(root / 'progress.jsonl', root / 'checkpoint')
        self.spark.streams.addListener(listener)
        source = self.spark.readStream.format('text').option('maxFilesPerTrigger', 1).load(str(root / 'input'))
        source = (source.select(F.col('value').alias('raw_json'))
            .withColumn('metric_key', F.get_json_object('raw_json', '$.metric_id'))
            .withColumn('watermark_time', F.to_timestamp(F.get_json_object('raw_json', '$.event_time'))))
        query = (build_detector(source, config, baselines).writeStream.format('parquet').outputMode('append')
                 .option('path', str(root / 'output')).option('checkpointLocation', str(root / 'checkpoint'))
                 .trigger(availableNow=True).start())
        self.assertTrue(query.awaitTermination(90), 'Spark did not drain within 90 seconds')
        if query.isActive:
            query.stop()
        listener.await_termination()
        self.spark.streams.removeListener(listener)
        listener.close()
        return list(read_scored(root / 'output'))

    def test_restart_matches_sequential_reference_and_commits_once(self):
        config = DetectorConfig(history_size=5, min_periods=2)
        data = [event(i, 10. + (i % 3)) for i in range(30)]
        expected = MetricEngine(config, BASELINES).process(data)
        with tempfile.TemporaryDirectory(prefix='anomaly-spark-') as tmp:
            root = Path(tmp)
            (root / 'input').mkdir()
            (root / 'input/0.json').write_text('\n'.join(json.dumps(e) for e in data[:12]))
            first = self.consume(root, config)
            self.assertEqual(len(first), 12)
            (root / 'input/1.json').write_text('\n'.join(json.dumps(e) for e in data[12:]))
            rows = self.consume(root, config)
            self.assertEqual(len(rows), len(data))
            self.assertEqual(len({r['event_id'] for r in rows}), len(data))
            rows.sort(key=lambda e: e['sequence'])
            for actual, reference in zip(rows, expected):
                self.assertEqual(actual['rolling_anomaly'], reference['rolling_anomaly'])
                if actual['rolling_zscore'] is not None:
                    self.assertAlmostEqual(actual['rolling_zscore'], reference['rolling_zscore'], places=10)
            # A restart with no new input must not append duplicate rows.
            self.assertEqual(len(self.consume(root, config)), len(data))
            metrics = performance(rows, root / 'progress.jsonl')
            self.assertEqual(metrics['matched_records'], len(data) - 2)
            self.assertIsNotNone(metrics['p95_ms'])

    def test_watermark_timeout_drains_out_of_order_values(self):
        config = DetectorConfig(history_size=5, min_periods=2, mode='buffered', lateness_seconds=10)
        data = [event(i, 10. + (i % 3)) for i in range(10)]
        control = dict(event(30), is_control=True, value=None)
        with tempfile.TemporaryDirectory(prefix='anomaly-buffered-') as tmp:
            root = Path(tmp)
            (root / 'input').mkdir()
            (root / 'input/0.json').write_text('\n'.join(json.dumps(data[i]) for i in [2, 0, 3, 1, 5]))
            (root / 'input/1.json').write_text('\n'.join(json.dumps(data[i]) for i in [4, 7, 6, 9, 8]))
            (root / 'input/2.json').write_text(json.dumps(control))
            rows = self.consume(root, config)
            scored = sorted([r for r in rows if r['record_status'] in ('accepted', 'calibration')], key=lambda r: r['sequence'])
            self.assertEqual(len(scored), len(data))
            expected = MetricEngine(DetectorConfig(history_size=5, min_periods=2), BASELINES).process(data)
            self.assertEqual([r['rolling_anomaly'] for r in scored], [r['rolling_anomaly'] for r in expected])


if __name__ == '__main__':
    unittest.main()

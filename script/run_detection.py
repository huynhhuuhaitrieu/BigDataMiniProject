"""Run the separate Spark Structured Streaming anomaly pipeline."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys
import tempfile
import zipfile
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.calibration import load_calibration
from anomaly_detection.config import DetectorConfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--calibration', required=True)
    parser.add_argument('--topic', default='anomaly_input')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--master', default='local[2]')
    parser.add_argument('--shuffle-partitions', type=int, default=4)
    parser.add_argument('--max-offsets', type=int, default=5000)
    parser.add_argument('--trigger-seconds', type=int, default=1)
    parser.add_argument('--history-size', type=int, default=100)
    parser.add_argument('--min-periods', type=int, default=20)
    parser.add_argument('--threshold', type=float, default=3)
    parser.add_argument('--mode', choices=['ordered', 'buffered'], default='ordered')
    parser.add_argument('--lateness-seconds', type=int, default=600)
    parser.add_argument('--max-pending', type=int, default=10000)
    parser.add_argument('--output', required=True)
    parser.add_argument('--checkpoint', required=True)
    parser.add_argument('--progress', required=True)
    parser.add_argument('--json-input', help='Directory of replay JSONL files instead of Kafka (for local validation)')
    parser.add_argument('--max-files-per-trigger', type=int, default=1)
    parser.add_argument('--available-now', action='store_true')
    args = parser.parse_args()
    if min(args.shuffle_partitions, args.max_offsets, args.trigger_seconds, args.max_files_per_trigger) < 1:
        parser.error('partition counts, max offsets/files and trigger interval must be positive')
    if sys.version_info[:2] not in ((3, 11), (3, 12)):
        parser.error('Use Python 3.11 or 3.12 for this Spark 3.5.1 pipeline')
    config = DetectorConfig(history_size=args.history_size, min_periods=args.min_periods,
        threshold=args.threshold, mode=args.mode, lateness_seconds=args.lateness_seconds, max_pending=args.max_pending)
    artifact = load_calibration(args.calibration)
    checkpoint = Path(args.checkpoint).resolve()
    output = Path(args.output).resolve()
    if output == checkpoint or output in checkpoint.parents or checkpoint in output.parents:
        parser.error('output and checkpoint must be separate, non-nested directories')
    checkpoint.mkdir(parents=True, exist_ok=True)
    contract = dict(config=asdict(config), calibration=artifact, output=str(output),
                    input=args.json_input or dict(topic=args.topic, bootstrap=args.bootstrap_servers),
                    shuffle_partitions=args.shuffle_partitions, schema_version=1)
    contract_path = checkpoint / 'anomaly-contract.json'
    if contract_path.exists():
        if json.loads(contract_path.read_text()) != contract:
            parser.error('Checkpoint contract changed; use fresh checkpoint/output paths')
    else:
        with contract_path.open('x') as stream:
            json.dump(contract, stream, indent=2)
    from pyspark.sql import SparkSession, functions as F
    from anomaly_detection.stateful import build_detector
    from anomaly_detection.progress import progress_listener
    builder = (SparkSession.builder.appName('NABStatefulAnomalyDetection').master(args.master)
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', str(args.shuffle_partitions)))
    if args.master.startswith('local'):
        builder = builder.config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
    if not args.json_input:
        builder = builder.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    package_files = tempfile.TemporaryDirectory(prefix='anomaly-package-')
    archive = Path(package_files.name) / 'anomaly_detection.zip'
    package_root = Path(__file__).resolve().parents[1]
    with zipfile.ZipFile(archive, 'w') as bundle:
        for module in (package_root / 'anomaly_detection').glob('*.py'):
            bundle.write(module, module.relative_to(package_root))
    spark.sparkContext.addPyFile(str(archive))
    listener = progress_listener(args.progress, checkpoint)
    spark.streams.addListener(listener)
    query = None
    try:
        if args.json_input:
            source = (spark.readStream.format('text').option('maxFilesPerTrigger', args.max_files_per_trigger)
                      .load(args.json_input).select(F.col('value').alias('raw_json')))
        else:
            source = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', args.bootstrap_servers)
                .option('subscribe', args.topic).option('startingOffsets', 'earliest')
                .option('maxOffsetsPerTrigger', args.max_offsets).load()
                .select(F.col('value').cast('string').alias('raw_json')))
        records = (source.withColumn('metric_key', F.coalesce(F.get_json_object('raw_json', '$.metric_id'), F.lit('__invalid__')))
            .withColumn('watermark_time', F.to_timestamp(F.get_json_object('raw_json', '$.event_time'))))
        scored = build_detector(records, config, artifact['metrics'])
        writer = (scored.writeStream.format('parquet').outputMode('append')
            .option('path', str(output)).option('checkpointLocation', str(checkpoint)))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime=f'{args.trigger_seconds} seconds')
        query = writer.start()
        query.awaitTermination()
    except KeyboardInterrupt:
        if query:
            query.stop()
    finally:
        try:
            if query is not None:
                if query.isActive:
                    query.stop()
                listener.await_termination()
        finally:
            spark.streams.removeListener(listener)
            try:
                listener.close()
            finally:
                spark.stop()
                package_files.cleanup()


if __name__ == '__main__':
    main()

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def create_spark_connection():
    spark_conn = SparkSession.builder \
        .appName("NABKafkaStreaming") \
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1")) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        ) \
        .getOrCreate()
    spark_conn.sparkContext.setLogLevel("ERROR")
    return spark_conn


def connect_to_kafka(spark_conn):
    return spark_conn.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")) \
        .option("subscribe", os.environ.get("NAB_TOPIC", "nab_raw")) \
        .option("startingOffsets", "earliest") \
        .load()


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("sent_at", StringType(), True),
    ])

    return spark_df.selectExpr("CAST(value AS STRING) AS json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("timestamp").isNotNull() & col("value").isNotNull())


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    kafka_df = connect_to_kafka(spark_conn)
    nab_df = create_selection_df_from_kafka(kafka_df)

    query = nab_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", os.environ.get("NAB_OUTPUT_PATH", "output/nab")) \
        .option("checkpointLocation", os.environ.get("NAB_CHECKPOINT_PATH", "/tmp/nab-checkpoint")) \
        .start()
    query.awaitTermination()
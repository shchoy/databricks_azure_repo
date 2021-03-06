# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.window import Window

# COMMAND ----------

def create_stream_writer(
    dataframe: DataFrame,
    checkpoint: str,
    name: str,
    partition_column: str,
    mode: str = "append",
    mergeSchema: bool = False,
) -> DataStreamWriter:

    stream_writer = (
        dataframe.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .partitionBy(partition_column)
        .queryName(name)
    )

    if mergeSchema:
        stream_writer = stream_writer.option("mergeSchema", True)
    if partition_column is not None:
        stream_writer = stream_writer.partitionBy(partition_column)
    return stream_writer


# COMMAND ----------

def read_stream_delta(spark: SparkSession, deltaPath: str) -> DataFrame:
    return spark.readStream.format("delta").load(deltaPath)


# COMMAND ----------

def read_stream_raw(spark: SparkSession, rawPath: str) -> DataFrame:
    kafka_schema = "value STRING"
    return spark.readStream.format("text").schema(kafka_schema).load(rawPath)


# COMMAND ----------

def update_silver_table(spark: SparkSession, silverPath: str) -> bool:

    update_match = """
    health_tracker.eventtime = updates.eventtime
    AND
    health_tracker.device_id = updates.device_id
  """

    update = {"heartrate": "updates.heartrate"}

    dateWindow = Window.orderBy("p_eventdate")

    interpolatedDF = spark.read.table("health_tracker_plus_silver").select(
        "*",
        lag(col("heartrate")).over(dateWindow).alias("prev_amt"),
        lead(col("heartrate")).over(dateWindow).alias("next_amt"),
    )

    updatesDF = interpolatedDF.where(col("heartrate") < 0).select(
        "device_id",
        ((col("prev_amt") + col("next_amt")) / 2).alias("heartrate"),
        "eventtime",
        "name",
        "p_eventdate",
    )

    silverTable = DeltaTable.forPath(spark, silverPath)

    (
        silverTable.alias("health_tracker")
        .merge(updatesDF.alias("updates"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------

def transform_bronze(bronze: DataFrame) -> DataFrame:

    json_schema = "device_id INTEGER, heartrate DOUBLE, device_type STRING, name STRING, time FLOAT"

    return (
        bronze.select(from_json(col("value"), json_schema).alias("nested_json"))
        .select("nested_json.*")
        .select(
            "device_id",
            "device_type",
            "heartrate",
            from_unixtime("time").cast("timestamp").alias("eventtime"),
            "name",
            from_unixtime("time").cast("date").alias("p_eventdate"),
        )
    )


# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
        lit("files.training.databricks.com").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )


# COMMAND ----------

def transform_silver_mean_agg(silver: DataFrame) -> DataFrame:
    return silver.groupBy("device_id").agg(
        mean(col("heartrate")).alias("mean_heartrate"),
        stddev(col("heartrate")).alias("std_heartrate"),
        max(col("heartrate")).alias("max_heartrate"),
    )

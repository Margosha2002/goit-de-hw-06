from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    IntegerType,
)
from configs import kafka_config
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
    )
    .option("subscribe", f'{kafka_config["my_name"]}_building_sensors')
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .load()
)

json_schema = (
    StructType()
    .add("timestamp", DoubleType())
    .add("sensor_id", StringType())
    .add("temperature", IntegerType())
    .add("humidity", IntegerType())
)

clean_df = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn(
        "timestamp",
        from_unixtime(col("value_json.timestamp")).cast("timestamp"),
    )
    .withColumn("temperature", col("value_json.temperature"))
    .withColumn("humidity", col("value_json.humidity"))
    .drop("value_json", "value_deserialized")
)

clean_df.writeStream.trigger(availableNow=True).outputMode("append").format(
    "console"
).option("checkpointLocation", "/tmp/checkpoints/clean").start()

averages = (
    clean_df.withWatermark("timestamp", "10 seconds")
    .groupBy(window(clean_df.timestamp, "1 minutes", "30 seconds"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
)

temperature_alerts = averages.filter(col("avg_temperature") > 30).select(
    col("window.start").cast("string").alias("key"),
    to_json(
        struct(
            col("window").alias("window"),
            col("avg_temperature").alias("t_avg"),
            col("avg_humidity").alias("h_avg"),
            current_timestamp().alias("timestamp"),
        )
    ).alias("value"),
)

humidity_alerts = averages.filter(col("avg_humidity") > 70).select(
    col("window.start").cast("string").alias("key"),
    to_json(
        struct(
            col("window").alias("window"),
            col("avg_temperature").alias("t_avg"),
            col("avg_humidity").alias("h_avg"),
            current_timestamp().alias("timestamp"),
        )
    ).alias("value"),
)

temperature_alerts.writeStream.trigger(availableNow=True).format("kafka").outputMode(
    "update"
).option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]).option(
    "topic", f'{kafka_config["my_name"]}_temperature_alerts'
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config",
    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
).option(
    "checkpointLocation", "/tmp/temperature_alerts_3"
).start()

humidity_alerts.writeStream.trigger(availableNow=True).format("kafka").outputMode(
    "update"
).option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]).option(
    "topic", f'{kafka_config["my_name"]}_humidity_alerts'
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config",
    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
).option(
    "checkpointLocation", "/tmp/temperature_alerts_4"
).start()

try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"Error occurred: {e}")

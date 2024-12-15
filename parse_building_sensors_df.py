from pyspark.sql.functions import *
from building_sensors_schema import building_sensors_schema


def parse_building_sensors_df(df):
    clean_df = (
        df.selectExpr(
            "CAST(key AS STRING) AS key_deserialized",
            "CAST(value AS STRING) AS value_deserialized",
            "*",
        )
        .drop("key", "value")
        .withColumnRenamed("key_deserialized", "key")
        .withColumn(
            "value_json", from_json(col("value_deserialized"), building_sensors_schema)
        )
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

    return clean_df

from pyspark.sql.functions import *


def get_building_sensors_averages(df):
    return (
        df.withWatermark("timestamp", "10 seconds")
        .groupBy(window(df.timestamp, "1 minutes", "30 seconds"))
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
        )
    )

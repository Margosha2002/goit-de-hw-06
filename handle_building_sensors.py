import os
from pyspark.sql import SparkSession
from parse_building_sensors_df import parse_building_sensors_df
from get_building_sensors_averages import get_building_sensors_averages
from generate_alerts import generate_alerts
from send_alerts import send_alerts
from get_alerts_conditions import get_alerts_conditions
from get_building_sensors import get_building_sensors

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

df = get_building_sensors(spark)

clean_df = parse_building_sensors_df(df)

averages = get_building_sensors_averages(clean_df)

alerts_conditions = get_alerts_conditions(spark)

alerts = generate_alerts(alerts_conditions, averages)

send_alerts(alerts)

try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"Error occurred: {e}")

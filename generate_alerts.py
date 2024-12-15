from pyspark.sql.functions import *
from configs import kafka_config


def generate_alerts(alerts_df, data_df):
    alerts = []
    for row in alerts_df.collect():
        alert_condition = lit(True)
        topic = None
        if row["humidity_min"] != -999 & row["humidity_max"] != -999:
            alert_condition = alert_condition & (
                (col("avg_humidity") >= row["humidity_min"])
                & (col("avg_humidity") <= row["humidity_max"])
            )
            topic = f'{kafka_config["my_name"]}_humidity_alerts'

        if row["temperature_min"] != -999 & row["temperature_max"] != -999:
            alert_condition = alert_condition & (
                (col("avg_temperature") >= row["temperature_min"])
                & (col("avg_temperature") <= row["temperature_max"])
            )
            topic = f'{kafka_config["my_name"]}_temperature_alerts'

        if not topic:
            continue

        alerts.append(
            [
                topic,
                data_df.filter(alert_condition).select(
                    lit(row["code"]).alias("code"),
                    lit(row["message"]).alias("message"),
                    col("window").alias("window"),
                    col("avg_temperature").alias("t_avg"),
                    col("avg_humidity").alias("h_avg"),
                    current_timestamp().alias("timestamp"),
                ),
            ]
        )
    return alerts

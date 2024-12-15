from pyspark.sql.functions import *
from send_alert import send_alert


def send_alerts(alerts):
    for alert in alerts:
        topic = alert[0]
        alert_df = alert[1]

        send_alert(
            alert_df.select(
                col("window.start").cast("string").alias("key"),
                to_json(
                    struct(
                        col("window").alias("window"),
                        col("t_avg").alias("t_avg"),
                        col("h_avg").alias("h_avg"),
                        col("code").alias("code"),
                        col("message").alias("message"),
                        current_timestamp().alias("timestamp"),
                    )
                ).alias("value"),
            ),
            topic,
        )

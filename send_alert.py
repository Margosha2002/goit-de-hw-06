from configs import kafka_config
import random


def send_alert(alert_df, kafka_topic_prefix):
    query = (
        alert_df.writeStream.trigger(availableNow=True)
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        )
        .option(
            "checkpointLocation",
            f"/tmp/{kafka_topic_prefix}_alerts_{random.randint(1, 1000)}",
        )
        .option("topic", kafka_topic_prefix)
        .start()
    )
    return query

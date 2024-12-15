from configs import kafka_config


def get_building_sensors(spark):
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

    return df

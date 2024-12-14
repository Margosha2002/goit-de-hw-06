from kafka.admin import NewTopic, KafkaAdminClient
from configs import kafka_config


def create_topic(topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
    )

    topic_name = f"{kafka_config['my_name']}_{topic_name}"
    num_partitions = 2
    replication_factor = 1

    new_topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        error_message = getattr(e, "message", None)
        if error_message:
            print(error_message)
        else:
            print(str(e))

    admin_client.close()

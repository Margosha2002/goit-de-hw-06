from kafka import KafkaConsumer
from configs import kafka_config
import json
import asyncio


async def consume_event(topic_name, callback):
    def consume_event_sync():
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            security_protocol=kafka_config["security_protocol"],
            sasl_mechanism=kafka_config["sasl_mechanism"],
            sasl_plain_username=kafka_config["username"],
            sasl_plain_password=kafka_config["password"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda v: v.decode("utf-8"),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="my_consumer_group_3",
        )

        topic_name_full = f'{kafka_config["my_name"]}_{topic_name}'
        consumer.subscribe([topic_name_full])

        print(f"Subscribed to topic '{topic_name_full}'")

        try:
            for message in consumer:
                callback(message.value)
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            consumer.close()

    await asyncio.to_thread(consume_event_sync)

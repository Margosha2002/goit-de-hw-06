from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
import asyncio


def produce_event(topic_name, data):
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    topic_name = f"{kafka_config['my_name']}_{topic_name}"

    try:
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()
        # print(f"Message {data} sent to topic '{topic_name}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()


async def produce_building_sensors(sensor_id):
    print("Started producing random data")

    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85),
        }
        produce_event("building_sensors", data)
        await asyncio.sleep(2)

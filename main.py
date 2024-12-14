import asyncio

from create_topic import create_topic
from producer import produce_building_sensors
from consumer import consume_event
import random


def handle_alert(data):
    print(data)


async def main():
    create_topic("building_sensors")
    create_topic("temperature_alerts")
    create_topic("humidity_alerts")

    sensor_id = random.randint(1, 1000)

    producer = asyncio.create_task(produce_building_sensors(sensor_id))
    consumer_temperature_alerts = asyncio.create_task(
        consume_event("temperature_alerts", handle_alert)
    )
    consumer_humidity_alerts = asyncio.create_task(
        consume_event("humidity_alerts", handle_alert)
    )
    await asyncio.gather(
        producer,
        consumer_temperature_alerts,
        consumer_humidity_alerts,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")

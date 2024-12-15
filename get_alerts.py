import csv

with open("data/alerts_conditions.csv", "r") as file:
    reader = csv.DictReader(file)
    alerts_data = [row for row in reader]


def get_alerts(temperature, humidity):
    alerts = []

    for condition in alerts_data:
        humidity_min = float(condition["humidity_min"])
        humidity_max = float(condition["humidity_max"])
        temperature_min = float(condition["temperature_min"])
        temperature_max = float(condition["temperature_max"])
        message = condition["message"]
        code = condition["code"]

        if (
            humidity_min != -999
            and humidity_max != -999
            and humidity >= humidity_min
            and humidity <= humidity_max
        ):
            alerts.append({"message": message, "code": code})

        if (
            temperature_min != -999
            and temperature_max != -999
            and temperature >= temperature_min
            and temperature <= temperature_max
        ):
            alerts.append({"message": message, "code": code})

    return alerts

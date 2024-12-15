from alerts_conditions_schema import alerts_conditions_schema


def get_alerts_conditions(spark):
    alerts_conditions = spark.read.csv(
        "data/alerts_conditions.csv", schema=alerts_conditions_schema, header=True
    )

    alerts_conditions = alerts_conditions.fillna(
        {
            "humidity_min": -999,
            "humidity_max": -999,
            "temperature_min": -999,
            "temperature_max": -999,
        }
    )

    return alerts_conditions

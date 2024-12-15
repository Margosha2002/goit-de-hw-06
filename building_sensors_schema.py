from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    IntegerType,
)

building_sensors_schema = (
    StructType()
    .add("timestamp", DoubleType())
    .add("sensor_id", StringType())
    .add("temperature", IntegerType())
    .add("humidity", IntegerType())
)

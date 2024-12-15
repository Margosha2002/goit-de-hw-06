from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
)

alerts_conditions_schema = (
    StructType()
    .add("id", IntegerType())
    .add("humidity_min", IntegerType())
    .add("humidity_max", IntegerType())
    .add("temperature_min", IntegerType())
    .add("temperature_max", IntegerType())
    .add("code", IntegerType())
    .add("message", StringType())
)

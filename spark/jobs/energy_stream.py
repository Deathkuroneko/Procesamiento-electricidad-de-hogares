from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType, TimestampType
# 1️⃣ Spark Session
spark = SparkSession.builder \
    .appName("EnergyConsumptionStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Esquema del mensaje (AJÚSTALO A TU CSV)


schema = StructType([
    StructField("datetime", TimestampType()),
    StructField("Global_active_power", DoubleType()),
    StructField("Global_reactive_power", DoubleType()),
    StructField("Voltage", DoubleType()),
    StructField("Global_intensity", DoubleType()),
    StructField("Sub_metering_1", DoubleType()),
    StructField("Sub_metering_2", DoubleType()),
    StructField("Sub_metering_3", DoubleType())
])


# 3️⃣ Leer desde Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "energy-consumption") \
    .option("startingOffsets", "latest") \
    .load()

# 4️⃣ Kafka value (bytes → string → json)
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

# 5️⃣ Mostrar en consola (DEBUG)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

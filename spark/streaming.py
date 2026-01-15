from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("EnergyStreamingKappa") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/energy.streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Esquema del mensaje Kafka
# -----------------------------
schema = StructType([
    StructField("Datetime", StringType()),
    StructField("Global_active_power", DoubleType()),
    StructField("Global_reactive_power", DoubleType()),
    StructField("Voltage", DoubleType()),
    StructField("Global_intensity", DoubleType()),
    StructField("Sub_metering_1", DoubleType()),
    StructField("Sub_metering_2", DoubleType()),
    StructField("Sub_metering_3", DoubleType())
])

# -----------------------------
# Leer desde Kafka
# -----------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "energy-consumption") \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------
# Kafka value -> JSON -> columnas
# -----------------------------
json_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# -----------------------------
# Convertir timestamp
# -----------------------------
stream_df = json_df.withColumn(
    "timestamp",
    col("Datetime").cast("timestamp")
)

# -----------------------------
# Análisis: consumo promedio por hora
# -----------------------------
avg_hourly = stream_df \
    .groupBy(
        window(col("timestamp"), "1 hour")
    ) \
    .agg(
        avg("Global_active_power").alias("avg_power")
    )

# -----------------------------
# Guardar histórico en HDFS
# -----------------------------
hdfs_query = stream_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/energy/raw") \
    .option("checkpointLocation", "hdfs://namenode:9000/energy/checkpoints/raw") \
    .outputMode("append") \
    .start()

# -----------------------------
# Guardar resultados en MongoDB
# -----------------------------
mongo_query = avg_hourly.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoints/mongo") \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ---------------------------------------------------------
# 1. Spark Session (Spark 4.1.0 + Kafka + MongoDB)
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("EnergyStreamingKappa") \
    .master("spark://spark:7077") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
        "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0"
    ) \
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://mongodb:27017/energy.streaming"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# 2. Esquema del mensaje Kafka
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# 3. Lectura desde Kafka
# ---------------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "energy-consumption") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------------------------------------
# 4. Kafka value -> JSON -> columnas
# ---------------------------------------------------------
json_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ---------------------------------------------------------
# 5. Conversión de timestamp + limpieza
# ---------------------------------------------------------
stream_df = json_df \
    .withColumn("timestamp", col("Datetime").cast("timestamp")) \
    .filter(col("timestamp").isNotNull()) \
    .withWatermark("timestamp", "10 minutes")

# ---------------------------------------------------------
# 6. Procesamiento: promedio por hora
# ---------------------------------------------------------
avg_hourly = stream_df \
    .groupBy(
        window(col("timestamp"), "1 hour")
    ) \
    .agg(
        avg("Global_active_power").alias("avg_power")
    ) \
    .select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("avg_power")
    )

# ---------------------------------------------------------
# 7. Función de escritura a MongoDB (foreachBatch)
# ---------------------------------------------------------
def write_to_mongo(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "energy") \
            .option("collection", "consumo_promedio") \
            .save()

# ---------------------------------------------------------
# 8. Flujo A: histórico completo en HDFS
# ---------------------------------------------------------
hdfs_query = stream_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/energy/raw") \
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/energy/checkpoints/raw"
    ) \
    .outputMode("append") \
    .start()

# ---------------------------------------------------------
# 9. Flujo B: agregados a MongoDB
# ---------------------------------------------------------
mongo_query = avg_hourly.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("update") \
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/energy/checkpoints/mongo"
    ) \
    .start()

print(">>> Streaming Kappa iniciado correctamente.")

spark.streams.awaitAnyTermination()

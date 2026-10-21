from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, avg, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("MonitoreoTransaccionesTiempoReal") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema JSON de los mensajes
schema = StructType([
    StructField("id_transaccion", StringType(), True),
    StructField("id_cuenta", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("ciudad", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("timestamp", StringType(), True)
])

print("=== INICIANDO CONSUMIDOR SPARK STREAMING EN TIEMPO REAL ===")

# 3. Lectura continua desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacciones_bancarias") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Deserialización del mensaje JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# 5. Procesamiento y Clasificación en Tiempo Real
df_processed = df_parsed.withColumn(
    "nivel_riesgo",
    when(col("monto") >= 1000000, "ALTO RIESGO / ALERTA FRAUDE")
    .otherwise("NORMAL")
)

# 6. Agregación en tiempo real: Métricas financieras acumuladas por Tipo de Transacción
df_metrics = df_processed.groupBy("tipo", "nivel_riesgo") \
    .agg(
        count("*").alias("total_eventos"),
        _sum("monto").alias("monto_total_cop"),
        avg("monto").alias("monto_promedio")
    )

# 7. Visualización en consola en tiempo real (Update Mode)
query = df_metrics.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

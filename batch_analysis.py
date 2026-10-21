import json
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, desc, upper

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisAtencionesSaludRIPS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== CONECTANDO Y DESCARGANDO DATOS DESDE LA API DE DATOS ABIERTOS ===")

# 2. Endpoint de la API
api_url = "https://www.datos.gov.co/resource/4k9h-8qiu.json?$limit=50000"

req = urllib.request.Request(api_url, headers={'User-Agent': 'Mozilla/5.0'})
response = urllib.request.urlopen(req)
data_json = json.loads(response.read().decode('utf-8'))

# Cargar en Spark DataFrame a partir del JSON
rdd_json = spark.sparkContext.parallelize([json.dumps(record) for record in data_json])
df_raw = spark.read.json(rdd_json)

print(f"\n--- Total de registros cargados desde la API: {df_raw.count()} ---")
print("\n--- Esquema del dataset ---")
df_raw.printSchema()

# 3. Limpieza y Transformaciones
# Eliminamos registros nulos en diagnóstico, departamento y municipio
df_clean = df_raw.dropna(subset=["diagnostico", "departamento", "tipoatencion"]) \
                 .dropDuplicates()

# Convertimos la columna 'numeroatenciones' a tipo entero
df_transformed = df_clean.withColumn("atenciones_num", col("numeroatenciones").cast("integer")) \
                         .withColumn("departamento_fmt", upper(col("departamento"))) \
                         .withColumn("tipoatencion_fmt", upper(col("tipoatencion")))

print(f"\nTotal de registros listos tras limpieza: {df_transformed.count()}")

# 4. Análisis Exploratorio de Datos (EDA)

# KPI 1: Top 10 Diagnósticos Médicos con mayor volumen de atenciones
print("\n--- TOP 10 DIAGNÓSTICOS MÁS FRECUENTES ---")
df_transformed.groupBy("diagnostico") \
    .agg(_sum("atenciones_num").alias("total_atenciones")) \
    .sort(desc("total_atenciones")) \
    .show(10, truncate=False)

# KPI 2: Distribución de Atenciones por Departamento
print("\n--- ATENCIONES MÉDICAS POR DEPARTAMENTO ---")
df_transformed.groupBy("departamento_fmt") \
    .agg(_sum("atenciones_num").alias("total_atenciones")) \
    .sort(desc("total_atenciones")) \
    .show(10, truncate=False)

# KPI 3: Atenciones por Tipo de Atención (Urgencias, Consulta, etc.)
print("\n--- DISTRIBUCIÓN POR TIPO DE ATENCIÓN ---")
df_transformed.groupBy("tipoatencion_fmt") \
    .agg(_sum("atenciones_num").alias("total_atenciones")) \
    .sort(desc("total_atenciones")) \
    .show(truncate=False)

# 5. Finalizar sesión de Spark
spark.stop()
print("\n=== PROCESAMIENTO BATCH DESDE API CONCLUIDO EXITOSAMENTE ===")

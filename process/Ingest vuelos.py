# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storage_name","rsmdsdldevsdletl")
dbutils.widgets.text("container","bronze")
dbutils.widgets.text("catalogo","catalog_dev")
dbutils.widgets.text("esquema","bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/datos_vuelos_historicos_complex.csv"

# COMMAND ----------

df_vuelos = spark.read.option("header",True)\
    .option("inferSchema",True)\
    .csv(ruta)

# COMMAND ----------

vuelos_schema = StructType([
    StructField("ID_Vuelo", StringType(), False),  
    StructField("Aerolinea", StringType(), True),
    StructField("Origen", StringType(), False),
    StructField("Destino", StringType(), False),
    StructField("Fecha_Programada", DateType(), True),  
    StructField("Hora_Salida_UTC", StringType(), True), 
    StructField("Distancia_KM", IntegerType(), True), 
    StructField("Capacidad_Pasajeros", IntegerType(), True),
    StructField("Pasajeros_Reservados", IntegerType(), True), 
    StructField("Clase_Tarifa", StringType(), True),
    StructField("Costo_Base", FloatType(), True),      
    StructField("Impuestos", FloatType(), True),
    StructField("Tipo_Avion", StringType(), True)
])

# COMMAND ----------

df_vuelos_clean = spark.read.option("header",True)\
    .schema(vuelos_schema)\
    .csv(ruta)

# COMMAND ----------

df_clean = df_vuelos_clean.select(
    col("ID_Vuelo").alias("id_vuelo"),
    col("Aerolinea").alias("info_aerolinea"),
    col("Origen").alias("geo_origen"),
    col("Destino").alias("geo_destino"),
    col("Fecha_Programada").alias("fecha_programada"),
    col("Hora_Salida_UTC").alias("hora_salida_utc"),
    col("Distancia_KM").alias("op_distancia_km"),
    col("Capacidad_Pasajeros").alias("capacidad_pasajeros"),
    col("Pasajeros_Reservados").alias("op_pasajeros_reservados"),
    col("Clase_Tarifa").alias("tarifa_clase"),
    col("Costo_Base").alias("tarifa_costo_base"),
    col("Impuestos").alias("tarifa_impuestos"),
    col("Tipo_Avion").alias("op_tipo_avion")
)

vuelos_final_df = df_clean.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

vuelos_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.vuelos")

# COMMAND ----------



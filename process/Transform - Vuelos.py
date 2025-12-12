# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink","silver")

# COMMAND ----------

catalog = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_vuelo = spark.table(f"{catalog}.{esquema_source}.vuelos")

# COMMAND ----------

df_transform = df_vuelo.withColumn(
    "analisis_anio",
    year(col("fecha_programada"))
).withColumn(
    "analisis_mes",
    month(col("fecha_programada"))
).withColumn(
    "analisis_dia_semana",
    dayofweek(col("fecha_programada")) 
)

df_transform.display()

# COMMAND ----------

df_transform = df_transform.withColumn(
    "tarifa_precio_total_ticket",
    round(col("tarifa_costo_base") + col("tarifa_impuestos"), 2)
)

# COMMAND ----------

UMBRAL_BAJO_COSTO = 400.00

df_transform = df_transform.withColumn(
    "indicador_es_bajo_costo",
    when(col("tarifa_precio_total_ticket") < UMBRAL_BAJO_COSTO, True)
    .otherwise(False)
)

df_transform.display()

# COMMAND ----------

df_transform = df_transform.withColumn(
    "geo_id_ruta",
    concat(col("geo_origen"), lit("-"), col("geo_destino"))
)

# COMMAND ----------

FACTOR_KM_TO_MILES = 0.621371

df_transform = df_transform.withColumn(
    "op_distancia_millas",
    round(col("op_distancia_km") * lit(FACTOR_KM_TO_MILES), 2)
)

# COMMAND ----------

UMBRAL_ALTA_CAPACIDAD = 300 

df_transform = df_transform.withColumn(
    "indicador_alta_capacidad",
    when(col("capacidad_pasajeros") >= UMBRAL_ALTA_CAPACIDAD, lit(1))
    .otherwise(lit(0)) 
)

# COMMAND ----------

df_transform.write.mode("overwrite").saveAsTable(f"{catalog}.{esquema_sink}.vuelos")

# COMMAND ----------


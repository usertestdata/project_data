# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, avg, count, round
dbutils.widgets.removeAll()


# COMMAND ----------

dbutils.widgets.text("catalogo","catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink","golden")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_vuelos_transformed = spark.table(f"{catalogo}.{esquema_source}.vuelos")

# COMMAND ----------

DIMENSIONES_GOLD = [
    col("analisis_anio").alias("dim_anio"),
    col("analisis_mes").alias("dim_mes"),
    col("info_aerolinea").alias("dim_aerolinea"),
    col("geo_id_ruta").alias("dim_ruta_id"),
]


METRICAS_GOLD = [
    
    round(sum(col("tarifa_precio_total_ticket")), 2).alias("gld_ingreso_total"),
    count(col("id_vuelo")).alias("gld_vuelos_contados"),
    
    
    sum(col("op_pasajeros_reservados")).alias("gld_pasajeros_totales"),
    sum(col("capacidad_pasajeros")).alias("gld_capacidad_total"),
    

    sum(when(col("indicador_es_bajo_costo") == True, lit(1)).otherwise(lit(0))).alias("gld_vuelos_bajo_costo"),
]


df_gold_agregado = df_vuelos_transformed.groupBy(DIMENSIONES_GOLD).agg(*METRICAS_GOLD)


df_transformed = df_gold_agregado.withColumn(
    "gld_load_factor_porc",
    round(coalesce(col("gld_pasajeros_totales") / col("gld_capacidad_total"), lit(0)) * lit(100), 2)
).withColumn(
    "gld_precio_promedio_ticket",
    round(coalesce(col("gld_ingreso_total") / col("gld_pasajeros_totales"), lit(0)), 2)
)

print("DataFrame Gold - Esquema Agregado para BI:")
df_transformed.printSchema()

# COMMAND ----------

df_transformed.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.vuelos")



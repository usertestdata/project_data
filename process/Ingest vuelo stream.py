# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("container","streaming")
dbutils.widgets.text("storageLocation","rsmdsdldevsdletl")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink","gold")

# COMMAND ----------

container = dbutils.widgets.get("container")
storageLocation = dbutils.widgets.get("storageLocation")

catalog = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")


# COMMAND ----------

ruta = f"abfss://{container}@{storageLocation}.dfs.core.windows.net"
ruta

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="accessScopeforADLS")

# COMMAND ----------



connectionString = dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-connectionString1")
eventHubName = dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-eventHubName1")

# COMMAND ----------

dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-connectionString1")
dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-eventHubName1")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Layer

# COMMAND ----------


ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}


# COMMAND ----------


df = spark.readStream \
    .format("eventhubs") \
    .options(**{**ehConf}) \
    .load() \


df.display()

# COMMAND ----------

# Writing stream: Persist the streaming data to a Delta table 'streaming.bronze' in 'append' mode with checkpointing
df.writeStream\
    .option("checkpointLocation", f"{ruta}/FileStore/tables/bronze/vuelos")\
    .outputMode("append")\
    .format("delta")\
    .toTable("catalog_streaming.bronze.vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Layer

# COMMAND ----------

schema_tracking = StructType([
    StructField("ID_Vuelo", StringType(), True),
    StructField("Timestamp_Evento", TimestampType(), True), 
    StructField("Estado_Actual", StringType(), True),
    StructField("Retraso_Minutos", IntegerType(), True),
    StructField("Puerta_Asignada", StringType(), True),
    StructField("Ruta_Evento", StringType(), True),
    StructField("Nivel_Servicio", IntegerType(), True),
    StructField("Consumo_Combustible_Litros", FloatType(), True)
])

# COMMAND ----------



df_parsed = (
    spark.readStream
    .format("delta")
    .table("catalog_streaming.bronze.vuelos")
    .withColumn("body", col("body").cast("string")) 
    .withColumn(
        "data", 
        from_json(col("body"), schema_tracking) 
    )
    .select(col("data.*")) 
)


df_parsed.display()



# COMMAND ----------

df_parsed.writeStream\
    .option("checkpointLocation", f"{ruta}/FileStore/tables/bronze/vuelos_parsed")\
    .outputMode("append")\
    .format("delta")\
    .toTable("catalog_streaming.silver.vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold Layer

# COMMAND ----------

df_simples = spark.table(f"{catalog}.{esquema_source}.vuelos")
df_simples


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, upper, trim, when, lit


def process_batch(current_df, batch_id):

    
    if current_df.isEmpty():
        print(f"Batch {batch_id}: Lote vacío, omitido.")
        return

   
    
    df_limpio_tracking = current_df.filter(
        # 1. Integridad de la Clave: Filtrar nulos, vacíos y corruptos
        (col("ID_Vuelo").isNotNull()) & (trim(col("ID_Vuelo")) != "") & (~col("ID_Vuelo").contains("_ERR")) 
    ).filter(
        # 2. Filtrar Timestamps nulos
        col("Timestamp_Evento").isNotNull() 
    ).withColumn(
      
        "estado_actual_normalizado", upper(trim(col("Estado_Actual"))) 
    ).withColumn(
       
        "tracking_retraso_minutos",
        when(
            (col("estado_actual_normalizado") == "RETRASADO") & 
            (col("Retraso_Minutos").isNull() | (col("Retraso_Minutos") <= 0)),
            lit(MIN_RETRASO_CORRECCION) 
        ).otherwise(col("Retraso_Minutos"))
    ).withColumn(
     
        "indicador_retraso_alto",
        when(col("tracking_retraso_minutos") >= UMBRAL_RETRASO_ALTO, True).otherwise(False) 
    )



    window_spec = Window.partitionBy("ID_Vuelo").orderBy(col("Timestamp_Evento").desc())

  
    df_con_rn = df_limpio_tracking.withColumn("rn", row_number().over(window_spec))


    df_ultimo_estado = df_con_rn.filter(col("rn") == 1).drop("rn")


    df_batch_unido = df_simples.join( 
        df_ultimo_estado.select(tracking_cols_final), 
        df_simples.id_vuelo == col("tracking_key"),
        how="left"
    ).drop("tracking_key")

    (df_batch_unido 
        .write
        .format("delta")
        .mode("append")
        .option("checkpointLocation", f"{ruta}/FileStore/tables/final_write") 
        .saveAsTable("catalog_streaming.gold.vuelos")
    )

    
    print(f"Batch {batch_id}: Vuelos procesados, unidos y escritos con éxito.")


(df_parsed 
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", f"{ruta}/FileStore/tables/stream_starter") 
    .start()
)

# COMMAND ----------


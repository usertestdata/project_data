# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storageName","rsmdsdldevsdletl")
dbutils.widgets.text("catalogo","catalog_dev")
dbutils.widgets.text("nameContainer","metastore")

# COMMAND ----------


storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
nameContainer = dbutils.widgets.get("nameContainer")

ruta = f"abfss://{nameContainer}@{storageName}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminacion tablas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_dev.bronze.vuelos;
# MAGIC DROP TABLE IF EXISTS catalog_streaming.bronze.vuelos;

# COMMAND ----------

dbutils.fs.rm(f"{ruta}/tablas/vuelos", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminacion tablas Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS catalog_dev.silver.vuelos;
# MAGIC DROP TABLE IF EXISTS catalog_streaming.silver.vuelos;

# COMMAND ----------

# REMOVE DATA (Silver)
dbutils.fs.rm(f"{ruta}/tablas/VENTAS", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminacion tablas Golden

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS catalog_dev.golden.vuelos;
# MAGIC DROP TABLE IF EXISTS catalog_streaming.gold.vuelos;

# COMMAND ----------

# REMOVE DATA (Golden)

dbutils.fs.rm(f"{ruta}/tablas/vuelos", True)

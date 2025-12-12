# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog_dev TO `analistas`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_dev.bronze TO `ingeniero_datos`;
# MAGIC GRANT CREATE ON SCHEMA catalog_dev.bronze TO `ingeniero_datos`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE catalog_dev.bronze.vuelos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA catalog_dev.bronze;

# COMMAND ----------


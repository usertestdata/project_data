-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storageName default "rsmdsdldevsdletl";
CREATE WIDGET TEXT container DEFAULT "streaming";

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_dev.silver;
CREATE SCHEMA IF NOT EXISTS catalog_dev.golden;
CREATE SCHEMA IF NOT EXISTS catalog_dev.exploratory;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
URL 'abfss://gold@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-styreaming`
URL 'abfss://streaming@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

DROP CATALOG IF EXISTS streaming CASCADE;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_streaming;
CREATE SCHEMA IF NOT EXISTS catalog_streaming.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_streaming.silver;
CREATE SCHEMA IF NOT EXISTS catalog_streaming.gold;

-- COMMAND ----------


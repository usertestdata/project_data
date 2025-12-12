
# ✈️ Plataforma de Ingeniería de Datos: Vuelos Aéreo - ETL Híbrido

![Descripción de la imagen](https://github.com/usertestdata/project_data/blob/main/extras/terraform/img/arquitectura.png?raw=true)


## 📜 1. Visión General del Proyecto

Este proyecto establece una **Plataforma de Ingeniería de Datos Híbrida** diseñada para analizar el rendimiento operativo y la rentabilidad de vuelos. La solución utiliza una **Lakehouse Architecture** en Databricks, gestionada por **Terraform** e orquestada por **Azure Data Factory (ADF)**.

El sistema procesa dos flujos de datos esenciales:

1.  **Dataset 1 (Batch):** Datos históricos de tarifas, capacidad y rentabilidad.
2.  **Dataset 2 (Streaming):** Eventos en tiempo casi real de seguimiento operativo y estado de vuelos (Event Hubs).

El objetivo es implementar y probar los diferentes servicios de azure enfocado a la ingenieria de datos.

La utilizacion de terraform ayuda a levantar algunos sevicios pero requiere configurcacion posterior que se mencionara en el apartado de "Ejecución del Pipeline"

Todos los datos son random generados con un script de python. el codigo de terrafrom y script de agregara en una carpeta llamada "extras"
-----

## 💻 2. Tecnologías Clave

| Categoría | Herramienta | Uso en el Proyecto |
| :--- | :--- | :--- |
| **Infraestructura (IaC)** | **Terraform** | Despliegue automatizado de todos los recursos en Azure: ADLS Gen2, Azure Data Factory (ADF), Azure Key Vault (AKV) y el *Workspace* de Databricks. |
| **Orquestación** | **Azure Data Factory (ADF)** | Gestión del flujo de datos, ejecución programada de *notebooks* de Databricks (Batch) y monitorización continua del *pipeline* de *streaming*. |
| **Procesamiento** | **Databricks (PySpark)** | Limpieza, transformación ELT/ETL y agregación de datos.  |
| **Streaming** | **Azure Event Hubs** | Ingesta de datos operativos de vuelos en tiempo casi real (Dataset 2). |
| **Almacenamiento** | **Delta Lake** | Implementación de las capas Bronze, Silver y Gold. |
| **Visualización** | **Databricks SQL** | Creación del *dashboard* final que consume la Capa Gold. |

-----

## 🌊 3. Arquitectura del Lakehouse (Capas de Datos)

El flujo de datos sigue el estándar Lakehouse (Medallion Architecture) en Delta Lake:

### 3.1. 🥉 Capa BRONZE (Datos Crudos)

  * **Descripción:** Zona de aterrizaje que contiene la réplica exacta de las fuentes de datos.
  * **Contenido:** Archivos CSV históricos (generado con script) y eventos binarios sin procesar de Event Hubs.

### 3.2. 🥈 Capa SILVER (Limpieza y Enriquecimiento)

  * **Descripción:** Datos limpios, normalizados y listos para la agregación.
  * **Transformaciones Clave:** Limpieza de nulos, conversión de tipos, normalización de `Estado_Actual`, cálculo de KPIs iniciales y parsing de JSON del *stream*.

### 3.3. 🥇 Capa GOLD (Métricas de Negocio)

  * **Descripción:** Tablas agregadas y optimizadas para el consumo de BI.
  * **Tablas Resultantes:**
    1.  **`rendimiento_historico_vuelos`:** Agregación del **Dataset 1** (Batch) por `Aerolínea` y `Mes/Año`. Métricas: `SUM(ingreso_total)`, `AVG(load_factor)`.
    2.  **`estado_operacional_actual`:** Resultado del *pipeline* de **Streaming**. Ofrece el **último estado operativo** de cada vuelo activo, unido con sus datos históricos de rentabilidad.

-----

## ⚙️ 4. Pipelines de Procesamiento y Lógica Clave

El proyecto utiliza un **proceso híbrido** para gestionar la actualización continua y la agregación histórica.

### 4.1. ➡️ Pipeline Batch (Dataset 1)

  * **Mecanismo:** Procesamiento y escritura de datos estáticos (`df.write.format("delta").saveAsTable(...)`).
  * **Lógica de Gold:** Agregación final (`groupBy` y `agg`) de las métricas de rentabilidad y capacidad para alimentar los *reports* de tendencia.

### 4.2. 🚀 Pipeline Streaming Híbrido (Dataset 2)

  * **Mecanismo:** `Structured Streaming` con `foreachBatch`.
  * **Lógica Clave (dentro de `process_batch`):**
    1.  **Selección del Último Estado:** Se utiliza una **función de ventana (`ROW_NUMBER()`)** sobre el *batch* estático (`current_df`) para identificar y seleccionar el evento más reciente (`Timestamp_Evento`) por `ID_Vuelo`, resolviendo el problema de duplicación.
    2.  **Unión Híbrida:** Se realiza un **Left Join** entre el último estado operativo (Streaming) y la tabla estática de Silver (Dataset 1).

-----

## 📈 5. Visualización y Métricas de Negocio

El **Dashboard de Databricks SQL** ofrece una visión unificada de las métricas críticas:

  * **Cantidad de vuelvos (Histórico):** Análisis de cantidad de vuelos realizados.
  * **Pasajeros por mes:** Cantidad de pasajeros por mes.
 
  
-----

## 🛠️ 6. Despliegue y Ejecución

### 6.1. Requisitos

  * Terraform CLI, Azure CLI.
  * Credenciales de un *Service Principal* de Azure.

### 6.2. Despliegue de Infraestructura (Terraform)

Debes configurar el archivo viables con el valor. la variable "prefix" es importante. ya que algunos servicio de azure requiere nombres únicos.

```bash
# Inicialización 
terraform init

# Vista previa de los cambios a realizar
terraform plan

# Aplicación de los cambios
terraform apply
```


### 6.3. Ejecución del Pipeline

1. **Configuración de Secretos:** Las credenciales de Event Hubs deben configurarse a través de Azure Key Vault y referenciarse en Databricks mediante **Secret Scopes**.
2. **Configuración de conexión de Data Factory:** Configurar y validar la conexión de Data Factory al workspace de Databricks.
3. **Configuración del conector de Databricks:** El conector de Databricks debe tener permisos de Contributor Storage Blob en el bucket del metastore.
4. **Arranque:** Azure Data Factory activa los *notebooks* de Batch. El notebook de streaming se debe ejecutar por separado desde Databricks.


### 7. Pendientes

1. **Terraform:** Como se menciona en la ejecución del pipeline, hay configuraciones manuales que se deben realizar. Quedaron pendientes de agregar como mejora para seguir investigando. También mencionar que solo se usó el proveedor de Azure, pero también existe el de Databricks, que puede ser una mejora para incorporar en alguna versión futura del proyecto.

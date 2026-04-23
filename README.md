# Amazon Product Recommendation Engine (ALS on Apache Spark)

## 1. Introducción y Justificación Técnica
Este proyecto implementa un motor de recomendación a gran escala utilizando el dataset de **Amazon Product Data (UCSD)**. La problemática central abordada es la **escalabilidad** en sistemas de recomendación masivos.

### El Desafío del Big Data
Al enfrentarnos a un dataset con millones de usuarios y productos, el sistema debe gestionar una **Matriz de Interacción Dispersa (Sparse Matrix)**:
* **Limitación de Memoria:** Una matriz de $10^6$ usuarios por $10^6$ productos generaría un volumen de datos inmanejable para arquitecturas de un solo nodo.
* **Procesamiento de JSON:** El uso de archivos JSON masivos requiere una etapa de ETL (Extract, Transform, Load) distribuida para evitar cuellos de botella en la lectura de datos no estructurados.

## 2. Arquitectura del Sistema
El sistema se divide en tres capas críticas:

1.  **Capa de Ingestión:** Procesamiento de archivos JSON mediante el motor de **Spark SQL**, transformando datos anidados en esquemas fuertemente tipados.
2.  **Capa de Procesamiento (ETL):** * Limpieza de registros nulos y duplicados.
    * **Indexación de IDs:** Transformación de identificadores alfanuméricos (ASIN/UserIDs) a índices numéricos mediante `StringIndexer`, requisito técnico para los tensores de Spark MLlib.
3.  **Capa de Modelado:** Implementación del algoritmo **Alternating Least Squares (ALS)** para filtrado colaborativo.

## 3. El Algoritmo: Alternating Least Squares (ALS)
Se ha seleccionado ALS por su capacidad nativa de paralelización en clusters distribuidos. 

### Fundamento Matemático
ALS descompone la matriz de utilidad $R$ en dos matrices de menor rango: $U$ (usuarios) y $P$ (productos), de modo que:
$$R \approx U \times P^T$$

El algoritmo minimiza la función de pérdida por mínimos cuadrados de forma alterna: fija $U$ para optimizar $P$, y luego fija $P$ para optimizar $U$. Este enfoque permite que cada partición del cluster trabaje en un subconjunto de vectores latentes de forma independiente, optimizando el uso de la CPU.

**Hiperparámetros configurados:**
* **Rank:** Cantidad de factores latentes (capacidad de abstracción del modelo).
* **RegParam:** Parámetro de regularización para mitigar el sobreajuste (overfitting).
* **ColdStartStrategy:** Configurado en `"drop"` para asegurar la robustez del evaluador ante usuarios sin historial previo.

## 4. Stack Tecnológico
* **Lenguaje:** Python (PySpark).
* **Procesamiento:** Apache Spark (DataFrames & MLlib).
* **Storage de Origen:** Dataset de Amazon Reviews (JSON).
* **Storage de Destino:** Formato Parquet (columnar y comprimido).

## 5. Pipeline de Ejecución

### Fase 1: ETL y Limpieza
```python
# Ejemplo de carga y limpieza distribuida
df = spark.read.json("path/to/amazon_reviews.json")
df_clean = df.select("reviewerID", "asin", "overall").dropna()
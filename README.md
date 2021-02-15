# PruebaIndiretail_JoaquinSantini
Repo donde reside el código fuente de la notebook Databricks que se solicita en el documento "PruebaTecnidaDataEngineer.docx"


# Estructura del repositorio
Se presentan 4 carpetas, "data" , "output", "src" y "test".

En la carpeta "data" se encuentran los archivos .parquet originales desde donde se corre la notebook solicitada.

En la carpeta "output" se encuentran los archivos .parquet solicitados. El proceso NO los sube al resositorio, sino que los genera dentro del FS de Databricks.

En la carpeta "src" se encuentran 2 versiones del mismo proceso en 2 formatos distintos:
- 1) Versión pyspark: notebook que en la mayor parte de la primera parte de la solución se utilizó pyspark puro. La notebook se exportó en 2 formatos: .py y .dbc
- 2) Versión sparksql: notebook donde se utilizó sparksql para la gran mayoría de las tareas. La notebook se exportó en 2 formatos: .py y .dbc

La decisión de generar el mismo proceso en 2 versiones distintas se debe a que no se aclaró en la consigna si se prefería un lenguaje en particular, por ende se combinaron pyspark y sparksql. La performance de ambas notebooks es similar, dado que no existen diferencias entre ejecutar una celda en pyspark y sparksql, entendiendo que la única forma de obtener mejor performance es con RDD puros. Se tomó como referencia para la justificación el siguiente artículo: https://community.cloudera.com/t5/Community-Articles/Spark-RDDs-vs-DataFrames-vs-SparkSQL/ta-p/246547

En la carpeta "test" se encuentra la notebook de testing en 2 formatos distintos: .py y .dbc


# Algunas aclaraciones sobre la entrega

### Entorno

Databricks Runtime Version: 7.5
Spark Version: 3.0.1
Driver Type: Community Optimized 15.3GB Memory, 2 Cores (1 DBU)


### Espacio libre en DBFS

Cuando los archivos se bajan desde el repositorio, se necesitan al menos 7MB libres. Cuando se generan los datos solicitados por el cliente, se necesitan al menos 28MB libres. En la notebook se muestra cómo se calcularon los 28MB de memoria necesarios.


### Documentación en Notebook

Dentro de cada respectiva notebook, se encuentran documentadas algunas decisiones en cuanto a lo que se analizó como opciones y se incluye también una sección para los precios de venta nulos.


### Tiempo de Proceso

Las notebooks tienen un tiempo medio de proceso de 6 minutos aproximadamente en el entorno antes descripto.
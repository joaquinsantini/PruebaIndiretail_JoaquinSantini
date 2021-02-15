# Databricks notebook source
# MAGIC %md
# MAGIC ## Autor: Joaquín Santini
# MAGIC ## Descripción: La notebook genera los archivos parquet solicitados en la carpeta /output/ del FS de Databricks.
# MAGIC <br>
# MAGIC ### Secciones de la notebook
# MAGIC - Objetivo
# MAGIC - Paso 1: Descarga de Archivos
# MAGIC - Paso 2: Cálculo del Stock Diario
# MAGIC - 2.1 Columna código raíz de producto
# MAGIC - 2.2 Suma de Unidades
# MAGIC - 2.3 Cálculo de Columna Stock
# MAGIC - 2.4 Cálculo de Stock al 01/01/2019
# MAGIC - 2.5.1 Cálculo de Stock por Intervalos de Fecha
# MAGIC - 2.5.2 Cálculo de Stock Diario Propagado a Todas las Fechas
# MAGIC - Paso 3: KPIs
# MAGIC - 3.1 Beneficios a Nivel Tienda y Año
# MAGIC - 3.2 Beneficios a Nivel Familia y Año
# MAGIC - 3.3 Rotación a Nivel Tienda y Año
# MAGIC - 3.4 Rotación a Nivel Familia y Año
# MAGIC - Paso 4: Generación de Ficheros Parquet en el DBFS
# MAGIC - Análisis de productos sin precio de venta
# MAGIC - Cálculo de Espacio Requerido para los Ficheros Generados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objetivo
# MAGIC 
# MAGIC La empresa Indiretail nos ha encargado crear una aplicación que calcule el stock diario y una serie de KPIs, y así saber cómo ha sido la evolución del stock y las ventas en su empresa y el impacto del COVID.
# MAGIC 
# MAGIC Se proporcionan los siguientes datos de 2019 y 2020:
# MAGIC - Movimientos de stock: las entradas son valores positivos y las salidas valores negativos.
# MAGIC - Ventas
# MAGIC - Maestro de productos

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 1: Se bajan los archivos desde el repositorio de GitHub y se cargan en distintos DataFrames
# MAGIC 
# MAGIC #### NOTA: Se tiene la suposición que en el FS de Databricks hay al menos 7MB de memoria libres para que los archivos puedan ser descargados, sino la siguiente celda dará error

# COMMAND ----------

import urllib.request

# Bajo los archivos desde GitHub
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/data/products.parquet?raw=true", "/tmp/products.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/data/sales.parquet?raw=true", "/tmp/sales.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/data/stock_movements.parquet?raw=true", "/tmp/stock_movements.parquet")

# Los muevo al DBFS
dbutils.fs.mv("file:/tmp/products.parquet", "dbfs:/data/products.parquet")
dbutils.fs.mv("file:/tmp/sales.parquet", "dbfs:/data/sales.parquet")
dbutils.fs.mv("file:/tmp/stock_movements.parquet", "dbfs:/data/stock_movements.parquet")

# Cargo los DataFrame
df_products = spark.read.format('parquet').load('dbfs:/data/products.parquet')
df_sales = spark.read.format('parquet').load('dbfs:/data/sales.parquet')
df_stock_movements = spark.read.format('parquet').load('dbfs:/data/stock_movements.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2: Cálculo del stock diario
# MAGIC 
# MAGIC A partir de los movimientos de stock hay que calcular el stock diario desde el 01/01/2019, a nivel de código raíz de producto

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) Añadir a la tabla de movimientos de stock el código raíz del producto
# MAGIC 
# MAGIC **IMPORTANTE:** los productos con movimientos de stock que no tengan un código raíz asignado **deben ser eliminados**

# COMMAND ----------

# Registro como tablas temporales los DataFrames
df_products.createOrReplaceTempView('df_products')
df_sales.createOrReplaceTempView('df_sales')
df_stock_movements.createOrReplaceTempView('df_stock_movements')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aclaración: Se considera hacer un INNER JOIN porque con un LEFT JOIN se iban a incluir movimientos de stock de productos NO presentes en el maestro y por consigna se pide EXPLÍCITAMENTE que se descarten

# COMMAND ----------

df_stock_movements = df_stock_movements.join(df_products, "ProductId").select('StoreId', 'ProductId', 'Date', 'Quantity', 'ProductRootCode')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Agregar la tabla a código raíz del producto, tienda y fecha, sumando las unidades

# COMMAND ----------

from pyspark.sql.functions import col

df_stock_agg = df_stock_movements.groupBy('ProductRootCode', 'StoreId', 'Date').sum('Quantity').select('ProductRootCode', 'StoreId', 'Date', col('sum(Quantity)').alias('Quantity'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Acumular los movimientos de stock para obtener el stock en esa fecha, a nivel de código raíz del producto, tienda y fecha
# MAGIC 
# MAGIC #### Aclaración: al particionar por código raíz de producto y tienda, debemos ordenar de forma ASCENDENTE las fechas para que la función agregada SUM() tenga sentido y vaya acumulando el campo Quantity en forma de Stock

# COMMAND ----------

from pyspark.sql.functions import sum as sum_spark
from pyspark.sql.window import Window

df_stock_agg = df_stock_agg.select('ProductRootCode', 'StoreId', 'Date', 'Quantity', sum_spark('Quantity').over(Window.partitionBy('ProductRootCode', 'StoreId').orderBy('Date')).alias('Stock'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Calcular el stock inicial de cada código raíz de producto – tienda a 01/01/2019 y mantener las fechas posteriores a 01/01/2019 y su stock
# MAGIC 
# MAGIC #### Aclaración: el análisis se divide en 2 partes, donde se filtra primero los registros con fechas menores o iguales al 01/01/2019, ya que como se solicita actualizar el stock a esa fecha, debemos calcular la máxima fecha de TODOS los registros anteriores a dicha fecha para "convertirlo" en stock a ese momento. Cuando se calcula las máximas fechas, nos quedamos con ese registro "más actual" en el tiempo y lo unimos con TODOS los otros registros restantes, ya que se pide EXPLÍCITAMENTE que no se modifiquen los mismos

# COMMAND ----------

# Agrupo por código raíz de producto y tienda para quedarme con la fecha más alta previa al 01/01/2019
from pyspark.sql.functions import col, max as max_dt

df_stock_max_fecha = df_stock_agg.filter(col('Date') <= '2019-01-01').groupBy('ProductRootCode', 'StoreId').agg(max_dt("Date")).select('ProductRootCode', 'StoreId', col('max(Date)').alias('Date'))

# COMMAND ----------

# Los registros menores al 01/01/2019 se convierten en Stock al 01/01/2019, el resto de los registros se mantienen como estaban previamente
df_union_1 = df_stock_agg.join(df_stock_max_fecha, ['ProductRootCode', 'StoreId', 'Date']).selectExpr('ProductRootCode', 'StoreId', "CAST('2019-01-01' AS DATE) AS Date", 'Stock')
df_union_2 = df_stock_agg.filter(col('Date') > '2019-01-01').select('ProductRootCode', 'StoreId', 'Date', 'Stock')

df_stock_agg = df_union_1.union(df_union_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.1) Para cada código raíz de producto – tienda, los intervalos de fechas y el stock en ese intervalo. Se considera el 31/12/2020, como última fecha
# MAGIC 
# MAGIC #### Aclaración: para generar los intervalos de tiempo, debemos saber por cada fecha cuál es la que le sigue inmediatamente. Para ello, la solución que se pensó fue JOINNEAR consigo misma la tabla y tener del lado "izquierdo" la fecha T y del lado "derecho" la fecha T + 1. Para poder realizar dicho JOIN, se particionan los registros por código raíz de producto y tienda, se ordena por fecha ASCENDENTE y generamos el ROW_NUMBER(). Esto nos genera los valores T para cada fecha y podemos realizar el cálculo

# COMMAND ----------

from pyspark.sql.functions import row_number

df_stock_agg = df_stock_agg.filter(col('Date') <= '2020-12-31').select('ProductRootCode', 'StoreId', 'Date', 'Stock', row_number().over(Window.partitionBy('ProductRootCode', 'StoreId').orderBy('Date')).alias('num_registro'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aclaración 1: se elige realizar un LEFT JOIN a diferencia de un INNER JOIN porque el registro que pertenece a la ÚLTIMA fecha no tendrá el registro T + 1 anteriormente mencionado, por ende, como de ese lado "derecho" el registro será nulo, se considera el IFNULL y por default esa fecha de fin de período será el 31/12/2020 (último día del período solicitado)
# MAGIC 
# MAGIC #### Aclaración 2: se descartan los registros con Stock <= a cero porque se pide EXPLÍCITAMENTE que se excluyan

# COMMAND ----------

# Con los registros enumerados, genero los intervalos de fecha
df_interval_stock = df_stock_agg.filter(col('Stock') > 0)\
                                .alias('s1')\
                                .join(df_stock_agg.alias('s2'), (col('s1.ProductRootCode') == col('s2.ProductRootCode')) & (col('s1.StoreId') == col('s2.StoreId')) & (col('s1.num_registro') == col('s2.num_registro') - 1), how = 'left')\
                                .selectExpr('s1.ProductRootCode', 's1.StoreId', "s1.Date AS StartDate", "IFNULL(s2.Date - INTERVAL 1 DAYS, CAST('2020-12-31' AS DATE)) AS EndDate", 's1.Stock')

df_interval_stock.createOrReplaceTempView('df_interval_stock')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2) El stock diario propagado a todas las fechas
# MAGIC 
# MAGIC #### Aclaración: a fin de realizar la operación de forma performante en Spark, se elige utilizar una vista lateral. Las mismas fueron introducidas como un feature en las últimas versiones de Spark para justamente generar a partir de un ARRAY de datos, una columna nueva que se sumará a nuestra tabla de origen. Es parecido a un CROSS JOIN, sólo que en este caso podemos generar nuestro ARRAY con funciones propias de Spark que ya están optimizadas como por ejemplo POSEXPLODE. Al utilizar DATEDIFF formamos una lista de fechas para cada día entre cada intervalo. Convirtiendo dicha lista en una vista lateral, podemos generar la "propagación" de días solicitada

# COMMAND ----------

# Genero una vista lateral con la diferencia en días entre las fechas de inicio y fin; con una vista temporal puedo propagar a cada día del intervalo
query = """
SELECT  s.ProductRootCode,
        s.StoreId,
        DATE_ADD(s.StartDate, v.f) AS Date,
        s.Stock
        FROM df_interval_stock s
        LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(s.EndDate, s.StartDate)), ' ')) v AS f, c"""

df_daily_stock = spark.sql(query)
df_daily_stock.createOrReplaceTempView('df_daily_stock')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3: KPIs
# MAGIC 
# MAGIC El cliente nos pide que calculemos los siguientes KPIs:
# MAGIC - Los beneficios. El cálculo es el siguiente -> Beneficio = (Unidades vendidas × Precio venta al público) - (Entradas Stock × Coste del producto)
# MAGIC - La rotación. El cálculo es el siguiente -> Rotación = (Unidades vendidas en el periodo) / (Stock medio en el periodo)
# MAGIC 
# MAGIC Ambos KPIs hay que entregarlos:
# MAGIC - A nivel de tienda y año, para 2019 y 2020
# MAGIC - A nivel de familia de producto y año, para 2019 y 2020
# MAGIC 
# MAGIC #### Aclaración: para el cálculo de los KPIs, se utilizó SPARK SQL ya que las queries son más visuales para los desarrolladores que estén leyendo el código y como ya se aclaró en el README del repositorio, la performance no sufre grandes cambios respecto a PYSPARK puro

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) Beneficios a nivel tienda y año
# MAGIC 
# MAGIC #### Aclaración: se decide partir el análisis de este KPI en 2 partes. Primero se generan para cada tienda las Ventas y los Costos por año. En segundo lugar, se agrupa por tienda y año para generar lo que se solicitó por consigna. El CROSS JOIN utilizado se deben a que pueden existir productos para los cuales no hubo ventas, como así también pueden existir productos para los cuales no hubo movimiento de stock. Además, si no partimos de los años 2019 y 2020, podemos estar dejando afuera registros; por ejemplo de tiendas que en algún año no hayan tenido ventas o costos. En dicha situación, habría que realizar más cruces (LEFT e INNER) y esto hace decaer la performance del proceso

# COMMAND ----------

# Genero las ventas y los costos agrupados por tienda y año
query = """
SELECT  s.StoreId,
        YEAR(s.Date) AS Anio,
        SUM(s.Quantity * p.RetailPrice) AS TotalVentas
        FROM df_sales s
        INNER JOIN df_products p ON s.ProductId = p.ProductId AND p.RetailPrice IS NOT null
        GROUP BY s.StoreId, YEAR(s.Date)"""

df_sales_agg_t = spark.sql(query)
df_sales_agg_t.createOrReplaceTempView('df_sales_agg_t')

query = """
SELECT  m.StoreId,
        YEAR(m.Date) AS Anio,
        SUM(m.Quantity * p.SupplierPrice) AS TotalCostos
        FROM df_stock_movements m
        INNER JOIN df_products p ON m.ProductId = p.ProductId AND p.RetailPrice IS NOT null
        WHERE m.Quantity > 0
        GROUP BY m.StoreId, YEAR(m.Date)"""

df_stock_movements_agg_t = spark.sql(query)
df_stock_movements_agg_t.createOrReplaceTempView('df_stock_movements_agg_t')

# COMMAND ----------

# Genero el beneficio por tienda y producto para 2019 y 2020, teniendo en cuenta TODOS los productos presentes en el maestro con precio de venta al público informado
query = """
SELECT  stores.StoreId,
        p_anio.anio,
        IFNULL(s.TotalVentas, 0) - IFNULL(m.TotalCostos, 0) AS Beneficio
        FROM (SELECT 2019 AS anio UNION SELECT 2020 AS anio) p_anio
        CROSS JOIN (SELECT StoreId FROM df_sales UNION SELECT StoreId FROM df_stock_movements) stores
        LEFT JOIN df_sales_agg_t s ON stores.StoreId = s.StoreId AND p_anio.anio = s.Anio
        LEFT JOIN df_stock_movements_agg_t m ON stores.StoreId = m.StoreId AND p_anio.anio = m.Anio"""

df_store_benefits = spark.sql(query)
df_store_benefits.createOrReplaceTempView('df_store_benefits')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Beneficios a nivel familia de producto y año

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aclaración: al igual que en los beneficios a nivel tienda y año, en este caso se agrupa por familia de producto y se sigue la misma lógica

# COMMAND ----------

# Genero las ventas y los costos agrupados por familia y año
query = """
SELECT  p.Family,
        YEAR(s.Date) AS Anio,
        SUM(s.Quantity * p.RetailPrice) AS TotalVentas
        FROM df_sales s
        INNER JOIN df_products p ON s.ProductId = p.ProductId AND p.RetailPrice IS NOT null
        GROUP BY p.Family, YEAR(s.Date)"""

df_sales_agg_f = spark.sql(query)
df_sales_agg_f.createOrReplaceTempView('df_sales_agg_f')

query = """
SELECT  p.Family,
        YEAR(m.Date) AS Anio,
        SUM(m.Quantity * p.SupplierPrice) AS TotalCostos
        FROM df_stock_movements m
        INNER JOIN df_products p ON m.ProductId = p.ProductId AND p.RetailPrice IS NOT null
        WHERE m.Quantity > 0
        GROUP BY p.Family, YEAR(m.Date)"""

df_stock_movements_agg_f = spark.sql(query)
df_stock_movements_agg_f.createOrReplaceTempView('df_stock_movements_agg_f')

# COMMAND ----------

# Genero el beneficio por tienda y familia de producto para 2019 y 2020, teniendo en cuenta TODOS los productos presentes en el maestro con precio de venta al público informado
query = """
SELECT  families.Family,
        p_anio.anio,
        IFNULL(s.TotalVentas, 0) - IFNULL(m.TotalCostos, 0) AS Beneficio
        FROM (SELECT 2019 AS anio UNION SELECT 2020 AS anio) p_anio
        CROSS JOIN (SELECT DISTINCT Family FROM df_products) families
        LEFT JOIN df_sales_agg_f s ON families.Family = s.Family AND p_anio.anio = s.Anio
        LEFT JOIN df_stock_movements_agg_f m ON families.Family = m.Family AND p_anio.anio = m.Anio"""

df_family_benefits = spark.sql(query)
df_family_benefits.createOrReplaceTempView('df_family_benefits')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Rotación a nivel tienda y año

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aclaración 1: para el cálculo de la rotación se siguió una lógica similar para el cálculo del beneficio, en este caso también generamos agrupaciones previas para el archivo daily_stock.
# MAGIC 
# MAGIC #### Aclaración 2: para el cálculo del stock medio se utilizaron años de 365 días, dejando afuera los años de 366 días como en realidad fue el 2020.
# MAGIC 
# MAGIC #### Aclaración 3: en los casos donde no existió un stock medio positivo para alguna sucursal, no se especificó en la consigna si la rotación debía ser el total de ventas o simplemente cero. Como la división por cero no existe, se decidió dejar el valor del total de ventas.

# COMMAND ----------

# Genero las unidades vendidas agrupadas por tienda y año
query = """
SELECT  StoreId,
        YEAR(Date) AS Anio,
        SUM(Quantity) AS UnidadesVendidas
        FROM df_sales
        GROUP BY StoreId, YEAR(Date)"""

df_sales_agg_u_t = spark.sql(query)
df_sales_agg_u_t.createOrReplaceTempView('df_sales_agg_u_t')

# Genero el stock medio agrupado por tienda y año
query = """
SELECT  StoreId,
        YEAR(Date) AS Anio,
        SUM(Stock) / 365 AS StockMedio
        FROM df_daily_stock
        GROUP BY StoreId, YEAR(Date)"""

df_daily_stock_agg_t = spark.sql(query)
df_daily_stock_agg_t.createOrReplaceTempView('df_daily_stock_agg_t')

# COMMAND ----------

# Calculo la Rotacion por tienda y año
query = """
SELECT  stores.StoreId,
        p_anio.anio,
        CASE
            WHEN IFNULL(d.StockMedio, 0) <> 0 THEN IFNULL(s.UnidadesVendidas, 0) / d.StockMedio
            ELSE                                   IFNULL(s.UnidadesVendidas, 0)
        END AS Rotacion
        FROM (SELECT 2019 AS anio UNION SELECT 2020 AS anio) p_anio
        CROSS JOIN (SELECT StoreId FROM df_sales UNION SELECT StoreId FROM df_stock_movements) stores
        LEFT JOIN df_sales_agg_u_t s ON stores.StoreId = s.StoreId AND p_anio.anio = s.Anio
        LEFT JOIN df_daily_stock_agg_t d ON stores.StoreId = d.StoreId AND p_anio.anio = d.Anio"""

df_store_rotation = spark.sql(query)
df_store_rotation.createOrReplaceTempView('df_store_rotation')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) Rotación a nivel familia de producto y año

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aclaración: como ya se comentó previamente para el cálculo de la rotación por tienda y año, para el caso de la familia de producto se siguió la misma lógica

# COMMAND ----------

# Genero las unidades vendidas agrupadas por familia y año
query = """
SELECT  p.Family,
        YEAR(s.Date) AS Anio,
        SUM(s.Quantity) AS UnidadesVendidas
        FROM df_sales s
        INNER JOIN df_products p ON s.ProductId = p.ProductId
        GROUP BY p.Family, YEAR(s.Date)"""

df_sales_agg_u_f = spark.sql(query)
df_sales_agg_u_f.createOrReplaceTempView('df_sales_agg_u_f')

# Genero el stock medio agrupado por familia y año
query = """
SELECT  p.Family,
        YEAR(d.Date) AS Anio,
        SUM(d.Stock) / 365 AS StockMedio
        FROM df_daily_stock d
        INNER JOIN df_products p ON d.ProductRootCode = p.ProductRootCode
        GROUP BY p.Family, YEAR(d.Date)"""

df_daily_stock_agg_f = spark.sql(query)
df_daily_stock_agg_f.createOrReplaceTempView('df_daily_stock_agg_f')

# COMMAND ----------

# Calculo la Rotacion por familia y año
query = """
SELECT  families.Family,
        p_anio.anio,
        CASE
            WHEN IFNULL(d.StockMedio, 0) <> 0 THEN IFNULL(s.UnidadesVendidas, 0) / d.StockMedio
            ELSE                                   IFNULL(s.UnidadesVendidas, 0)
        END AS Rotacion
        FROM (SELECT 2019 AS anio UNION SELECT 2020 AS anio) p_anio
        CROSS JOIN (SELECT DISTINCT Family FROM df_products) families
        LEFT JOIN df_sales_agg_u_f s ON families.Family = s.Family AND p_anio.anio = s.Anio
        LEFT JOIN df_daily_stock_agg_f d ON families.Family = d.Family AND p_anio.anio = d.Anio"""

df_family_rotation = spark.sql(query)
df_family_rotation.createOrReplaceTempView('df_family_rotation')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4: Genero los parquet en el DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC #### NOTA: Se tiene la suposición que en el FS de Databricks hay al menos 26MB de memoria libres para que los archivos puedan ser generados, sino la siguiente celda dará error

# COMMAND ----------

# Genero el fichero interval_stock.parquet
df_interval_stock.write.format('parquet').mode('overwrite').save('dbfs:/output/interval_stock.parquet')

# Genero el fichero daily_stock.parquet
df_daily_stock.write.format('parquet').mode('overwrite').save('dbfs:/output/daily_stock.parquet')

# Genero el fichero store_benefits.parquet
df_store_benefits.write.format('parquet').mode('overwrite').save('dbfs:/output/store_benefits.parquet')

# Genero el fichero family_benefits.parquet
df_family_benefits.write.format('parquet').mode('overwrite').save('dbfs:/output/family_benefits.parquet')

# Genero el fichero store_rotation.parquet
df_store_rotation.write.format('parquet').mode('overwrite').save('dbfs:/output/store_rotation.parquet')

# Genero el fichero family_rotation.parquet
df_family_rotation.write.format('parquet').mode('overwrite').save('dbfs:/output/family_rotation.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análisis de productos sin precio de venta
# MAGIC 
# MAGIC Dado que existen productos en el archivo products.parquet que no poseen precio de venta al público, se analizó ésta situación en particular ya que al momento de generar el KPI de "Beneficios", existen productos que no se analizaron dentro del mismo. Ese subconjunto de productos, se presenta a continuación:

# COMMAND ----------

# Listado de productos sin precio de venta al público
query = """
SELECT  *
        FROM df_products
        WHERE RetailPrice IS null"""

df_productos_sin_precio = spark.sql(query)
df_productos_sin_precio.createOrReplaceTempView('df_productos_sin_precio')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  Family,
# MAGIC         COUNT(1) AS Cantidad
# MAGIC         FROM df_productos_sin_precio
# MAGIC         GROUP BY Family

# COMMAND ----------

# MAGIC %md
# MAGIC #### Como se ve en la consulta previa, todos los productos pertenecen a la familia MISCELLANEOUS y son en total 45. De este listado de 45 productos, no podemos calcular precio de venta, pero SI podemos calcular el costo. A continuación, se desarrolla dicho cálculo:

# COMMAND ----------

# Genero el costo de los productos agrupado por tienda y año
query = """
SELECT  m.StoreId,
        YEAR(m.Date) AS Anio,
        SUM(m.Quantity * p.SupplierPrice) AS TotalCosto
        FROM df_stock_movements m
        INNER JOIN df_productos_sin_precio p ON m.ProductId = p.ProductId
        WHERE m.Quantity > 0
        GROUP BY m.StoreId, YEAR(m.Date)"""

df_stock_movements_agg_sp = spark.sql(query)
df_stock_movements_agg_sp.createOrReplaceTempView('df_stock_movements_agg_sp')

query = """
SELECT  stores.StoreId,
        p_anio.anio,
        IFNULL(m.TotalCosto, 0) AS TotalCosto
        FROM (SELECT 2019 AS anio UNION SELECT 2020 AS anio) p_anio
        CROSS JOIN (SELECT StoreId FROM df_sales UNION SELECT StoreId FROM df_stock_movements) stores
        LEFT JOIN df_stock_movements_agg_sp m ON stores.StoreId = m.StoreId AND p_anio.anio = m.Anio"""

df_store_cost_misc = spark.sql(query)
df_store_cost_misc.createOrReplaceTempView('df_store_cost_misc')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  CONCAT(Anio, ' - ', StoreId) AS Anio_StoreId,
# MAGIC         TotalCosto
# MAGIC         FROM df_store_cost_misc
# MAGIC         ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(TotalCosto) FROM df_store_cost_misc

# COMMAND ----------

# MAGIC %md
# MAGIC #### El total de costo analizado fue de 726.505,90 y se encuentra distribuido como se muestra en el gráfico anterior. Si el cliente interpreta que dicho costo debe restarse a los beneficios anteriormente calculados, basta que cruzar los 2 conjuntos y realizar la resta correspondiente por tienda y año, ya que ambos datasets poseen el mismo formato. A contin

# COMMAND ----------

# A los beneficios anteriormente calculados, le resto los costos que no se analizaron en dicho momento
query = """
SELECT  b.StoreId,
        b.Anio,
        b.Beneficio - m.TotalCosto AS TotalBeneficio
        FROM df_store_benefits b
        INNER JOIN df_store_cost_misc m ON b.StoreId = m.StoreId AND b.Anio = m.Anio"""

df_store_benefits_v2 = spark.sql(query)
df_store_benefits_v2.createOrReplaceTempView('df_store_benefits_v2')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  CONCAT(Anio, ' - ', StoreId) AS Anio_StoreId,
# MAGIC         TotalBeneficio
# MAGIC         FROM df_store_benefits_v2
# MAGIC         ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Del mismo modo que se analizó previamente el costo agrupado por tienda y año, lo podemos generar también agrupado por familia de producto y año:

# COMMAND ----------

# Genero el costo de los productos agrupado por tienda y año
query = """
SELECT  pr.Family,
        YEAR(m.Date) AS Anio,
        SUM(m.Quantity * p.SupplierPrice) AS TotalCosto
        FROM df_stock_movements m
        INNER JOIN df_productos_sin_precio p ON m.ProductId = p.ProductId
        INNER JOIN df_products pr ON p.ProductId = pr.ProductId
        WHERE m.Quantity > 0
        GROUP BY pr.Family, YEAR(m.Date)"""

df_stock_movements_agg_spf = spark.sql(query)
df_stock_movements_agg_spf.createOrReplaceTempView('df_stock_movements_agg_spf')

query = """
SELECT  families.Family,
        p_anio.anio,
        IFNULL(m.TotalCosto, 0) AS TotalCosto
        FROM (SELECT 2019 AS anio UNION SELECT 2020 AS anio) p_anio
        CROSS JOIN (SELECT DISTINCT Family FROM df_products) families
        LEFT JOIN df_stock_movements_agg_spf m ON families.Family = m.Family AND p_anio.anio = m.Anio"""

df_family_cost_misc = spark.sql(query)
df_family_cost_misc.createOrReplaceTempView('df_family_cost_misc')

# A los beneficios anteriormente calculados, le resto los costos que no se analizaron en dicho momento
query = """
SELECT  b.Family,
        b.Anio,
        b.Beneficio - m.TotalCosto AS TotalBeneficio
        FROM df_family_benefits b
        LEFT JOIN df_family_cost_misc m ON b.Family = m.Family AND b.Anio = m.Anio"""

df_family_benefits_v2 = spark.sql(query)
df_family_benefits_v2.createOrReplaceTempView('df_family_benefits_v2')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  CONCAT(Anio, ' - ', Family) AS Anio_Family,
# MAGIC         TotalBeneficio
# MAGIC         FROM df_family_benefits_v2
# MAGIC         ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### NOTA: Se tiene la suposición que en el FS de Databricks hay al menos 26MB de memoria libres para que los archivos puedan ser generados, sino la siguiente celda dará error

# COMMAND ----------

# Genero las nuevas versiones en parquet
df_store_benefits_v2.write.format('parquet').mode('overwrite').save('dbfs:/output/store_benefits_v2.parquet')
df_family_benefits_v2.write.format('parquet').mode('overwrite').save('dbfs:/output/family_benefits_v2.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cálculo de espacio necesario por cada archivo generado

# COMMAND ----------

size_bytes = 0

l_archivos = [item.path for item in dbutils.fs.ls('dbfs:/output/')]

for archivo in l_archivos:
    l_size = [item.size for item in dbutils.fs.ls(archivo)]
    size_bytes += sum(l_size)

print('Bytes necesarios: {}'.format(size_bytes))
print('MB necesarios: {}'.format(size_bytes / 1024 / 1024))

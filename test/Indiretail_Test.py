# Databricks notebook source
# MAGIC %md
# MAGIC ## Autor: Joaquín Santini
# MAGIC ## Descripción: La notebook carga las salidas generadas por el proceso solicitado y valida los conjuntos de datos.
# MAGIC <br>
# MAGIC ### Secciones de la notebook
# MAGIC - Descarga de Archivos
# MAGIC - Validación de interval_stock
# MAGIC - Validación de daily_stock
# MAGIC - Validación de store_benefits
# MAGIC - Validación de family_benefits
# MAGIC - Validación de store_rotation
# MAGIC - Validación de family_rotation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Se descargan los archivos generados por el proceso y los ficheros origen
# MAGIC 
# MAGIC #### NOTA: Se tiene la suposición que en el FS de Databricks hay al menos 30MB de memoria libres para que los archivos puedan ser descargados, sino la siguiente celda dará error

# COMMAND ----------

import urllib.request

# Bajo los archivos desde GitHub
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/data/products.parquet?raw=true", "/tmp/products.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/data/sales.parquet?raw=true", "/tmp/sales.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/data/stock_movements.parquet?raw=true", "/tmp/stock_movements.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/output/daily_stock.parquet?raw=true", "/tmp/daily_stock.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/output/family_benefits.parquet?raw=true", "/tmp/family_benefits.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/output/family_rotation.parquet?raw=true", "/tmp/family_rotation.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/output/interval_stock.parquet?raw=true", "/tmp/interval_stock.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/output/store_benefits.parquet?raw=true", "/tmp/store_benefits.parquet")
urllib.request.urlretrieve("https://github.com/joaquinsantini/PruebaIndiretail_JoaquinSantini/blob/main/output/store_rotation.parquet?raw=true", "/tmp/store_rotation.parquet")

# Los muevo al DBFS
dbutils.fs.mv("file:/tmp/products.parquet", "dbfs:/data/products.parquet")
dbutils.fs.mv("file:/tmp/sales.parquet", "dbfs:/data/sales.parquet")
dbutils.fs.mv("file:/tmp/stock_movements.parquet", "dbfs:/data/stock_movements.parquet")
dbutils.fs.mv("file:/tmp/daily_stock.parquet", "dbfs:/output_repo/daily_stock.parquet")
dbutils.fs.mv("file:/tmp/family_benefits.parquet", "dbfs:/output_repo/family_benefits.parquet")
dbutils.fs.mv("file:/tmp/family_rotation.parquet", "dbfs:/output_repo/family_rotation.parquet")
dbutils.fs.mv("file:/tmp/interval_stock.parquet", "dbfs:/output_repo/interval_stock.parquet")
dbutils.fs.mv("file:/tmp/store_benefits.parquet", "dbfs:/output_repo/store_benefits.parquet")
dbutils.fs.mv("file:/tmp/store_rotation.parquet", "dbfs:/output_repo/store_rotation.parquet")

# Cargo los DataFrame
df_products = spark.read.format('parquet').load('dbfs:/data/products.parquet')
df_sales = spark.read.format('parquet').load('dbfs:/data/sales.parquet')
df_stock_movements = spark.read.format('parquet').load('dbfs:/data/stock_movements.parquet')
df_daily_stock = spark.read.format('parquet').load('dbfs:/output_repo/daily_stock.parquet')
df_family_benefits = spark.read.format('parquet').load('dbfs:/output_repo/family_benefits.parquet')
df_family_rotation = spark.read.format('parquet').load('dbfs:/output_repo/family_rotation.parquet')
df_interval_stock = spark.read.format('parquet').load('dbfs:/output_repo/interval_stock.parquet')
df_store_benefits = spark.read.format('parquet').load('dbfs:/output_repo/store_benefits.parquet')
df_store_rotation = spark.read.format('parquet').load('dbfs:/output_repo/store_rotation.parquet')

# COMMAND ----------

df_products.createOrReplaceTempView('df_products')
df_sales.createOrReplaceTempView('df_sales')
df_stock_movements.createOrReplaceTempView('df_stock_movements')
df_daily_stock.createOrReplaceTempView('df_daily_stock')
df_family_benefits.createOrReplaceTempView('df_family_benefits')
df_family_rotation.createOrReplaceTempView('df_family_rotation')
df_interval_stock.createOrReplaceTempView('df_interval_stock')
df_store_benefits.createOrReplaceTempView('df_store_benefits')
df_store_rotation.createOrReplaceTempView('df_store_rotation')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación del archivo interval_stock

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validación 1: Stock positivo en todos los registros
# MAGIC 
# MAGIC #### Como se pidió por consigna, interval_stock no debe tener Stock negativo ni en cero en ningún registro

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_interval_stock WHERE Stock <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validación 2: cálculo de Stock
# MAGIC 
# MAGIC #### En esta validación tomamos 3 casos del archivo y validamos con los movimientos de stock que el cálculo esté bien realizado

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Primero obtenemos una muestra de algunos números agrupados
# MAGIC SELECT  ProductRootCode,
# MAGIC         StoreId,
# MAGIC         MAX(StartDate) AS StartDate
# MAGIC         FROM df_interval_stock
# MAGIC         GROUP BY ProductRootCode, StoreId
# MAGIC         LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC #### De los registros anteriormente obtenidos, verificamos que el Stock calculado a esa fecha tenga sentido. Para esto, buscamos los movimientos de stock

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ProductRootCode = 33
# MAGIC -- StoreId = 37
# MAGIC -- StartDate = 2020-12-21
# MAGIC SELECT  SUM(m.Quantity) AS Stock_A_Fecha
# MAGIC         FROM df_stock_movements m
# MAGIC         INNER JOIN df_products p ON m.ProductId = p.ProductId
# MAGIC         WHERE p.ProductRootCode = 33 AND
# MAGIC               m.StoreId = 37 AND
# MAGIC               m.Date <= '2020-12-21'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Al sumar el campo Quantity de todas las fechas anteriores a la StartDate, vemos que el Stock calculado tiene sentido y se valida de forma exitosa

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_interval_stock WHERE ProductRootCode = 33 AND StoreId = 37 AND StartDate = '2020-12-21'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ProductRootCode = 35
# MAGIC -- StoreId = 45
# MAGIC -- StartDate = 2020-12-28
# MAGIC SELECT  SUM(m.Quantity) AS Stock_A_Fecha
# MAGIC         FROM df_stock_movements m
# MAGIC         INNER JOIN df_products p ON m.ProductId = p.ProductId
# MAGIC         WHERE p.ProductRootCode = 35 AND
# MAGIC               m.StoreId = 45 AND
# MAGIC               m.Date <= '2020-12-28'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_interval_stock WHERE ProductRootCode = 35 AND StoreId = 45 AND StartDate = '2020-12-28'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ProductRootCode = 61
# MAGIC -- StoreId = 13
# MAGIC -- StartDate = 2020-02-01
# MAGIC SELECT  SUM(m.Quantity) AS Stock_A_Fecha
# MAGIC         FROM df_stock_movements m
# MAGIC         INNER JOIN df_products p ON m.ProductId = p.ProductId
# MAGIC         WHERE p.ProductRootCode = 61 AND
# MAGIC               m.StoreId = 13 AND
# MAGIC               m.Date <= '2020-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_interval_stock WHERE ProductRootCode = 61 AND StoreId = 13 AND StartDate = '2020-02-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación del archivo daily_stock

# COMMAND ----------

# MAGIC %md
# MAGIC #### El archivo daily_stock se generó a partir del archivo interval_stock validado anteriormente, por ende, vamos a utilizar nuevamente 3 casos al azar para ver que se condice lo generado en ambos archivos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nos quedamos con 3 registros al azar de interval_stock
# MAGIC SELECT  *,
# MAGIC         DATEDIFF(EndDate, StartDate) AS Diferencia_Dias
# MAGIC         FROM df_interval_stock
# MAGIC         WHERE Stock BETWEEN 8000 AND 10000
# MAGIC         ORDER BY Stock
# MAGIC         LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC #### Debemos validar que en daily_stock, existan tantos registros como diferencia en días + 1 (ya que la función DATEDIFF no cuenta una de las cotas) entre la StartDate y la EndDate dadas. El Stock se debe mantener a través de todos los registros.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM df_daily_stock WHERE ProductRootCode = 14630 AND StoreId = 51 AND Date BETWEEN '2019-07-24' AND '2019-09-16' AND Stock = 8000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM df_daily_stock WHERE ProductRootCode = 221 AND StoreId = 1 AND Date BETWEEN '2019-04-30' AND '2019-05-30' AND Stock = 8000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM df_daily_stock WHERE ProductRootCode = 225 AND StoreId = 1 AND Date BETWEEN '2019-11-13' AND '2019-11-14' AND Stock = 8012

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación del archivo store_benefits

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utilizaremos ahora los archivos de ventas y movimientos de stock para verificar que los beneficios calculados sean correctos. Empezamos con 3 casos al azar para validar.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nos quedamos con 3 registros al azar de store_benefits
# MAGIC SELECT  *
# MAGIC         FROM df_store_benefits
# MAGIC         WHERE Anio = 2020
# MAGIC         ORDER BY Beneficio DESC
# MAGIC         LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC #### Para cada uno de los casos, calculamos primero el total de ventas para ese año
# MAGIC 
# MAGIC #### Aclaración: como ya se mencionó en la notebook de proceso, NO tendremos en cuenta productos sin precio de venta al público informado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  s.StoreId,
# MAGIC         YEAR(s.Date) AS Anio,
# MAGIC         SUM(s.Quantity * p.RetailPrice) AS TotalVentas
# MAGIC         FROM df_sales s
# MAGIC         INNER JOIN df_products p ON s.ProductId = p.ProductId AND p.RetailPrice IS NOT null
# MAGIC         WHERE s.StoreId IN(18, 30, 50) AND
# MAGIC               YEAR(s.Date) = 2020
# MAGIC         GROUP BY s.StoreId, YEAR(s.Date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### De igual manera, calculamos los costos para cada uno de los casos para ese año

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  m.StoreId,
# MAGIC         YEAR(m.Date) AS Anio,
# MAGIC         SUM(m.Quantity * p.SupplierPrice) AS TotalCostos
# MAGIC         FROM df_stock_movements m
# MAGIC         INNER JOIN df_products p ON m.ProductId = p.ProductId AND p.RetailPrice IS NOT null
# MAGIC         WHERE m.StoreId IN(18, 30, 50) AND
# MAGIC               YEAR(m.Date) = 2020 AND
# MAGIC               m.Quantity > 0
# MAGIC         GROUP BY m.StoreId, YEAR(m.Date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Como se puede observar, haciendo las cuentas necesarias se llega al resultado esperado:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 50 AS StoreId, 1415338 - 317071.9813988209 AS Beneficio
# MAGIC UNION
# MAGIC SELECT 18 AS StoreId, 633409 - 129437.66969925165 AS Beneficio
# MAGIC UNION
# MAGIC SELECT 30 AS StoreId, 559184 - 110792.32966670394 AS Beneficio
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación del archivo family_benefits

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utilizaremos ahora los archivos de ventas, productos y movimientos de stock para verificar que los beneficios calculados sean correctos. Se sigue la misma lógica que para el caso anterior

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nos quedamos con 3 registros al azar de store_benefits
# MAGIC SELECT  *
# MAGIC         FROM df_family_benefits
# MAGIC         WHERE Anio = 2019
# MAGIC         ORDER BY Beneficio DESC
# MAGIC         LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  p.Family,
# MAGIC         YEAR(s.Date) AS Anio,
# MAGIC         SUM(s.Quantity * p.RetailPrice) AS TotalVentas
# MAGIC         FROM df_sales s
# MAGIC         INNER JOIN df_products p ON s.ProductId = p.ProductId AND p.RetailPrice IS NOT null
# MAGIC         WHERE p.Family IN('NECKLACE', 'EARRINGS', 'PIERCING') AND
# MAGIC               YEAR(s.Date) = 2019
# MAGIC         GROUP BY p.Family, YEAR(s.Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  p.Family,
# MAGIC         YEAR(m.Date) AS Anio,
# MAGIC         SUM(m.Quantity * p.SupplierPrice) AS TotalCostos
# MAGIC         FROM df_stock_movements m
# MAGIC         INNER JOIN df_products p ON m.ProductId = p.ProductId AND p.RetailPrice IS NOT null
# MAGIC         WHERE p.Family IN('NECKLACE', 'EARRINGS', 'PIERCING') AND
# MAGIC               YEAR(m.Date) = 2019 AND
# MAGIC               m.Quantity > 0
# MAGIC         GROUP BY p.Family, YEAR(m.Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'PIERCING' AS Family, 2522646 - 1022751.7329291105 AS Beneficio
# MAGIC UNION
# MAGIC SELECT 'NECKLACE' AS Family, 3577815 - 1458542.5372424126 AS Beneficio
# MAGIC UNION
# MAGIC SELECT 'EARRINGS' AS Family, 2359214 - 822721.5286664963 AS Beneficio
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación del archivo store_rotation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utilizaremos ahora los archivos de ventas y daily_stock para verificar que la rotación calculada sea correcta. Empezamos con 3 casos al azar para validar.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nos quedamos con 3 registros al azar de store_benefits
# MAGIC SELECT  *
# MAGIC         FROM df_store_rotation
# MAGIC         WHERE Anio = 2019
# MAGIC         ORDER BY Rotacion DESC
# MAGIC         LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  StoreId,
# MAGIC         YEAR(Date) AS Anio,
# MAGIC         SUM(Quantity) AS UnidadesVendidas
# MAGIC         FROM df_sales
# MAGIC         WHERE StoreId IN(8, 13, 14) AND
# MAGIC               YEAR(Date) = 2019
# MAGIC         GROUP BY StoreId, YEAR(Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  StoreId,
# MAGIC         YEAR(Date) AS Anio,
# MAGIC         SUM(Stock) / 365 AS StockMedio
# MAGIC         FROM df_daily_stock
# MAGIC         WHERE StoreId IN(8, 13, 14) AND
# MAGIC               YEAR(Date) = 2019
# MAGIC         GROUP BY StoreId, YEAR(Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 8 AS StoreId, 34573 / 7000.087671232876 AS Rotacion
# MAGIC UNION
# MAGIC SELECT 14 AS StoreId, 47216 / 9706.016438356164 AS Rotacion
# MAGIC UNION
# MAGIC SELECT 13 AS StoreId, 21117 / 4157.109589041096 AS Rotacion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación del archivo family_rotation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utilizaremos ahora los archivos de ventas, productos y daily_stock para verificar que la rotación calculada sea correcta. Empezamos con 3 casos al azar para validar.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nos quedamos con 3 registros al azar de store_benefits
# MAGIC SELECT  *
# MAGIC         FROM df_family_rotation
# MAGIC         WHERE Anio = 2020
# MAGIC         ORDER BY Rotacion DESC
# MAGIC         LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  p.Family,
# MAGIC         YEAR(s.Date) AS Anio,
# MAGIC         SUM(s.Quantity) AS UnidadesVendidas
# MAGIC         FROM df_sales s
# MAGIC         INNER JOIN df_products p ON s.ProductId = p.ProductId
# MAGIC         WHERE p.Family IN('PIERCING', 'MISCELLANEOUS', 'NECKLACE') AND
# MAGIC               YEAR(s.Date) = 2020
# MAGIC         GROUP BY p.Family, YEAR(s.Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  p.Family,
# MAGIC         YEAR(d.Date) AS Anio,
# MAGIC         SUM(d.Stock) / 365 AS StockMedio
# MAGIC         FROM df_daily_stock d
# MAGIC         INNER JOIN df_products p ON d.ProductRootCode = p.ProductRootCode
# MAGIC         WHERE p.Family IN('PIERCING', 'MISCELLANEOUS', 'NECKLACE') AND
# MAGIC               YEAR(d.Date) = 2020
# MAGIC         GROUP BY p.Family, YEAR(d.Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'MISCELLANEOUS' AS Family, 372691 / 186532.56712328768 AS Rotacion
# MAGIC UNION
# MAGIC SELECT 'NECKLACE' AS Family, 50031 / 50569.7205479452 AS Rotacion
# MAGIC UNION
# MAGIC SELECT 'PIERCING' AS Family, 81929 / 29337.098630136985 AS Rotacion

# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adventureworkdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adventureworkdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adventureworkdl.dfs.core.windows.net", "390d5f8c-9cf7-4422-b2be-b1b6ed94d8bf")
spark.conf.set("fs.azure.account.oauth2.client.secret.adventureworkdl.dfs.core.windows.net", "Xwq8Q~IKI65ZxTuDtK2MefY06yFrckjr8xlhKaaA")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adventureworkdl.dfs.core.windows.net", "https://login.microsoftonline.com/350783b2-ac7e-4f10-a6e2-7ba5a1464496/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data loading

# COMMAND ----------

df_cal = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cust = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_prod_cat = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_prod = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_return = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Returns/")

# COMMAND ----------

df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Sales_*")

# COMMAND ----------

df_sales.filter(year(df_sales.OrderDate) == 2016).display()

# COMMAND ----------

df_terr = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_prod_subcat = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adventureworkdl.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATION
# MAGIC

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_cal = df_cal.withColumn("Month" , month(df_cal.Date))
df_cal = df_cal.withColumn("Year" , year(df_cal.Date))
df_cal.display()

# COMMAND ----------

df_cal.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust.printSchema()

# COMMAND ----------

df_cust = df_cust.withColumn("FullName" , concat_ws(" ", col("Prefix") , col("FirstName") , col("LastName")))

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.printSchema()

# COMMAND ----------

df_prod_subcat.display()

# COMMAND ----------

df_prod_subcat.printSchema()

# COMMAND ----------

df_prod_subcat.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/Product_Subcategories")\
    .save()

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.printSchema()

# COMMAND ----------

df_prod = df_prod.withColumn("ProductSKU" , split(col("ProductSKU"), "-")[0])\
    .withColumn("ProductName" , split(col("ProductName") , ",")[0])

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

df_return.display()

# COMMAND ----------

df_return.printSchema()

# COMMAND ----------

df_return.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()


# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_terr.printSchema()

# COMMAND ----------

df_terr.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

df_sales.display()


# COMMAND ----------

df_sales.printSchema()

# COMMAND ----------

df_sales = df_sales.withColumn("StockDate" , to_timestamp(col("StockDate")))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn("OrderNumber" , regexp_replace(col("OrderNumber") , 'S' , 'T' ))


# COMMAND ----------

df_sales = df_sales.withColumn("Multiply" , col("OrderQuantity") * col("OrderLineItem"))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.groupBy("OrderDate").agg(count("OrderNumber").alias("TotalOrder")).display()

# COMMAND ----------

df_sales.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod_subcat.display()

# COMMAND ----------

df_viz2 = df_prod_cat.join(df_prod_subcat, df_prod_cat.ProductCategoryKey == df_prod_subcat.ProductCategoryKey, "inner")

# COMMAND ----------

df_viz2.display()

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_viz2 = df_viz2.join(df_prod, df_viz2.ProductSubcategoryKey == df_prod.ProductSubcategoryKey, "inner")
df_viz2.display()

# COMMAND ----------

df_viz2.groupBy("CategoryName") \
    .agg(count("ProductKey").alias("Total")) \
    .display()

# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prod_cat.write.format("parquet")\
    .mode("append")\
    .option("path" , "abfss://silver@adventureworkdl.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()
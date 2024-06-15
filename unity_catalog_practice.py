# Databricks notebook source
# MAGIC %sql
# MAGIC select current_metastore()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT CURRENT_CATALOG()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sudhanshu_raj_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog sudhanshu_raj_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog()

# COMMAND ----------

# MAGIC %sql
# MAGIC create SCHEMA sudhanshu_raj_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sudhanshu_raj_schema.menu (recipe_id INT, app string, main string, desert string);
# MAGIC DELETE from sudhanshu_raj_schema.menu ;
# MAGIC  
# MAGIC INSERT INTO sudhanshu_raj_schema.menu 
# MAGIC     (recipe_id, app, main, desert) 
# MAGIC VALUES 
# MAGIC     (1,"Ceviche", "Tacos", "Flan"),
# MAGIC     (2,"Tomato Soup", "Souffle", "Creme Brulee"),
# MAGIC     (3,"Chips","Grilled Cheese","Cheescake");

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sudhanshu_raj_schema.dinner 
# MAGIC   AS SELECT recipe_id, concat(app," + ", main," + ",desert) as full_menu FROM sudhanshu_raj_schema.menu

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.range(3).withColumn("price", F.round(10*F.rand(seed=42),2)).withColumnRenamed("id", "recipe_id")
 
df.write.mode("overwrite").saveAsTable("sudhanshu_raj_schema.price")
 
dinner = spark.read.table("sudhanshu_raj_schema.dinner")
price = spark.read.table("sudhanshu_raj_schema.price")
 
dinner_price = dinner.join(price, on="recipe_id")
dinner_price.write.mode("overwrite").saveAsTable("sudhanshu_raj_schema.dinner_price")

# Databricks notebook source
# MAGIC %run ./config_file

# COMMAND ----------

from pyspark.sql import functions as F


def process_bronze():
  
    schema = "Age INT, Name STRING, Value INT,LastUpdate TIMESTAMP"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load('abfss://raw@rajdatabrickspractice.dfs.core.windows.net/jsoncreateddata')
                        .withColumn("input_file_name", F.input_file_name()  )  
                        .withColumn("year_month", F.date_format("LastUpdate", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", "abfss://raw@rajdatabrickspractice.dfs.core.windows.net/bronze_checkpoint")
                      .option("mergeSchema", True)
                    #   .partitionBy("year_month")
                      .trigger(availableNow=True)
                      .toTable("deloitte.sudhanshu.bronze"))
    
    query.awaitTermination()

# COMMAND ----------

 process_bronze()

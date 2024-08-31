# Databricks notebook source
key = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-31T04:08:51Z&st=2024-08-15T19:08:51Z&spr=https&sig=bUX9OCXmUqJ28vMJasbtYJ2VZgwpzP4R3uaV5asH8TE%3D'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.rajdatabrickspractice.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.rajdatabrickspractice.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.rajdatabrickspractice.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-31T04:08:51Z&st=2024-08-15T19:08:51Z&spr=https&sig=bUX9OCXmUqJ28vMJasbtYJ2VZgwpzP4R3uaV5asH8TE%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@rajdatabrickspractice.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.format('json').load('abfss://raw@rajdatabrickspractice.dfs.core.windows.net/jsoncreateddata')
display(df)

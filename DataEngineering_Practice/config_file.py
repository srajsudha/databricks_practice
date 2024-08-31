# Databricks notebook source
def set_azure_sas_token(account_name, sas_token):
    spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{account_name}.dfs.core.windows.net", sas_token)

account_name = 'rajdatabrickspractice'
sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-31T04:08:51Z&st=2024-08-15T19:08:51Z&spr=https&sig=bUX9OCXmUqJ28vMJasbtYJ2VZgwpzP4R3uaV5asH8TE%3D'
set_azure_sas_token(account_name, sas_token)

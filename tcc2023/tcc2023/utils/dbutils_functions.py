# Databricks notebook source
dbutils.fs.ls("/")

# COMMAND ----------

# verifica se ja tem montado
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount()

# COMMAND ----------

dbutils.fs.ls("/mnt/training/airbnb/amsterdam-listings/amsterdam-listings-2018-12-06.parquet/")

# COMMAND ----------

# df = spark.read.parquet("/mnt/training/airbnb/amsterdam-listings/*.parquet").display()
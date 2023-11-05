# Databricks notebook source
# MAGIC %md **Ingestion -> Fii Fundamentus**

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_bronze/fii_fundamentus_ws_to_bronze

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_silver/fii_fundamentus_ws_to_silver

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_gold/fii_fundamentus_ws_to_gold

# COMMAND ----------

# MAGIC %md **Ingestion -> Acoes Fundamentus**

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_bronze/acao_fundamentus_ws_to_bronze

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_silver/acao_fundamentus_ws_to_silver

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_gold/acao_fundamentus_ws_to_gold

# COMMAND ----------

# MAGIC %md **Ingestion -> Proventos Fundamentus FII**

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_bronze/proventos_fii_fundamentus_ws_to_bronze

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_silver/proventos_fii_fundamentus_ws_to_silver

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_gold/proventos_fii_fundamentus_ws_to_gold

# COMMAND ----------

# MAGIC %md **Ingestion -> Proventos Fundamentus ACAO**

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_bronze/proventos_acao_fundamentus_ws_to_bronze

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_silver/proventos_acao_fundamentus_ws_to_silver

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_gold/proventos_acao_fundamentus_ws_to_gold

# COMMAND ----------

# MAGIC %md **Ingestion -> bi investment manager**

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/web_scraping/to_business/bi_investment_manager

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ufrj_tcc_2023.fundamentus__acao

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ufrj_tcc_2023.fundamentus__fii

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ufrj_tcc_2023.fundamentus__proventos

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ufrj_tcc_2023.business__ticker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ufrj_tcc_2023.business__proventos
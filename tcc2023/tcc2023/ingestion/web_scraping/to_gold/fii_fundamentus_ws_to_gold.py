# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class FiiFundamentusWsToGold:
    
    DEFAULT_SCHEMA = StructType([
        StructField("papel", StringType(), True),
        StructField("segmento", StringType(), True),
        StructField("cotacao", StringType(), True),
        StructField("dividend_yield", StringType(), True),
        StructField("pvp", StringType(), True),
        StructField("valor_mercado", StringType(), True),
        StructField("liquidez", StringType(), True),
        StructField("vacancia_media", StringType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("created_at", DateType(), True)
    ])

    MERGE_KEY = """target.papel = update.papel 
    AND target.updated_at = update.updated_at"""

    def get_dataframe(self):
        return IngestionController("fundamentus/fii").read_silver()


    def run(self):        
        IngestionController("fundamentus/fii") \
        .write_gold(self.get_dataframe(),
            self.DEFAULT_SCHEMA,
            "fii",
            None,
            self.MERGE_KEY,
            "fundamentus",
            "created_at"
        )

# COMMAND ----------

FiiFundamentusWsToGold().run()

# COMMAND ----------

# %sql
# select * from ufrj_tcc_2023.fundamentus__fii
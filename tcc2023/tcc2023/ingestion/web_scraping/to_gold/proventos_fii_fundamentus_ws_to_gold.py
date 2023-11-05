# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class ProventosFiiFundamentusWsToGold:

    DEFAULT_SCHEMA = StructType([
        StructField("papel", StringType(), True),
        StructField("data_pgto", DateType(), True),
        StructField("tipo", StringType(), True),
        StructField("valor", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("created_at", DateType(), True)
    ])

    MERGE_KEY = """target.papel = update.papel
    AND target.data_pgto = update.data_pgto
    AND target.updated_at = update.updated_at"""

    def get_dataframe(self):
        return IngestionController("fundamentus/proventos/fii").read_silver()
    
    
    def run(self):       
        IngestionController("fundamentus/proventos/fii") \
        .write_gold(self.get_dataframe(),
            self.DEFAULT_SCHEMA,
            "proventos",
            None,
            self.MERGE_KEY,
            "fundamentus",
            "created_at"
        )

# COMMAND ----------

ProventosFiiFundamentusWsToGold().run()
# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class AcaoFundamentusWsToGold:

    DEFAULT_SCHEMA = StructType([
        StructField('papel', StringType(), True),
        StructField('cotacao', StringType(), True),
        StructField('pl', StringType(), True),
        StructField('pvp', StringType(), True),
        StructField('dividend_yield', StringType(), True),
        StructField('pativo', StringType(), True),
        StructField('pcapgiro', StringType(), True),
        StructField('pebit', StringType(), True),
        StructField('pativocirc', StringType(), True),
        StructField('evebit', StringType(), True),
        StructField('evebita', StringType(), True),
        StructField('mrgebit', StringType(), True), 
        StructField('mrgliq', StringType(), True), 
        StructField('liqcorrente', StringType(), True),
        StructField('roic', StringType(), True),
        StructField('roe', StringType(), True),
        StructField('liq2meses', StringType(), True),
        StructField('patriliquido', StringType(), True),
        StructField('divbruta_por_patri', StringType(), True),
        StructField('cresc_5a', StringType(), True),
        StructField('updated_at', TimestampType(), True),
        StructField("created_at", DateType(), True)
    ])



    MERGE_KEY = """target.papel = update.papel
    AND target.updated_at = update.updated_at"""

    def get_dataframe(self):
        return IngestionController("fundamentus/acoes").read_silver() \


    def run(self):
        IngestionController("fundamentus/acoes") \
        .write_gold(self.get_dataframe(),
            self.DEFAULT_SCHEMA,
            "acao",
            None,
            self.MERGE_KEY,
            "fundamentus",
            "created_at"
        )

# COMMAND ----------

AcaoFundamentusWsToGold().run()
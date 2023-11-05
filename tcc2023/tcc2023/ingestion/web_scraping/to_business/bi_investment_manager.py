# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class BiInvestmentManager:

    MERGE_KEY = """target.papel = update.papel
    AND target.updated_at = update.updated_at"""

    PROVENTO_MERGE_KEY = """target.papel = update.papel
    AND target.data_pgto = update.data_pgto"""

    PROVENTOS_SCHEMA = StructType([
        StructField('papel', StringType(), True),
        StructField('data_pgto', DateType(), True),
        StructField('valor_total', DoubleType(), True)
    ])

    TICKER_SCHEMA = StructType([
        StructField('papel', StringType(), True), 
        StructField('cotacao', StringType(), True), 
        StructField('dividend_yield', StringType(), True), 
        StructField('pvp', StringType(), True), 
        StructField('created_at', DateType(), True),
        StructField('updated_at', TimestampType(), True), 
        StructField('tipo_ticker', StringType(), False)
    ])

    @property
    def get_info_fii(self):
        window_spec = Window.partitionBy("papel") \
            .orderBy(col("updated_at").desc())

        return IngestionController("fundamentus/fii") \
            .read_gold("ufrj_tcc_2023") \
            .withColumn("tmp_max_dt", rank().over(window_spec)) \
            .filter("tmp_max_dt = 1") \
            .selectExpr(
                "papel"
                ,"cotacao"
                ,"dividend_yield"
                ,"pvp"
                ,"created_at"
                ,"updated_at"
                ,"'FII' as tipo_ticker"
            ).distinct()

    
    @property
    def get_info_acao(self):
        window_spec = Window.partitionBy("papel") \
            .orderBy(col("updated_at").desc())

        return IngestionController("fundamentus/acao") \
            .read_gold("ufrj_tcc_2023") \
            .withColumn("tmp_max_dt", rank().over(window_spec)) \
            .filter("tmp_max_dt = 1") \
            .selectExpr(
                "papel"
                ,"cotacao"
                ,"dividend_yield"
                ,"pvp"
                ,"created_at"
                ,"updated_at"
                ,"'ACAO' as tipo_ticker"
            ).distinct()

    
    @property
    def get_info_proventos(self):
        return IngestionController("fundamentus/proventos") \
            .read_gold("ufrj_tcc_2023") \
            .groupBy("papel", "data_pgto") \
            .agg(sum("valor").alias("valor_total"))

    
    def run(self):
        window_spec = Window.partitionBy("papel") \
            .orderBy(col("updated_at").desc())

        ticker_df = Utils.union_all_df([self.get_info_acao, self.get_info_fii]) \
            .withColumn("tmp_max_dt", rank().over(window_spec)) \
            .filter("tmp_max_dt = 1") \
            .drop("tmp_max_dt")

        print("start write process table business ticker")
        IngestionController("business/ticker") \
        .write_gold(ticker_df,
            self.TICKER_SCHEMA,
            "ticker",
            None,
            self.MERGE_KEY,
            "business"
        )

        print("start write process table business proventos")
        IngestionController("business/proventos") \
        .write_gold(self.get_info_proventos,
            self.PROVENTOS_SCHEMA,
            "proventos",
            None,
            self.PROVENTO_MERGE_KEY,
            "business"
        )

# COMMAND ----------

BiInvestmentManager().run()
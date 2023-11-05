# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class ProventosAcaoFundamentusWsToSilver:

    def get_dataframe(self):
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") 
        return IngestionController("fundamentus/proventos/acao").read_bronze() \
            .withColumn("data_pgto", to_date(col("data_pgto"), "dd/MM/yyyy")) \
            .selectExpr(
                "papel                                                  as papel"
                ,"data_pgto                                             as data_pgto"
                ,"tipo                                                  as tipo"
                ,"replace(valor, ',','.') *1                            as valor"
                ,"updated_at                                            as updated_at"
                ,"current_date                                          as created_at"
            ).filter("data_pgto <= current_date")
    
    
    def run(self):       
        IngestionController("fundamentus/proventos/acao").write_silver(self.get_dataframe())

# COMMAND ----------

ProventosAcaoFundamentusWsToSilver().run()
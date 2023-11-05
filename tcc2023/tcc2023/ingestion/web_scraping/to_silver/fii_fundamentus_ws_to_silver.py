# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class FiiFundamentusWsToSilver:

    def set_filters_fii(self, dataframe):
        return dataframe \
            .filter("segmento is not null and segmento <> ''") \
            .filter("liquidez >= 100000") \
            .filter("pvp > 0.95") \
            .filter("dividend_yield > 8") \
            .filter("vacancia_media = 0")


    def get_dataframe(self):
        return IngestionController("fundamentus/fii").read_bronze() \
            .selectExpr(
                "papel                                                  as papel"
                ,"segmento                                              as segmento"
                ,"cotacao                                               as cotacao"
                ,"replace(replace(dividend_yield, '%',''),',','.')      as dividend_yield"
                ,"replace(pvp, ',', '.')                                as pvp"
                ,"replace(valor_mercado, '.', '')                       as valor_mercado"
                ,"replace(liquidez, '.','')                             as liquidez"
                ,"replace(replace(vacancia_media, '%',''),',','.')      as vacancia_media"
                ,"updated_at                                            as updated_at" 
                ,"current_date                                          as created_at"
            ).transform(self.set_filters_fii)


    def run(self):        
        IngestionController("fundamentus/fii").write_silver(self.get_dataframe())

# COMMAND ----------

FiiFundamentusWsToSilver().run()
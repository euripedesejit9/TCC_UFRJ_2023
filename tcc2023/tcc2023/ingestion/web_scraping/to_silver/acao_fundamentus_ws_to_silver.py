# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class AcaoFundamentusWsToSilver:

    def set_filters_ticker(self, dataframe):
        return dataframe \
            .filter("liqcorrente > 1") \
            .filter("dividend_yield > 8")


    def get_dataframe(self):
        return IngestionController("fundamentus/acoes").read_bronze() \
            .selectExpr(
                "Papel					                            as papel"
                ,"replace(Cotacao, ',', '.')			            as cotacao"
                ,"replace(PL, ',', '.') 					        as pl"
                ,"replace(PVP, ',', '.') 			                as pvp"
                ,"replace(replace(DividendYied, '%',''),',','.')	as dividend_yield"
                ,"replace(PAtivo, ',', '.') 				        as pativo"
                ,"replace(PCapGiro, ',', '.') 				        as pcapgiro"
                ,"replace(PEbit, ',', '.') 				            as pebit"
                ,"replace(PAtivoCirc, ',', '.') 				    as pativocirc"
                ,"replace(EVEbit, ',', '.') 				        as evebit"
                ,"replace(EVEbita, ',', '.') 			            as evebita"
                ,"replace(replace(MrgEbit, '%',''),',','.')		    as mrgebit"
                ,"replace(replace(MrgLiq, '%',''),',','.')			as mrgliq"
                ,"replace(LiqCorrente, ',', '.') 		            as liqcorrente"
                ,"replace(replace(ROIC, '%',''),',','.')			as roic"
                ,"replace(replace(ROE, '%',''),',','.')		        as roe"
                ,"Liq2Meses				                            as liq2meses"
                ,"PatriLiquido			                            as patriliquido"
                ,"replace(DivBruta_por_Patri, ',', '.') 		    as divbruta_por_patri"
                ,"replace(replace(Cresc_5a, '%',''),',','.')		as cresc_5a"
                ,"updated_at			                            as updated_at"
                ,"current_date                                      as created_at"
            ).transform(self.set_filters_ticker)


    def run(self):        
        IngestionController("fundamentus/acoes").write_silver(self.get_dataframe())

# COMMAND ----------

AcaoFundamentusWsToSilver().run()
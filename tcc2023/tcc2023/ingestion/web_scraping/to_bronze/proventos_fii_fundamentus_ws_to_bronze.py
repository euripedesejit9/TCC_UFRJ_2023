# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class ProventosFiiFundamentusWsToBronze:

    def __init__(self, papel, url="https://www.fundamentus.com.br/fii_proventos.php?"):
        self.papel = papel
        self.url = url + f"papel={self.papel}&tipo=2"

    def get_fii_data_list(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"}
        req = Request(self.url, headers=headers)
        response = urlopen(req)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')

        # Encontrando a tabela que contém os dados dos fundos imobiliários
        tabela = soup.find('table', class_='resultado')

        if tabela:
            # Inicializar listas para armazenar os dados
            data_fii_list = []

            # Encontrar todas as linhas da tabela
            linhas = tabela.find_all('tr')

            # Iterar pelas linhas da tabela, começando da segunda linha para ignorar o cabeçalho
            for i, linha in enumerate(linhas[1:]):
                colunas = linha.find_all('td')

                # Extrair informações específicas das colunas desejadas
                tipo = colunas[1].get_text()
                data_pgto = colunas[2].get_text()
                valor = colunas[3].get_text()

                fii_dict = {
                    'id': i,
                    'tipo': tipo,
                    'data_pgto': data_pgto,
                    'valor': valor
                }

                data_fii_list.append(fii_dict)
        else:
            print('Tabela não encontrada.')

        return data_fii_list


    def get_dataframe(self):
        return spark.createDataFrame(self.get_fii_data_list()) \
            .withColumn("papel", lit(self.papel)) \
            .withColumn("updated_at", lit(current_timestamp()))
    
    
    def run(self):       
        IngestionController("fundamentus/proventos/fii").write_bronze(self.get_dataframe())

# COMMAND ----------

tickers_gold = IngestionController("fundamentus/fii").read_gold("ufrj_tcc_2023").select("papel").distinct()
tickers_list = Utils.collect_col_values(tickers_gold, "papel")

df_list = []
for ticker in tickers_list:
    df = ProventosFiiFundamentusWsToBronze(ticker).get_dataframe()
    df_list.append(df)

df = Utils.union_all_df(df_list)
IngestionController("fundamentus/proventos/fii").write_bronze(df)
print(f"qty ticket with provents : {len(tickers_list)}")
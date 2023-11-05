# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class FiiFundamentusWsToBronze:

    def __init__(self, url="https://www.fundamentus.com.br/fii_resultado.php"):
        self.url = url

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
                papel = colunas[0].find('span', class_='tips').get_text()
                segmento = colunas[1].get_text()
                cotacao = colunas[2].get_text()
                dividend_yield =colunas[4].get_text()
                p_vp = colunas[5].get_text()
                valor_mercado = colunas[6].get_text()
                liquidez = colunas[7].get_text()
                vacancia_media = colunas[12].get_text()

                fii_dict = {
                    'id': i,
                    'papel': papel,
                    'segmento': segmento,
                    'cotacao': cotacao,
                    'dividend_yield': dividend_yield,
                    'pvp': p_vp,
                    'valor_mercado': valor_mercado,
                    'liquidez' : liquidez,
                    'vacancia_media': vacancia_media
                }
                # Adicione o dicionário de ações à lista de resumo
                data_fii_list.append(fii_dict)
        else:
            print('Tabela não encontrada.')

        return data_fii_list


    def get_dataframe(self):
        return spark.createDataFrame(self.get_fii_data_list()) \
            .withColumn("updated_at", lit(current_timestamp()))
    
    
    def run(self):        
        IngestionController("fundamentus/fii").write_bronze(self.get_dataframe())

# COMMAND ----------

FiiFundamentusWsToBronze().run()
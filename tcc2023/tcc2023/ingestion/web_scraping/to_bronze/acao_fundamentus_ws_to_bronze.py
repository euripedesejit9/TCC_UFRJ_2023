# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/ingestion/ingestion_controller

# COMMAND ----------

class AcaoFundamentusWsToBronze:

    def __init__(self, url="https://www.fundamentus.com.br/resultado.php"):
        self.url = url

    def get_ticker_data_list(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"}
        req = Request(self.url, headers=headers)
        response = urlopen(req)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')

        # Encontrando a tabela que contém os dados das acoes
        tabela = soup.find('table')

        if tabela:
            # Encontrar todas as linhas da tabela
            linhas = tabela.find_all('tr')

            #quantidade ações
            qtd = soup.findAll('span',class_='tips') 
            qtd = range(int(len(qtd)-1)) #retira o ultimo registro para não ter erro

            #Declarando variáveis cards
            resumo = []

            # Iterar pelas linhas da tabela, começando da segunda linha para ignorar o cabeçalho
            papel =  tabela.find('td').find('span',class_='tips').getText()
            cotacao = tabela.find('td').findNext('td').contents[0]

            for i in qtd:

                acoes ={}

                PL = cotacao.findNext('td').contents[0]
                PVP = PL.findNext('td').contents[0]
                PSR = PVP.findNext('td').contents[0]
                DividendYied = PSR.findNext('td').contents[0]
                PAtivo = DividendYied.findNext('td').contents[0]
                PCapGiro = PAtivo.findNext('td').contents[0]
                PEbit= PCapGiro.findNext('td').contents[0]
                PAtivoCirc= PEbit.findNext('td').contents[0]
                EVEbit= PAtivoCirc.findNext('td').contents[0]
                EVEbita= EVEbit.findNext('td').contents[0]
                MrgEbit= EVEbita.findNext('td').contents[0]
                MrgLiq= MrgEbit.findNext('td').contents[0]
                LiqCorrente= MrgLiq.findNext('td').contents[0]
                ROIC= LiqCorrente.findNext('td').contents[0]
                ROE= ROIC.findNext('td').contents[0]
                Liq2Meses= ROE.findNext('td').contents[0]
                PatriLiquido= Liq2Meses.findNext('td').contents[0]
                DivBruta_por_Patri= PatriLiquido.findNext('td').contents[0]
                Cresc_5a= DivBruta_por_Patri.findNext('td').contents[0]

                acoes['id']= i
                acoes['Papel'] = papel
                acoes['Cotacao'] = cotacao
                acoes['PL'] = PL
                acoes['PVP']=PVP
                acoes['DividendYied']=DividendYied
                acoes['PAtivo']=PAtivo
                acoes['PCapGiro']=PCapGiro
                acoes['PEbit']=PEbit
                acoes['PAtivoCirc']=PAtivoCirc
                acoes['EVEbit']=EVEbit
                acoes['EVEbita']=EVEbita
                acoes['MrgEbit']=MrgEbit
                acoes['MrgLiq']=MrgLiq
                acoes['LiqCorrente']=LiqCorrente
                acoes['ROIC']=ROIC
                acoes['ROE']=ROE
                acoes['Liq2Meses']=Liq2Meses
                acoes['PatriLiquido']=PatriLiquido
                acoes['DivBruta_por_Patri']=DivBruta_por_Patri
                acoes['Cresc_5a']=Cresc_5a

                #Adiciona o dicionário de ações em uma lista
                resumo.append(acoes)

                #try retorna erro por a ultima linha não encontra o span
                try:
                    papel = Cresc_5a.findNext('td').span.a.contents[0]
                    cotacao = papel.findPrevious('td').findNext('td').contents[0]

                except HTTPError as e:
                    print(e.status, e.reason)

        else:
            print('Tabela não encontrada.')

        return resumo


    def get_dataframe(self):
        acao_df = pd.DataFrame(self.get_ticker_data_list())
        return spark.createDataFrame(acao_df) \
            .withColumn("updated_at", lit(current_timestamp()))
    
    
    def run(self):
        IngestionController("fundamentus/acoes").write_bronze(self.get_dataframe())

# COMMAND ----------

AcaoFundamentusWsToBronze().run()
from datetime import timedelta, datetime
import pandas as pd


class Utils:

    @staticmethod
    def generate_str_date(offset):
        dt = datetime.now()
        target_dt = dt - timedelta(days=offset)
        return target_dt.strftime('%Y-%m-%d')

    @staticmethod
    def df_column(lista):
        colunas = list(lista[0].keys())
        return colunas


    @staticmethod
    def save_list_to_csv(list):
        df = pd.json_normalize(list)
        df = pd.DataFrame(df)
        return df.to_csv("fiis.csv", encoding='utf-8', index=False, sep=';', decimal=',')
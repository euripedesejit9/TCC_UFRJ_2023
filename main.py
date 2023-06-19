from utils import *
from extractor_json_status_invest import *

res =[]
for i in json_res['list']:
    res.append(i)

columns_to_remove = ['dividend_cagr', 'cota_cagr', 'gestao', 'segmentid', 'subsectorid', 'sectorname', 'companyid', 'sectorid']
for item in res:
    for column in columns_to_remove:
        item.pop(column, None)

def get_better_fii():
    print("#ALERTA -> Para casas decimais por favor utilize ponto(.)")
    print('.....')

    price_lower_bound = float(input("Digite o menor preço (R$) que você deseja pagar para adquirir um FII: "))
    price_upper_bound = float(input("Digite o maior preço (R$) que você deseja pagar para adquirir um FII: "))
    patrimonio = float(input("Digite o valor de patrimônio líquido (R$) mínimo que o FII deve ter: "))
    dividend_yield = float(input("Digite o valor de Dividend Yield (%) mínimo que o FII deve ter: "))
    p_vp = float(input("Digite o valor de P/VP (%) mínimo que o FII deve ter: "))
    liquidez_media_diaria = float(input("Digite o valor da liquidez diária (R$) mínima que o FII deve ter: "))

    filtered_fii = [
        item for item in res
        if item['dy'] >= dividend_yield
           and item['patrimonio'] >= patrimonio
           and item['p_vp'] >= p_vp
           and price_lower_bound <= item['price'] <= price_upper_bound
           and item['liquidezmediadiaria'] >= liquidez_media_diaria
    ]
    return filtered_fii

res_fii = get_better_fii()

print("De acordo com os parâmetros escolhidos, os melhores FII's para iniciar a sua análise fundamentalista são:")
print(len(res_fii))
for i in res_fii:
    print(i['ticker'])

# exporta para csv
Utils.save_list_to_csv(res_fii)
print(res_fii, "\n Arquivo Csv salvo.")
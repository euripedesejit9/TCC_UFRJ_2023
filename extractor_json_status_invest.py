import requests
url = "https://statusinvest.com.br/category/advancedsearchresultpaginated"

querystring = {"search":"{\"Segment\":\"\",\"Gestao\":\"\",\"my_range\":\"0;20\",\"dy\":{\"Item1\":null,\"Item2\":null},\"p_vp\":{\"Item1\":null,\"Item2\":null},\"percentualcaixa\":{\"Item1\":null,\"Item2\":null},\"numerocotistas\":{\"Item1\":null,\"Item2\":null},\"dividend_cagr\":{\"Item1\":null,\"Item2\":null},\"cota_cagr\":{\"Item1\":null,\"Item2\":null},\"liquidezmediadiaria\":{\"Item1\":null,\"Item2\":null},\"patrimonio\":{\"Item1\":null,\"Item2\":null},\"valorpatrimonialcota\":{\"Item1\":null,\"Item2\":null},\"numerocotas\":{\"Item1\":null,\"Item2\":null},\"lastdividend\":{\"Item1\":null,\"Item2\":null}}","orderColumn":"","isAsc":"","page":"0","take":"1500","CategoryType":"2"}

payload = ""
headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,gl;q=0.6",
    "referer": "https://statusinvest.com.br/fundos-imobiliarios/busca-avancada",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}
response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
json_res = response.json()
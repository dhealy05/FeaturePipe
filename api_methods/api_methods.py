import json
import requests
import pandas as pd
import os.path

TICKER_PATH = './data/tickers.csv'
API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

def get_tickers():
    if os.path.isfile(TICKER_PATH):
        tickers_df = pd.read_csv(TICKER_PATH)
        return tickers_df['tickers']
    else:
        tickers = get_tickers_from_api()
        tickers_df = pd.DataFrame(tickers, columns = ['tickers'])
        tickers_df.to_csv(TICKER_PATH, sep = ',', index = False)
        return tickers

def get_tickers_from_api():
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=" + API_KEY
    response = requests.get(url)
    json_data = json.loads(response.text)
    tickers = json_data['tickers']

    ticker_symbols = []
    for ticker in tickers:
        ticker_symbols.append(ticker['ticker'])

    return ticker_symbols

#def get_tickers():
#    url = "https://api.polygon.io/v2/reference/tickers?apiKey=" + API_KEY
#    response = requests.get(url)
#    json_data = json.loads(response.text)
#    print(json_data)

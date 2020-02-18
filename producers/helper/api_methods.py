import json
import requests
import pandas as pd

import os
from dotenv import load_dotenv
load_dotenv()

TICKER_PATH = './data/tickers.csv'
API_KEY = os.getenv('API_KEY')

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

        try:
            ticker = str(ticker['ticker'])
        except:
            continue

        if ticker == 'nan':
            continue

        ticker_symbols.append(ticker.lower())

    return ticker_symbols

#get_tickers())

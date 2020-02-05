import sys
sys.path.insert(0, './api_methods')
from api_methods import get_tickers

import json
import requests
import pulsar
from websocket import create_connection
from pulsar.schema import *

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

consumer_dictionary = {}

class Stock(Record):
    symbol = String()
    exchange_id = Integer()
    trade_id = Integer()
    price = Float()
    size = Integer()
    tape = Integer()
    time = Long()
    #conditions = List()

def init_consumers():

    count = 0

    tickers = get_tickers()

    for ticker in tickers:

        try:
            ticker = str(ticker)
        except:
            continue

        if ticker == 'nan':
            continue

        if type(ticker) == str:
            print(ticker)
            consumer_dictionary[ticker] = client.subscribe(ticker, subscription_name=ticker + "_sub", schema=AvroSchema(Stock))

        print(count)
        count = count + 1

init_consumers()

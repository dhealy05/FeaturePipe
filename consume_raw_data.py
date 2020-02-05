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

tickers = get_tickers()

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

    #tickers = get_tickers()

    for ticker in tickers:

        print(ticker)
        consumer_dictionary[ticker] = client.subscribe(ticker, subscription_name=ticker + "_sub", schema=AvroSchema(Stock))

        print(count)
        count = count + 1

init_consumers()

while True:

    for ticker in tickers:

        msg = consumer_dictionary[ticker].receive()
        print(msg.value())
        consumer_dictionary[ticker].acknowledge(msg)

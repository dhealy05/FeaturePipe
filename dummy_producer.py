import sys
sys.path.insert(0, './api_methods')
from api_methods import get_tickers

try:
    import thread
except ImportError:
    import _thread as thread

import websocket
import json
import requests
import pulsar
from pulsar.schema import *
import pickle
import os.path
import random

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

producer_dictionary = {}
producer_count = 0

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

def init_producers():

    count = 0

    #tickers = get_tickers()

    for ticker in tickers:

        try:
            ticker = str(ticker)
        except:
            continue

        if ticker == 'nan':
            continue

        if type(ticker) == str:
            producer_dictionary[ticker] = client.create_producer(ticker, schema=AvroSchema(Stock))

        print(count)
        count = count + 1

def make_stock(symbol):
    return Stock(symbol = symbol, exchange_id = 1, trade_id = 1, price = 100.0, tape = 1, size = 1, time = 1000000)

def send_message(ticker):
    stock = make_stock(ticker)
    producer_dictionary[ticker].send(stock)

init_producers()

while True:

    try:
        ticker = str(ticker)
    except:
        continue

    if ticker == 'nan':
        continue
        
    symbol = random.choice(tickers)
    send_message(symbol)

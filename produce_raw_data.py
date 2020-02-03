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
producer = client.create_producer('raw_stock_data', schema=AvroSchema(Stock))

producer_dictionary = {}

ws = create_connection("wss://socket.polygon.io/stocks")
response = ws.recv()

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
    tickers = get_tickers()
    for ticker in tickers:
        producer_dictionary[ticker] = client.create_producer(ticker, schema=AvroSchema(Stock))

def init_websocket():

    auth = {"action":"auth","params":API_KEY}
    auth_json = json.dumps(auth)
    ws.send(auth_json)
    response = ws.recv()

    subscribe = {"action":"subscribe","params":"T.MSFT"}
    subscribe_json = json.dumps(subscribe)
    ws.send(subscribe_json)
    response = ws.recv()

    tickers = get_tickers()

    for ticker in tickers:

        try:
            symbol = str(ticker)
        except:
            continue

        subscribe = {"action":"subscribe","params":"T.*"}
        subscribe_json = json.dumps(subscribe)
        ws.send(subscribe_json)
        result = ws.recv()
        print(result)

def produce_all():

    while True:
        result = ws.recv()
        print(result)
        #send_message(result)

def send_message(result):
    json_result = json.loads(result)
    final = json_result[0]

    if final['ev'] == 'T':
        stock = Stock(symbol = final['sym'], exchange_id = final['x'], trade_id = final['i'], price = final['p'], tape = final['z'], size = final['s'], time = final['t'])
        producer_dictionary[final['sym']].send(stock)

init_websocket()
produce_all()

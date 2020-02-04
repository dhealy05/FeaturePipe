import sys
sys.path.insert(0, './api_methods')
from api_methods import get_tickers

import json
import requests
import pulsar
from websocket import create_connection
from pulsar.schema import *
import pickle
import os.path

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"
PRODUCER_PATH = "./data/producers.pkl"

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

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

def get_producer_dictionary():
    if os.path.isfile(PRODUCER_PATH):
        with open(PRODUCER_PATH, 'rb') as input:
            producer_dictionary = pickle.load(input)
    else:
        init_producers()

def init_producers():

    tickers = get_tickers()

    for ticker in tickers:
        create_producer(ticker)

    with open(PRODUCER_PATH, 'wb') as output:
        pickle.dump(producer_dictionary, output, pickle.HIGHEST_PROTOCOL)

def create_producer(ticker):
    producer_dictionary[ticker] = client.create_producer(ticker, schema=AvroSchema(Stock))

def init_websocket():

    auth = {"action":"auth","params":API_KEY}
    auth_json = json.dumps(auth)
    ws.send(auth_json)
    response = ws.recv()

    subscribe = {"action":"subscribe","params":"T.*"}
    subscribe_json = json.dumps(subscribe)
    ws.send(subscribe_json)
    response = ws.recv()
    print(result)

def produce_all():

    while True:
        result = ws.recv()
        #print(result)
        send_message(result)

def make_stock(result_json):

    symbol, exchange_id, trade_id, price, tape, size, time = result_json.get('sym', -1), result_json.get('x', -1), result_json.get('i', -1), result_json.get('p', -1), result_json.get('x', -1), result_json.get('s', -1), result_json.get('t', -1)
    vars = [symbol, exchange_id, trade_id, price, tape, size, time]

    if -1 in vars:
        return Stock(symbol = "bad_data", exchange_id = 1, trade_id = 1, price = 1.0, tape = 1, size = 1, time = 1000000000)

    return Stock(symbol = symbol, exchange_id = exchange_id, trade_id = trade_id, price = price, tape = tape, size = size, time = time)

def send_message(result):

    json_result = json.loads(result)

    if len(result) > 1:

        final = json_result[0]

        if final.get('ev', 0) == 'T':

            stock = make_stock(final)
            producer.send(stock)

producer_dictionary = get_producer_dictionary()
init_websocket()
produce_all()

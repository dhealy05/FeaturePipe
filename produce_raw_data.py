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

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

tickers = get_tickers()
final_tickers = []

producer_dictionary = {}
producer_count = 0

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
            final_tickers.append(ticker)

        print(count)
        count = count + 1

def make_stock(result_json):

    symbol, exchange_id, trade_id, price, tape, size, time = result_json.get('sym', -1), result_json.get('x', -1), result_json.get('i', -1), result_json.get('p', -1), result_json.get('x', -1), result_json.get('s', -1), result_json.get('t', -1)
    vars = [symbol, exchange_id, trade_id, price, tape, size, time]

    if -1 in vars:
        return Stock(symbol = "bad_data", exchange_id = 1, trade_id = 1, price = 1.0, tape = 1, size = 1, time = 1000000000)

    try:
        symbol = str(symbol)
    except:
        return Stock(symbol = "bad_data", exchange_id = 1, trade_id = 1, price = 1.0, tape = 1, size = 1, time = 1000000000)

    return Stock(symbol = symbol.lower(), exchange_id = exchange_id, trade_id = trade_id, price = price, tape = tape, size = size, time = time)

def send_message(result):

    json_result = json.loads(result)

    if len(result) > 1:

        final = json_result[0]

        if final.get('ev', 0) == 'T':

            ticker = final.get('sym', -1)

            if ticker != -1:

                try:
                    ticker = str(ticker)
                except:
                    ticker = -1

                if ticker != -1:

                    ticker = ticker.lower()
                    stock = make_stock(final)

                    if ticker in final_tickers:
                        producer_dictionary[ticker].send(stock)

def on_message(ws, message):
    print(message)
    send_message(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):

    def run(*args):

        auth = {"action":"auth","params":API_KEY}
        auth_json = json.dumps(auth)
        ws.send(auth_json)

        subscribe = {"action":"subscribe","params":"T.*"}
        subscribe_json = json.dumps(subscribe)
        ws.send(subscribe_json)

    thread.start_new_thread(run, ())

if __name__ == "__main__":

    init_producers()

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://socket.polygon.io/stocks",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()

#init_producers()
#print("PRODUCED")
#producer_dictionary = get_producer_dictionary()
#init_websocket()
#print("SOCKETED")
#produce_all()

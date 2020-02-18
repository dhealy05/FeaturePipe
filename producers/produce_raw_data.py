import sys
sys.path.insert(0, './helper')
from api_methods import get_tickers
from init_producers import init_producers
from schemas import Stock

try:
    import thread
except ImportError:
    import _thread as thread

import websocket
import json

import os
from dotenv import load_dotenv
load_dotenv()

import pulsar
from pulsar.schema import *

API_KEY = os.getenv('API_KEY')

producer_dictionary, final_tickers = {}, []

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
                        producer_dictionary["all_stocks"].send(stock)

def on_message(ws, message):
    print(message)
    send_message(message)

def on_error(ws, error):
    print("### error ###")
    print(error)
    ws.close()

def on_close(ws):
    print("### closed ###")
    boot_websocket()

def on_open(ws):

    def run(*args):

        auth = {"action":"auth","params":API_KEY}
        auth_json = json.dumps(auth)
        ws.send(auth_json)

        subscribe = {"action":"subscribe","params":"T.*"}
        subscribe_json = json.dumps(subscribe)
        ws.send(subscribe_json)

    thread.start_new_thread(run, ())

def boot_websocket():

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://socket.polygon.io/stocks", on_message = on_message, on_error = on_error, on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()

if __name__ == "__main__":

    producer_dictionary, final_tickers = init_producers(get_tickers(), features = False, default_topic_only = True)
    boot_websocket()

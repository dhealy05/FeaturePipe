import json
import requests
import pulsar
from websocket import create_connection
from pulsar.schema import *

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

#client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
#producer = client.create_producer('raw_stock_data', schema=AvroSchema(Stock))

class Stock(Record):
    symbol = String()
    exchange_id = Integer()
    trade_id = Integer()
    price = Float()
    size = Integer()
    tape = Integer()
    time = Long()
    #conditions = List()

def auth_websocket():

    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=" + API_KEY
    response = requests.get(url)
    json_data = json.loads(response.text)
    tickers = json_data['tickers']

    client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
    #producer = client.create_producer('stonks', schema=AvroSchema(Stock))
    producer_dictionary = {}

    ws = create_connection("wss://socket.polygon.io/stocks")
    response = ws.recv()

    auth = {"action":"auth","params":API_KEY}
    auth_json = json.dumps(auth)
    ws.send(auth_json)
    response = ws.recv()

    for ticker in tickers:
        try:
            symbol = str(ticker['ticker'])
        except:
            continue
        subscribe = {"action":"subscribe","params":"T."+symbol}
        subscribe_json = json.dumps(subscribe)
        ws.send(subscribe_json)
        #producer = client.create_producer(symbol, schema=AvroSchema(Stock))
        producer_dictionary[symbol] = client.create_producer(symbol, schema=AvroSchema(Stock))

    while True:
        result = ws.recv()
        json_result = json.loads(result)
        final = json_result[0]
        if final['ev'] == 'T':
            stock = Stock(symbol = final['sym'], exchange_id = final['x'], trade_id = final['i'], price = final['p'], tape = final['z'], size = final['s'], time = final['t'])
            producer_dictionary[final['sym']].send(stock)

auth_websocket()
#stock_snapshot()

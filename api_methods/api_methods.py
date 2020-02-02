import json
import requests
from websocket import create_connection

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

def auth_websocket():
    ws = create_connection("wss://socket.polygon.io/stocks")
    response = ws.recv()
    print(response)
    auth = {"action":"auth","params":API_KEY}
    auth_json = json.dumps(auth)
    ws.send(auth_json)
    response = ws.recv()
    print(response)
    #ws.close()
    subscribe = {"action":"subscribe","params":"T.SPY"}
    subscribe_json = json.dumps(subscribe)
    ws.send(subscribe_json)

    while True:
        result = ws.recv()
        #result = json.loads(result)
        #xxx = result
        print(result)
        #print(result[1])

def stock_snapshot():
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=" + API_KEY
    response = requests.get(url)
    json_data = json.loads(response.text)
    tickers = json_data['tickers']
    for ticker in tickers:
        print(ticker['ticker'])
    #'count': 8707, 'status': 'OK', 'tickers': [...]

def get_tickers():
    url = "https://api.polygon.io/v2/reference/tickers?apiKey=" + API_KEY
    response = requests.get(url)
    json_data = json.loads(response.text)
    print(json_data)

#auth_websocket()
stock_snapshot()
#get_tickers()

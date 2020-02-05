#def create_producer(ticker):
#    producer_dictionary[ticker] = client.create_producer(ticker, schema=AvroSchema(Stock))

#def init_websocket():

#    ws = create_connection("wss://socket.polygon.io/stocks")
#    response = ws.recv()

#    auth = {"action":"auth","params":API_KEY}
#    auth_json = json.dumps(auth)
#    ws.send(auth_json)
#    response = ws.recv()

#    subscribe = {"action":"subscribe","params":"T.*"}
#    subscribe_json = json.dumps(subscribe)
#    ws.send(subscribe_json)
#    response = ws.recv()

#    while True:
#        result = ws.recv()
        #print(result)
#        send_message(result)

#def produce_all():

#    while True:
#        result = ws.recv()
        #print(result)
#        send_message(result)

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
            print(symbol)
        except:
            continue

        subscribe = {"action":"subscribe","params":"T."+symbol}
        subscribe_json = json.dumps(subscribe)
        ws.send(subscribe_json)
        result = ws.recv()
        print(result)

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

    final_tickers = ""

    for ticker in tickers:

        try:
            symbol = str(ticker)
            final_tickers += ",T."+symbol
        except:
            continue

    print(final_tickers)

    subscribe = {"action":"subscribe","params":final_tickers}
    subscribe_json = json.dumps(subscribe)
    ws.send(subscribe_json)
    result = ws.recv()
    print(result)

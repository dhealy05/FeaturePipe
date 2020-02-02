import json
import requests
import pulsar
from websocket import create_connection
from pulsar.schema import *

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

class Stock(Record):
    symbol = String()
    exchange_id = Integer()
    trade_id = Integer()
    price = Float()
    size = Integer()
    tape = Integer()
    time = Long()
    #conditions = List()

def consume_all_tickers():

    client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=" + API_KEY
    response = requests.get(url)
    json_data = json.loads(response.text)
    tickers = json_data['tickers']

    consumer_dictionary = {}

    for ticker in tickers:
        try:
            symbol = str(ticker['ticker'])
        except:
            print("Bad Symbol")
        consumer_dictionary[symbol] = client.subscribe(symbol,
                                            subscription_name=symbol + "_sub",
                                            schema=AvroSchema(Stock))

    #consumer = client.subscribe('schema_test',
    #                        subscription_name='schema_test_sub',
    #                        schema=AvroSchema(Example))

#help(consumer)
#while True:
#    msg = consumer.receive()
#    print(msg.value())
#    print("Received message phrase={} id={} greeting={}".format(ex.phrase, ex.id, ex.greeting))
    #print("Received message: '%s'" % msg.data())
#    consumer.acknowledge(msg)

#client.close()

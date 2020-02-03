import pulsar
from pulsar.schema import *
from pyhive import presto

class Stock(Record):
    symbol = String()
    exchange_id = Integer()
    trade_id = Integer()
    price = Float()
    size = Integer()
    tape = Integer()
    time = Long()
    #conditions = List()

class Features(Record):
    symbol = String()
    price = Float()
    ma_15 = Float()

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
consumer = client.subscribe('msft_test',
                            subscription_name='msft_test_sub',
                            schema=AvroSchema(Stock))

cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

while True:
    msg = consumer.receive()
    ex = msg.value()
    #print("Received message phrase={} id={} greeting={}".format(ex.phrase, ex.id, ex.greeting))
    #print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

client.close()

def get_all_averages():
    seconds = time.time()
    fifteen_minute = (seconds - (60*15))*1000
    average_price('msft_test', fifteen_minute)

def average_price(symbol, boundary):
    query = 'SELECT AVG(price) FROM pulsar."public/default".' + symbol + ' WHERE __publish_time__ > ' + str(boundary)
    cursor.execute(query)
    print cursor.fetchone()
    print cursor.fetchall()

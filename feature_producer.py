import pulsar
import time
import schedule
from pulsar.schema import *
from pyhive import presto

class Features(Record):
    symbol = String()
    #price = Float()
    ma_15 = Float()

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
producer = client.create_producer('msft_test_features', schema=AvroSchema(Features))

cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

def get_all_averages():
    seconds = time.time()
    fifteen_minute = (seconds - (60*15))
    average_price('msft_test', fifteen_minute)

def average_price(symbol, boundary):
    query = 'SELECT AVG(price) FROM pulsar."public/default".' + symbol + ' WHERE time > ' + str(boundary)
    cursor.execute(query)
    result = cursor.fetchone()
    #print cursor.fetchall()
    send_message(result[0])

def send_message(ma):
    features = Features(symbol = 'msft_test_features', ma_15 = ma)
    producer.send(features)


schedule.every(10).seconds.do(get_all_averages)
#get_all_averages()
while True:
    schedule.run_pending()

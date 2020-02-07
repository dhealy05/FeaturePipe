import sys
sys.path.insert(0, './api_methods')
from api_methods import get_tickers

#import pulsar
import time
import schedule
#from pulsar.schema import *
from pyhive import presto
import asyncio


#class Features(Record):
#    symbol = String()
#    price = Float()
#    ma_1 = Float()
#    ma_5 = Float()
#    std_1 = Float()
#    std_5 = Float()

#client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
#producer = client.create_producer("all_features", schema=AvroSchema(Features))

tickers = get_tickers()
final_tickers = []

producer_dictionary = {}
producer_count = 0

#feature_set = ['ma_1', 'ma_10', 'ma_60', 'ma_120', 'std_1', 'std_10', 'std_60', 'std_120', 'vol_1', 'vol_10', 'vol_60', 'vol_120']
#feature_set = ['ma_1', 'ma_5', 'std_1', 'std_5', 'vol_1', 'vol_5']
feature_set = ['avg_1', 'avg_5', 'stdev_1', 'stdev_5']

cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

def init_producers():

    producer_dictionary["all_features"] = client.create_producer("all_features", schema=AvroSchema(Features))

    count = 0

    tickers = get_tickers()

    for ticker in tickers:

        try:
            ticker = str(ticker)
        except:
            continue

        if ticker == 'nan':
            continue

        if type(ticker) == str:
            producer_dictionary[ticker] = client.create_producer(ticker + "_features", schema=AvroSchema(Features))
            final_tickers.append(ticker)

        print(count)
        count = count + 1

#def get_all_features():
#    for feature in feature_set:
#        action, num_minutes = feature.split("_")

#def get_feature(action, num_minutes):
    #action == AVG or action == STDDEV
#    seconds = time.time()
#    boundary = (seconds - (60*num_minutes))
#    query = 'SELECT ' + action + '(price), symbol, price FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
#    cursor.execute(query)
#    result = cursor.fetchone()
    #print cursor.fetchall()
    #send_message(result[0])

def send_message(ma):
    features = Features(symbol = 'msft_test_features', ma_15 = ma)
    producer.send(features)

#get_all_features()

loop = asyncio.get_event_loop()

def query():

    async def get_feature(action, num_minutes):
        #action == AVG or action == STDDEV
        seconds = time.time()
        boundary = (seconds - (60*num_minutes))
        query = 'SELECT ' + action + '(price), symbol, price FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
        cursor.execute(query)
        result = cursor.fetchone()
        print(result)
        #print cursor.fetchall()
        #send_message(result[0])

    async_tasks = []

    for feature in feature_set:
        action, num_minutes = feature.split("_")
        async_tasks.append(loop.create_task(get_feature(action, num_minutes)))

    loop.run_until_complete(asyncio.gather(*async_tasks))

query()
#schedule.every(10).seconds.do(get_all_averages)

#while True:
#    schedule.run_pending()

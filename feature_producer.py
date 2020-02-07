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
feature_set = ['avg_1', 'avg_5', 'stddev_1', 'stddev_5']

cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

from threading import Thread, Lock

def get_feature(action, num_minutes):
    #action == AVG or action == STDDEV
    seconds = time.time()
    boundary = (seconds - (60*num_minutes))
    query = 'SELECT ' + action + '(price), symbol, price FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
    cursor.execute(query)
    result = cursor.fetchall()
    return result
    #print cursor.fetchall()
    #send_message(result[0])

def get_all_features():
    for feature in feature_set:
        action, num_minutes = feature.split("_")
        global_features[feature] = get_feature(action, num_minutes)

def get_all_queries():

    queries = []
    seconds = time.time()

    for feature in feature_set:
        action, num_minutes = feature.split("_")
        boundary = (seconds - (60*num_minutes))
        query = 'SELECT ' + action + '(price), symbol, price FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
        queries.append(query)

    return queries

def send_message(ma):
    features = Features(symbol = 'msft_test_features', ma_15 = ma)
    producer.send(features)

class QueryWorker(Thread):
    __lock = Lock()

    def __init__(self, query, result_queue):
        Thread.__init__(self)
        self.query = query
        self.result_queue = result_queue

    def run(self):

        result = None
        logging.info("Connecting to database...")

        try:
            cursor.execute(query)
            result = cursor.fetchall()
        except Exception as e:
            logging.error("Unable to access database %s" % str(e))

        self.result_queue.append(result)

delay = 1
result_queue = []

workers = []
queries = get_all_queries()

for query in queries:
    worker = QueryWorker(query, result_queue)
    workers.append(worker)

for worker in workers:
    worker.start()

# Wait for the job to be done
while len(result_queue) < len(feature_set):
    sleep(delay)

job_done = True

for worker in workers:
    worker.join()

print(result_queue)    

#schedule.every(10).seconds.do(get_all_averages)

#while True:
#    schedule.run_pending()

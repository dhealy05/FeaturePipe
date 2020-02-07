import sys
sys.path.insert(0, './api_methods')
from api_methods import get_tickers

#import pulsar
import time
import schedule
#from pulsar.schema import *
from pyhive import presto
import asyncio

from threading import Thread, Lock

#class Features(Record):
#    symbol = String()
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

def get_all_queries():

    queries = []
    #seconds = time.time()
    seconds = 1581094483536

    for feature in feature_set:
        action, num_minutes = feature.split("_")
        num_minutes = int(num_minutes)
        boundary = (seconds - (60*num_minutes))
        query = 'SELECT ' + action + '(price), symbol FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
        queries.append([query, feature])

    return queries

class QueryWorker(Thread):
    __lock = Lock()

    def __init__(self, query, feature, result_queue):
        Thread.__init__(self)
        self.query = query
        self.feature = feature
        self.result_queue = result_queue

    def run(self):

        result = None
        print("Connecting to database...")

        try:
            cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()
            cursor.execute(self.query)
            result = cursor.fetchall()
        except Exception as e:
            print("Unable to access database %s" % str(e))

        self.result_queue.append([result, self.feature])

def run_all_queries():

    delay = 1
    result_queue = []

    workers = []
    queries = get_all_queries()

    for query in queries:
        worker = QueryWorker(query[0], query[1], result_queue)
        workers.append(worker)

    for worker in workers:
        worker.start()

    # Wait for the job to be done
    while len(result_queue) < len(feature_set):
        time.sleep(delay)

    job_done = True

    for worker in workers:
        worker.join()

    for results in result_queue:
        print(results[1])

    make_features(result_queue)

def make_features(queue):

    feature_dictionary = {}

    for query_result in queue[0][0]:
        feature_dictionary[query_result[1]] = {'symbol': query_result[1]}
    #for ticker in final_tickers:
    #    feature_dictionary[ticker] = {}

    for result in queue:

        feature = result[1]

        for query_result in result[0]:
            feature_dictionary[str(query_result[1])][feature] = str(query_result[0])

    for symbol in feature_dictionary:
        print(feature_dictionary[symbol])

run_all_queries()

#schedule.every(30).seconds.do(run_all_queries)

#while True:
#    schedule.run_pending()

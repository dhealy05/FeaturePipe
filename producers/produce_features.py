import sys
sys.path.insert(0, './helper')
from api_methods import get_tickers
from init_producers import init_producers
from schemas import Features

import pulsar
from pulsar.schema import *

import time
import schedule

from pyhive import presto
import asyncio

from threading import Thread, Lock

producer_dictionary, final_tickers = {}, []

#feature_set = ['avg_1', 'avg_5', 'stddev_1', 'stddev_5']
feature_set = ['avg_1', 'avg_5', 'avg_10', 'avg_15', 'avg_30', 'avg_60', 'avg_120', 'avg_240', 'avg_480', 'avg_960', 'avg_1440', 'stddev_1', 'stddev_5', 'stddev_10', 'stddev_15', 'stddev_30', 'stddev_60', 'stddev_120', 'stddev_240', 'stddev_480', 'stddev_960', 'stddev_1440', 'vol_1', 'vol_5', 'vol_10', 'vol_15', 'vol_30', 'vol_60', 'vol_120', 'vol_240', 'vol_480', 'vol_960', 'vol_1440']

def get_all_queries():

    queries = []
    #seconds = time.time()
    miliseconds = 1581094483536

    for feature in feature_set:
        action, num_minutes = feature.split("_")
        num_minutes = int(num_minutes)
        #boundary = (seconds - (60*num_minutes))*1000
        boundary = (miliseconds - (60*num_minutes)*1000)
        query = 'SELECT ' + action + '(price) as ' + feature + ', symbol FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
        queries.append({"query":query, "feature":feature})

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

        self.result_queue.append({"array":result, "feature":self.feature})

def run_all_queries():

    delay = 1
    result_queue = []

    workers = []
    queries = get_all_queries()

    for query_dict in queries:
        worker = QueryWorker(query_dict['query'], query_dict['feature'], result_queue)
        workers.append(worker)

    for worker in workers:
        worker.start()

    # Wait for the job to be done
    while len(result_queue) < len(feature_set):
        time.sleep(delay)

    job_done = True

    for worker in workers:
        worker.join()

    make_features(result_queue)

def make_features(queue):

    feature_dictionary = {}

    for feature, symbol in queue[0]['array']:
        feature_dictionary[symbol] = {'symbol': symbol}

    #for ticker in final_tickers:
    #    feature_dictionary[ticker] = {}

    for result in queue:

        for feature, symbol in result['array']:
            try:
                feature_dictionary[symbol][result['feature']] = str(feature)
            except:
                print("key error")

    for symbol in feature_dictionary:
        #print(feature_dictionary[symbol])
        feature_object = make_feature_object(feature_dictionary[symbol])
        producer_dictionary[symbol].send(feature_object)
        producer_dictionary["all_features"].send(feature_object)

def make_feature_object(dict):

    variables = [dict.get('symbol', 'bad_symbol')]

    for feature in feature_set:
        variables.append(dict.get(feature), 0.0)

    for i in range(0, len(variables)):
        if None == variables[i] or 'None' == variables[i]:
            variables[i] = 0.0

    #feature_object = Features(symbol = variables[0], avg_1 = variables[1], avg_5 = variables[2], stddev_1 = variables[3], stddev_5 = variables[4])
    features_object = Features(symbol = variables[0], avg_1 = variables[1], avg_5 = variables[2], avg_10 = variables[3], avg_15 = variables[4], avg_30 = variables[5], avg_60 = variables[6], avg_120 = variables[7], avg_240 = variables[8], avg_480 = variables[9], avg_960 = variables[10], avg_1440 = variables[11], stddev_1 = variables[12], stddev_5 = variables[13], stddev_10 = variables[14], stddev_15 = variables[15], stddev_30 = variables[16], stddev_60 = variables[17], stddev_120 = variables[18], stddev_240 = variables[19], stddev_480 = variables[20], stddev_960 = variables[21], stddev_1440 = variables[22], vol_1 = variables[23], vol_5 = variables[24], vol_10 = variables[25], vol_15 = variables[26], vol_30 = variables[27], vol_60 = variables[28], vol_120 = variables[29], vol_240 = variables[30], vol_480 = variables[31], vol_960 = variables[32], vol_1440 = variables[33])

    return feature_object

producer_dictionary, final_tickers = init_producers(get_tickers(), features = True)
#run_all_queries()

schedule.every(30).seconds.do(run_all_queries)

while True:
    schedule.run_pending()

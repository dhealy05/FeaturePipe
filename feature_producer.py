#import pulsar
import time
import schedule
#from pulsar.schema import *
from pyhive import presto

class Features(Record):
    symbol = String()
    price = Float()
    ma_1 = Float()
    ma_5 = Float()
    std_1 = Float()
    std_5 = Float()

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
producer = client.create_producer("all_features", schema=AvroSchema(Features))

#feature_set = ['ma_1', 'ma_10', 'ma_60', 'ma_120', 'std_1', 'std_10', 'std_60', 'std_120', 'vol_1', 'vol_10', 'vol_60', 'vol_120']
#feature_set = ['ma_1', 'ma_5', 'std_1', 'std_5', 'vol_1', 'vol_5']
feature_set = ['ma_1', 'ma_5', 'std_1', 'std_5']

cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

def get_feature(action, num_minutes):
    #action == AVG or action == STDDEV
    seconds = time.time()
    boundary = (seconds - (60*num_minutes))
    query = 'SELECT ' + action + '(price), symbol FROM pulsar."public/default".all_stocks WHERE time > ' + str(boundary) + ' GROUP BY symbol'
    cursor.execute(query)
    result = cursor.fetchone()
    #print cursor.fetchall()
    #send_message(result[0])

def send_message(ma):
    features = Features(symbol = 'msft_test_features', ma_15 = ma)
    producer.send(features)

schedule.every(10).seconds.do(get_all_averages)
#get_all_averages()
while True:
    schedule.run_pending()

import pulsar
from pulsar.schema import *
from pyhive import presto

class Features(Record):
    symbol = String()
    avg_1 = Float()
    avg_5 = Float()
    stddev_1 = Float()
    stddev_5 = Float()

#client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
client = pulsar.Client('pulsar://34.220.60.121:6650,34.222.114.52:6650,54.190.168.35:6650')

def subscribe(symbol):
    symbol = symbol.lower()
    consumer = client.subscribe(symbol+'_features', subscription_name='_features_sub', schema=AvroSchema(Features))
    #consumer = client.subscribe(symbol+'_features', schema=AvroSchema(Features))
    return consumer

def query(symbol, query):
    cursor = presto.connect('54.188.179.210', port=8081, username="featurepipe-client").cursor()
    cursor.execute(self.query)
    result = cursor.fetchall()
    return result

consumer = subscribe('msft')

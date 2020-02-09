import pulsar
from pulsar.schema import *

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
consumer = client.subscribe('raw_stock_data', subscription_name='raw_stock_data_sub', schema=AvroSchema(Stock))

#producer = client.create_producer('msft_test_features', schema=AvroSchema(Features))

#cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

while True:
    msg = consumer.receive()
    ex = msg.value()
    #print("Received message phrase={} id={} greeting={}".format(ex.phrase, ex.id, ex.greeting))
    #print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

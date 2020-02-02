import pulsar
from pulsar.schema import *

class Example(Record):
    phrase = String()
    id = Integer()
    greeting = Boolean()

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
consumer = client.subscribe('schema_test',
                            subscription_name='schema_test_sub',
                            schema=AvroSchema(Example))

#help(consumer)
while True:
    msg = consumer.receive()
    ex = msg.value()
    print(ex)
    print("Received message phrase={} id={} greeting={}".format(ex.phrase, ex.id, ex.greeting))
    #print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

client.close()

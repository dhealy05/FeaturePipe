import pulsar
from pulsar.schema import *

class Example(Record):
    phrase = String()
    id = Integer()
    greeting = Boolean()

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
producer = client.create_producer(
                    topic='A')

#producer.send(Example(phrase='Test Greeting', id=-1, greeting=True))
#producer.send(Example(phrase='NiHao', id=0, greeting=True))
#producer.send(Example(phrase='Bonjour', id=1, greeting=True))
#producer.send(Example(phrase='Bonjourno', id=2, greeting=True))
#producer.send(Example(phrase='Adios', id=3, greeting=False))
producer.send('hey')
producer.send('hi')
producer.send('ho')
producer.send('ha')
producer.send('hoo')

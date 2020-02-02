import pulsar
from pulsar.schema import *

class Example(Record):
    phrase = String()
    id = Integer()
    greeting = Boolean()

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
producer = client.create_producer(
                    topic='schema_test',
                    schema=AvroSchema(Example) )

producer.send(Example(phrase='Hello', id=1, greeting=True))
producer.send(Example(phrase='Yo', id=2, greeting=True))
producer.send(Example(phrase='Hey', id=3, greeting=True))
producer.send(Example(phrase='Hi', id=4, greeting=True))
producer.send(Example(phrase='Goodbye', id=5, greeting=False))
producer.send(Example(phrase='Farewell', id=6, greeting=False))

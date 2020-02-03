import json
import requests
import pulsar
import asyncio
from websocket import create_connection
from pulsar.schema import *

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

class Stock(Record):
    symbol = String()
    exchange_id = Integer()
    trade_id = Integer()
    price = Float()
    size = Integer()
    tape = Integer()
    time = Long()
    #conditions = List()

queue = asyncio.Queue(loop=loop)

async def produce(ws):
    while True:
        result = ws.recv()
        await queue.put(result)
        #json_result = json.loads(result)
        #final = json_result[0]
        #stock = Stock(symbol = final['sym'], exchange_id = final['x'], trade_id = final['i'], price = final['p'], tape = final['z'], size = final['s'], time = final['t'])
        #producer.send(stock)

async def consume(producer):
    while True:
        # wait for an item from the producer
        result = await queue.get()

        json_result = json.loads(result)
        final = json_result[0]
        stock = Stock(symbol = final['sym'], exchange_id = final['x'], trade_id = final['i'], price = final['p'], tape = final['z'], size = final['s'], time = final['t'])
        producer.send(stock)

        # Notify the queue that the item has been processed
        queue.task_done()


client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')
producer = client.create_producer('msft_test', schema=AvroSchema(Stock))

ws = create_connection("wss://socket.polygon.io/stocks")
response = ws.recv()

auth = {"action":"auth","params":API_KEY}
auth_json = json.dumps(auth)
ws.send(auth_json)
response = ws.recv()

subscribe = {"action":"subscribe","params":"T."+symbol}
subscribe_json = json.dumps(subscribe)
ws.send(subscribe_json)
response = ws.recv()

loop = asyncio.get_event_loop()

loop.create_task(produce(ws))
loop.create_task(consume(producer))

loop.run_forever()

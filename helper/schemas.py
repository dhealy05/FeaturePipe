from pulsar.schema import *

class Stock(Record):
    symbol = String()
    exchange_id = Integer()
    trade_id = Integer()
    price = Float()
    size = Integer()
    tape = Integer()
    time = Long()

class Features(Record):
    symbol = String()
    avg_1 = Float()
    avg_5 = Float()
    stddev_1 = Float()
    stddev_5 = Float()

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
    ma_1 = Float()
    ma_5 = Float()
    std_1 = Float()
    std_5 = Float()

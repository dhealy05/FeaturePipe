from pulsar.schema import *

class Features(Record):
    symbol = String()
    avg_1 = Float()
    avg_5 = Float()
    stddev_1 = Float()
    stddev_5 = Float()

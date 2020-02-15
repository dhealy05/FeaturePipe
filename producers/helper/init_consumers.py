import pulsar
from pulsar.schema import *
from schemas import Stock, Feature
from api_methods import get_tickers

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

tickers, consumer_dictionary = get_tickers(), {}

def init_consumers(features = False, default_topic_only = False):

    default_topic, default_schema, default_suffix = 'all_stocks', AvroSchema(Stock), ""

    if features:
        default_topic, default_schema, default_suffix = 'all_features', AvroSchema(Feature), "_features"

    consumer_dictionary[default_topic] = client.subscribe(default_topic, subscription_name=default_topic + "_sub", schema=default_schema)

    if default_topic_only:
        return

    count = 0

    for ticker in tickers:

        try:
            ticker = str(ticker)
        except:
            continue

        if ticker == 'nan':
            continue

        if type(ticker) == str:
            print(ticker)
            consumer_dictionary[ticker] = client.subscribe(ticker+default_suffix, subscription_name=ticker + default_suffix + "_sub", schema=default_schema)

        print(count)
        count = count + 1

init_consumers()

while True:
        msg = consumer_dictionary['all_features'].receive()
        print(msg.value())
        consumer_dictionary['all_features'].acknowledge(msg)

#    for ticker in tickers:

#        try:
#            ticker = str(ticker)
#        except:
#            continue

#        if ticker == 'nan':
#            continue

#        msg = consumer_dictionary[ticker].receive()
#        print(msg.value())
#        consumer_dictionary[ticker].acknowledge(msg)

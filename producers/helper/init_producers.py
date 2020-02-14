import pulsar
from pulsar.schema import *
from schemas import Stock, Features

client = pulsar.Client('pulsar://10.0.0.7:6650,10.0.0.8:6650,10.0.0.9:6650')

def init_producers(tickers, features = False, default_topic_only = False):

    producer_dictionary, final_tickers = {}, []

    default_topic, default_schema, default_suffix = 'all_stocks', AvroSchema(Stock), ""

    if features:
        default_topic, default_schema, default_suffix = 'all_features', AvroSchema(Features), "_features"

    producer_dictionary[default_topic] = client.create_producer(default_topic, schema=default_schema)

    count = 0

    for ticker in tickers:

        try:
            ticker = str(ticker)
        except:
            continue

        if ticker == 'nan':
            continue

        if type(ticker) == str:

            final_tickers.append(ticker)

            if not default_topic_only:
                producer_dictionary[ticker] = client.create_producer(ticker + default_suffix, schema=default_schema)

        print(count)
        count = count + 1

    return producer_dictionary, final_tickers

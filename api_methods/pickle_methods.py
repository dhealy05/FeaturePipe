def get_producer_dictionary():
    if os.path.isfile(PRODUCER_DICT_PATH):
        with open(PRODUCER_DICT_PATH, 'rb') as input:
            producer_dictionary = pickle.load(input)
    else:
        init_producers()

def init_producers():

    tickers = get_tickers()

    for ticker in tickers:
        if os.path.isfile(PRODUCER_PATH + ticker + ".pkl"):
            with open(PRODUCER_PATH + ticker + ".pkl", 'rb') as input:
                producer_dictionary[ticker] = pickle.load(input)
        else:
            create_producer(ticker)

    with open(PRODUCER_PATH, 'wb') as output:
        pickle.dump(producer_dictionary, output, pickle.HIGHEST_PROTOCOL)

def create_producer(ticker):

    producer = client.create_producer(ticker, schema=AvroSchema(Stock))

    producer_dictionary[ticker] = producer

    with open(PRODUCER_PATH + ticker + ".pkl", 'wb') as output:
        pickle.dump(producer, output, pickle.HIGHEST_PROTOCOL)

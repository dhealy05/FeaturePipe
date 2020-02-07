def init_producers():

    producer_dictionary["all_features"] = client.create_producer("all_features", schema=AvroSchema(Features))

    count = 0

    tickers = get_tickers()

    for ticker in tickers:

        try:
            ticker = str(ticker)
        except:
            continue

        if ticker == 'nan':
            continue

        if type(ticker) == str:
            producer_dictionary[ticker] = client.create_producer(ticker + "_features", schema=AvroSchema(Features))
            final_tickers.append(ticker)

        print(count)
        count = count + 1

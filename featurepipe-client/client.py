import requests
import json

import pandas as pd
import numpy as np

def get_features_as_df(symbol, start_date = None, end_date = None):
    #query_string = 'select * from pulsar."public/default".tsla_features where time > start_date and time < end_date'
    query_string = 'select * from pulsar."public/default".' + symbol.lower() #+ '_features'

    if None != start_date:
        start_date = convert_date_to_milliseconds(start_date)
        query_string = query_string + 'where time > ' + start_date

    if None != start_date and None != end_date:
        end_date = convert_date_to_milliseconds(end_date)
        query_string = query_string + 'and time < ' + end_date

    json_data = send_request(query_string)

    columns = ['symbol', 'avg_1', 'avg_5', 'stddev_1', 'stddev_5', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']
    df = pd.DataFrame(json_data, columns = columns)

    return df

def convert_date_to_milliseconds(date):
    return date.timestamp() * 1000

def send_request(query):
    response = requests.post("http://52.12.4.174:80/query", data = {'query': query})
    json_data = json.loads(response.text)
    return json_data

get_features_as_df('tsla')

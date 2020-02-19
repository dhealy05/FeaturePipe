import requests
import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def get_features_as_df(symbol, start_date = None, end_date = None):
    #query_string = 'select * from pulsar."public/default".tsla_features where time > start_date and time < end_date'
    query_string = 'select * from pulsar."public/default".all_features where symbol = ' + "'" + symbol.lower() + "'"

    if None != start_date:
        start_date = convert_date_to_milliseconds(start_date)
        query_string = query_string + 'and time > ' + start_date

    if None != start_date and None != end_date:
        end_date = convert_date_to_milliseconds(end_date)
        query_string = query_string + 'and time < ' + end_date

    json_data = send_request(query_string)

    avgs = ['avg_1', 'avg_5', 'avg_10', 'avg_15', 'avg_30', 'avg_60', 'avg_120', 'avg_240', 'avg_480', 'avg_960', 'avg_1440']
    devs = ['stddev_1', 'stddev_5', 'stddev_10', 'stddev_15', 'stddev_30', 'stddev_60', 'stddev_120', 'stddev_240', 'stddev_480', 'stddev_960', 'stddev_1440']
    vols = ['vol_1', 'vol_5', 'vol_10', 'vol_15', 'vol_30', 'vol_60', 'vol_120', 'vol_240', 'vol_480', 'vol_960', 'vol_1440']
    sys = ['__partition__', '__event_time__', '__publish_time__', '__message_id__', '__sequence_id__', '__producer_name__', '__key__', '__properties__']

    columns = ['symbol', 'avg_1', 'avg_5', 'avg_10', 'avg_15', 'avg_30', 'avg_60', 'avg_120', 'avg_240', 'avg_480', 'avg_960', 'avg_1440', 'stddev_1', 'stddev_5', 'stddev_10', 'stddev_15', 'stddev_30', 'stddev_60', 'stddev_120', 'stddev_240', 'stddev_480', 'stddev_960', 'stddev_1440', 'vol_1', 'vol_5', 'vol_10', 'vol_15', 'vol_30', 'vol_60', 'vol_120', 'vol_240', 'vol_480', 'vol_960', 'vol_1440', '__partition__', '__event_time__', '__publish_time__', '__message_id__', '__sequence_id__', '__producer_name__', '__key__', '__properties__']

    df = pd.DataFrame(json_data, columns = columns)
    #df = df.sort_values(by=['__publish_time__'])
    df = df.head(25)

    for col in columns:
        indexes = df[ df[col] == 0.0 ].index
        df.drop(indexes , inplace=True)

    df = df.drop(sys, axis=1)

    for dev in devs:
        plt.plot(df[dev])

    plt.show()
    #return df

def convert_date_to_milliseconds(date):
    return date.timestamp() * 1000

def send_request(query):
    response = requests.post("http://52.12.4.174:80/query", data = {'query': query})
    json_data = json.loads(response.text)
    return json_data

get_features_as_df('aapl')

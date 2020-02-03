from pyhive import presto
import time
import datetime

cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()

def get_all_averages():
    seconds = time.time()
    fifteen_minute = (seconds - (60*15))*1000
    average_price('msft_test', fifteen_minute)

def average_price(symbol, boundary):
    query = 'SELECT AVG(price) FROM pulsar."public/default".' + symbol + ' WHERE time > ' + str(boundary)
    cursor.execute(query)
    print cursor.fetchone()
    print cursor.fetchall()

get_all_averages()

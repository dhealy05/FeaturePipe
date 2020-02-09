import pulsar
from pulsar.schema import *
from pyhive import presto
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/query', methods=['POST'])

def query(query):
    request.form.get('query')
    cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()
    cursor.execute(self.query)
    result = cursor.fetchall()
    return result

app.run()

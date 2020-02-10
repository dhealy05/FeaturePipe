import pulsar
from pulsar.schema import *
from pyhive import presto
from flask import Flask, request, jsonify
import json

app = Flask(__name__)

@app.route('/query', methods=['POST'])

def query():
    query_string = request.form.get('query')
    cursor = presto.connect('10.0.0.10', port=8081, username="djh").cursor()
    cursor.execute(query_string)
    result = cursor.fetchall()
    response = json.dumps(result)
    print(response)
    return response

app.run(host="0.0.0.0", port=80)
#app.run()

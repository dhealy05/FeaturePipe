import websocket

try:
    import thread
except ImportError:
    import _thread as thread

import time
import json

API_KEY = "6deAryjhAoa53eNJ5hMZSQb8BOKp64kpuHmYfa"

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):

    def run(*args):

        auth = {"action":"auth","params":API_KEY}
        auth_json = json.dumps(auth)
        ws.send(auth_json)
        #response = ws.recv()
        #print(response)

        subscribe = {"action":"subscribe","params":"T.*"}
        subscribe_json = json.dumps(subscribe)
        ws.send(subscribe_json)
        #response = ws.recv()
        #print(response)

    thread.start_new_thread(run, ())

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://socket.polygon.io/stocks",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()

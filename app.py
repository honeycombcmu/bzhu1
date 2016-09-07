from flask import Flask, render_template, Response

import redis
import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from settings import KEY_SPACE
from settings import DB_URI
from settings import COLUMN_FAMILY


app = Flask(__name__)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)


def event_stream():
    pubsub = r.pubsub()
    pubsub.subscribe('WordCountTopology')
    for message in pubsub.listen():
        print message
        yield 'data: %s\n\n' % message['data']


@app.route('/')
def show_homepage():
  #Word Cloud = cloud.html and app-cloud.js
    return render_template("cloud.html")

@app.route('/basic')
def show_basic():
  #Basic d3 view = basic.html and app.js
    return render_template("basic.html")

@app.route('/stream')
def stream():
    return Response(event_stream(), mimetype="text/event-stream")

def fetch(key):
    pool = ConnectionPool(KEY_SPACE, [DB_URI])
    col_fam = ColumnFamily(pool, COLUMN_FAMILY)
    col_fam.insert('row_key', {'col_name': 'col_val'})
    return col_fam.get(str(key))


if __name__ == '__main__':
    app.run(threaded=True,
    host='0.0.0.0'
)


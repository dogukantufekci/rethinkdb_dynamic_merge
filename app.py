import argparse
import json
import os

from datetime import datetime

from flask import Flask, g, jsonify, render_template, request, abort, Response

import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError, RqlDriverError


class _DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            epoch = (obj.replace(tzinfo=None) - datetime(1970,1,1)).total_seconds() 

            return epoch
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)


RDB_HOST =  os.environ.get('RDB_HOST') or 'localhost'
RDB_PORT = os.environ.get('RDB_PORT') or 28015
CHATAPP_DB = 'chatapp'

## sample data
accounts = json.loads('[{"created_on":{"$reql_type$":"TIME","epoch_time":1392514959.828,"timezone":"+00:00"},"email":"pegora@pegora.com","id":"620a3b4d-e27d-480f-aca7-5641170bd76b","name":"Pegora","password":"123123123"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392514527.28,"timezone":"+00:00"},"email":"bizr@bizr.com","id":"f0f76b0f-64ee-4c5d-a2c3-375fb056dd46","name":"Bizr","password":"123123123"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392515064.823,"timezone":"+00:00"},"email":"halk_piyangosu@halk_piyangosu.com","id":"51247c4b-bb23-4ef9-80f9-db2dc47a0916","name":"Halk Piyangosu","password":"123"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392515093.252,"timezone":"+00:00"},"email":"halk_piyangosu@halk_piyangosu.com","id":"5724728b-247c-41f1-a71b-162e354b4ca1","name":"Halk Piyangosu","password":"123123"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392514547.961,"timezone":"+00:00"},"email":"creco@creco.com","id":"76339cdb-b540-4480-b1a7-66952cc89f73","name":"CRECO","password":"123123123"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392515223.867,"timezone":"+00:00"},"email":"halk_piyangosu@halk_piyangosu.com","id":"5d2261d4-6677-4045-9288-d83472245575","name":"Halk Piyangosu","password":"123123123"}]')
conversations = json.loads('[{"created_on":{"$reql_type$":"TIME","epoch_time":1392609647.149,"timezone":"+00:00"},"id":"fe0278e4-6a61-4484-9be5-c7cc59ed8c13","subject":"Life is awesome.","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","f0f76b0f-64ee-4c5d-a2c3-375fb056dd46","5724728b-247c-41f1-a71b-162e354b4ca1"],"type":"social"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392619268.497,"timezone":"+00:00"},"id":"6fb897cf-4e13-4522-abc7-dcac0b904f7b","subject":"It works","to":["620a3b4d-e27d-480f-aca7-5641170bd76b"],"type":"social"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392610285.843,"timezone":"+00:00"},"id":"8cb95585-c96c-443b-8659-e3b176b58865","subject":"Life is fantastic.","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","5724728b-247c-41f1-a71b-162e354b4ca1"],"type":"social"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392609014.921,"timezone":"+00:00"},"id":"be59113d-8b0d-4f21-8078-fb7cc98e6fa2","subject":"Life is wonderful","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","f0f76b0f-64ee-4c5d-a2c3-375fb056dd46","5724728b-247c-41f1-a71b-162e354b4ca1"]},{"created_on":{"$reql_type$":"TIME","epoch_time":1392608912.066,"timezone":"+00:00"},"id":"c87d5dfc-6164-4f54-8934-ccb26e1156ed","subject":"Hello world","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","f0f76b0f-64ee-4c5d-a2c3-375fb056dd46"]}]')
messages = json.loads('[{"conversation":"fe0278e4-6a61-4484-9be5-c7cc59ed8c13","from":"5724728b-247c-41f1-a71b-162e354b4ca1","id":"a618aafd-09a0-4981-af99-4a6d0428c389","text":"hello world!"},{"conversation":"fe0278e4-6a61-4484-9be5-c7cc59ed8c13","from":"5724728b-247c-41f1-a71b-162e354b4ca1","id":"c7937e51-72a4-4b73-975c-e18bc51b8fa3","text":"hello world!"}]')
message_readers = json.loads('[{"created_on":{"$reql_type$":"TIME","epoch_time":1392609647.149,"timezone":"+00:00"},"id":"fe0278e4-6a61-4484-9be5-c7cc59ed8c13","subject":"Life is awesome.","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","f0f76b0f-64ee-4c5d-a2c3-375fb056dd46","5724728b-247c-41f1-a71b-162e354b4ca1"],"type":"social"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392619268.497,"timezone":"+00:00"},"id":"6fb897cf-4e13-4522-abc7-dcac0b904f7b","subject":"It works","to":["620a3b4d-e27d-480f-aca7-5641170bd76b"],"type":"social"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392610285.843,"timezone":"+00:00"},"id":"8cb95585-c96c-443b-8659-e3b176b58865","subject":"Life is fantastic.","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","5724728b-247c-41f1-a71b-162e354b4ca1"],"type":"social"},{"created_on":{"$reql_type$":"TIME","epoch_time":1392609014.921,"timezone":"+00:00"},"id":"be59113d-8b0d-4f21-8078-fb7cc98e6fa2","subject":"Life is wonderful","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","f0f76b0f-64ee-4c5d-a2c3-375fb056dd46","5724728b-247c-41f1-a71b-162e354b4ca1"]},{"created_on":{"$reql_type$":"TIME","epoch_time":1392608912.066,"timezone":"+00:00"},"id":"c87d5dfc-6164-4f54-8934-ccb26e1156ed","subject":"Hello world","to":["620a3b4d-e27d-480f-aca7-5641170bd76b","f0f76b0f-64ee-4c5d-a2c3-375fb056dd46"]}]')
## /sample data


def dbSetup():
    connection = r.connect(host=RDB_HOST, port=RDB_PORT)
    try:
        r.db_create(CHATAPP_DB).run(connection)
        r.db(CHATAPP_DB).table_create('accounts').run(connection)
        r.db(CHATAPP_DB).table('accounts').insert(accounts).run(connection)
        r.db(CHATAPP_DB).table_create('conversations').run(connection)
        r.db(CHATAPP_DB).table('conversations').insert(conversations).run(connection)
        r.db(CHATAPP_DB).table_create('messages').run(connection)
        r.db(CHATAPP_DB).table('messages').insert(messages).run(connection)
        r.db(CHATAPP_DB).table_create('message_readers').run(connection)
        r.db(CHATAPP_DB).table('message_readers').insert(message_readers).run(connection)
        print 'Database setup completed. Now run the app without --setup.'
    except RqlRuntimeError:
        print 'App database already exists. Run the app without --setup.'
    finally:
        connection.close()


app = Flask(__name__)
app.config.from_object(__name__)

@app.before_request
def before_request():
    try:
        g.db_connection = r.connect(host=RDB_HOST, port=RDB_PORT, db=CHATAPP_DB)
    except RqlDriverError:
        abort(503, "No database connection could be established.")

@app.teardown_request
def teardown_request(exception):
    try:
        g.db_connection.close()
    except AttributeError:
        pass

@app.route("/accounts", methods=['GET'])
def get_accounts():
    selection = r.table("accounts").map(lambda account: 
    account.merge({
        "conversations": r.table("conversations").filter(lambda conversation: 
            conversation["to"].contains(account["id"])).coerce_to("array").map(lambda conversation:
            conversation.merge({
                "to": conversation["to"].map(lambda account: 
                    r.table("accounts").get(account)).coerce_to("array"),
                "messages": r.table("messages").filter(lambda message:
                    message["conversation"] == conversation["id"]).coerce_to("array").map(lambda message:
                    message.merge({
                        "from": r.table("accounts").get(message["from"]),
                        "readers": r.table("message_readers").filter(lambda readers:
                            readers["message"] == message["id"]).coerce_to("array"),
                    }))
            }))
    })).run(g.db_connection)

    selection = list(selection)
    return Response(
            json.dumps(selection, indent=4, cls=_DateEncoder),
            mimetype='application/json',
            status=200,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the Flask todo app')
    parser.add_argument('--setup', dest='run_setup', action='store_true')

    args = parser.parse_args()
    if args.run_setup:
        dbSetup()
    else:
        app.run(debug=True)

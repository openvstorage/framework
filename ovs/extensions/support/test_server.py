# Copyright 2015 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tets server module
"""

import json
from multiprocessing import Lock
from flask import Flask, request, g
app = Flask(__name__)

DATABASE = '/tmp/support_agent.json'
lock = Lock()


def read():
    try:
        lock.acquire()
        contents = '{}'
        try:
            with open(DATABASE, 'r') as database:
                contents = database.read()
        except Exception, ex:
            print ex
        data = json.loads(contents)
        if 'tasks' not in data:
            data['tasks'] = []
        if 'environments' not in data:
            data['environments'] = {}
        return data
    finally:
        lock.release()


def write(data):
    try:
        lock.acquire()
        with open(DATABASE, 'w') as database:
            database.write(json.dumps(data, indent=4))
    finally:
        lock.release()


@app.before_request
def before_request():
    g.data = read()


@app.after_request
def after_request(response):
    write(g.data)
    return response


@app.route('/', methods=['POST'])
def api():
    data = {'tasks': []}
    try:
        rdata = json.loads(request.form['data'])
        print rdata
        gid = rdata['gid']
        nid = rdata['nid']
        if gid not in g.data['environments']:
            g.data['environments'][gid] = {}
        if nid not in g.data['environments'][gid]:
            g.data['environments'][gid][nid] = {}
        g.data['environments'][gid][nid] = {}
        if 'services' in rdata['metadata']:
            g.data['environments'][gid][nid]['services'] = rdata['metadata']['services']
        if 'versions' in rdata['metadata']:
            g.data['environments'][gid][nid]['versions'] = rdata['metadata']['versions']

        new_tasks = []
        for task in g.data['tasks']:
            if task['executed']:
                new_tasks.append(task)
            if task['task'] == 'OPEN_TUNNEL':
                data['tasks'].append({'task': 'OPEN_TUNNEL',
                                      'metadata': {'user': 'root',
                                                   'endpoint': '10.100.169.101',
                                                   'port': 23}})
            if task['task'] == 'CLOSE_TUNNEL':
                data['tasks'].append({'task': 'CLOSE_TUNNEL',
                                      'metadata': {'user': 'root',
                                                   'endpoint': '10.100.169.101',
                                                   'port': 23}})
        g.data['tasks'] = new_tasks
    except Exception, ex:
        print ex
    return json.dumps(data)


@app.route('/list')
def list_data():
    data = []
    for task in g.data['tasks']:
        data.append(task)
    return json.dumps(data)


@app.route('/envs')
def list_envs():
    return json.dumps(g.data['environments'])


@app.route('/add/<task>')
def add(task):
    if task == 'OPEN_TUNNEL':
        g.data['tasks'].append({'task': 'OPEN_TUNNEL',
                                'executed': False})
        return json.dumps({'success': True})
    if task == 'CLOSE_TUNNEL':
        g.data['tasks'].append({'task': 'CLOSE_TUNNEL',
                                'executed': False})
        return json.dumps({'success': True})
    return json.dumps({'success': False,
                       'error': 'Unknown task'})

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')

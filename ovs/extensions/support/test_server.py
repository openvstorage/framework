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
            data['tasks'] = {}
        if 'envs' not in data:
            data['envs'] = {}
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

# The API endpoint to which agents connect


@app.route('/api', methods=['POST'])
def api():
    data = {'tasks': []}
    try:
        rdata = json.loads(request.form['data'])
        cid = rdata['cid']
        nid = rdata['nid']
        if cid not in g.data['envs']:
            g.data['envs'][cid] = {}
        if nid not in g.data['envs'][cid]:
            g.data['envs'][cid][nid] = {}
        g.data['envs'][cid][nid] = {}
        if 'services' in rdata['metadata']:
            g.data['envs'][cid][nid]['services'] = rdata['metadata']['services']
        if 'versions' in rdata['metadata']:
            g.data['envs'][cid][nid]['versions'] = rdata['metadata']['versions']

        if cid in g.data['tasks'] and nid in g.data['tasks'][cid]:
            for task in g.data['tasks'][cid][nid]:
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
            g.data['tasks'][cid][nid] = []
    except Exception, ex:
        print ex
    return json.dumps(data, indent=4)

# The "monitoring portal" interface


@app.route('/', methods=['GET'])
def root():
    return """
<html>
    <head><title>Monitoring portal</title></head>
    <script src="https://code.jquery.com/jquery-2.1.3.min.js"></script>
    <script>
        function print(data) {
            $("#pre").text(data);
        }
        function sh_nid() {
            if ($("#cid").val() == "") {
                $("#nid_field").hide();
            } else {
                $("#nid_field").show();
            }
        }
        function get_cid_nid() {
            var cid = $("#cid").val(),
                nid = $("#nid").val();
            return (cid == "" ? "" : ("/" + cid + (nid == "" ? "" : ("/" + nid))));
        }
        function list() {
            var url = "/list" + get_cid_nid();
            $.get(url)
             .done(print);
        }
        function info() {
            var url = "/envs" + get_cid_nid();
            $.get(url)
             .done(print);
        }
        function task(task_name) {
            var url = "/add" + get_cid_nid() + "/" + task_name;
            $.get(url)
             .done(print);
        }
        $(document).ready(function() {
            $("#cid").on("change keydown keyup blur focus", sh_nid);
        });
    </script>
    <body>
        <h1>Monitoring portal</h1>
        <p>
            Execute action on CID <input id="cid" style="width: 400px;"/> (empty for all)
            <span id='nid_field' style='display: none;'>, NID: <input id="nid" style="width: 400px;"/> (empty for all)</span>
            <ul>
                <li><a href="#" onclick="list();">List all pending tasks</a></li>
                <li><a href="#" onclick="info();">Show information</a></li>
                <li>
                    Add tasks
                    <ul>
                        <li><a href="#" onclick="task('OPEN_TUNNEL');">Open tunnel</a></li>
                        <li><a href="#" onclick="task('CLOSE_TUNNEL');">Close tunnel</a></li>
                    </ul>
                </li>
            </ul>
        </p>
        <h1>Data</h1>
        <pre id='pre'>
        </pre>
    </body>
</html>
"""


@app.route('/list')
@app.route('/list/<cid>')
@app.route('/list/<cid>/<nid>')
def list_data(cid=None, nid=None):
    if cid is not None and cid in g.data['tasks']:
        if nid is not None and nid in g.data['tasks'][cid]:
            return json.dumps(g.data['tasks'][cid][nid], indent=4)
        return json.dumps(g.data['tasks'][cid], indent=4)
    return json.dumps(g.data['tasks'], indent=4)


@app.route('/envs')
@app.route('/envs/<cid>')
@app.route('/envs/<cid>/<nid>')
def list_envs(cid=None, nid=None):
    if cid is not None:
        if nid is not None:
            return json.dumps(g.data['envs'][cid][nid], indent=4)
        return json.dumps(g.data['envs'][cid], indent=4)
    return json.dumps(g.data['envs'], indent=4)


@app.route('/add/<task>')
@app.route('/add/<cid>/<task>')
@app.route('/add/<cid>/<nid>/<task>')
def add(task, cid=None, nid=None):
    cids = [cid] if cid is not None else g.data['envs'].keys()
    for cid in cids:
        nids = [nid] if nid is not None and nid in g.data['envs'][cid] else g.data['envs'][cid].keys()
        for nid in nids:
            if cid not in g.data['tasks']:
                g.data['tasks'][cid] = {}
            if nid not in g.data['tasks'][cid]:
                g.data['tasks'][cid][nid] = []
            if task == 'OPEN_TUNNEL':
                g.data['tasks'][cid][nid].append({'task': 'OPEN_TUNNEL'})
            if task == 'CLOSE_TUNNEL':
                g.data['tasks'][cid][nid].append({'task': 'CLOSE_TUNNEL'})
    return json.dumps({'success': True}, indent=4)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')

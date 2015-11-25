#!/usr/bin/env python2
# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import random
import re
import string
import sys
import json
from subprocess import check_output, CalledProcessError

dist_info = check_output('cat /etc/os-release', shell=True)
if 'CentOS Linux' in dist_info:
    openstack_webservice_name = 'httpd'
else:  # Default fallback to Ubuntu in this case
    openstack_webservice_name = 'apache2'

# Cleanup *.pyc files
# TODO: set owner:group only where it is really needed
check_output('chown -R ovs:ovs /opt/OpenvStorage', shell=True)
check_output('find /opt/OpenvStorage -name *.pyc -exec rm -rf {} \;', shell=True)

SECRET_KEY_LENGTH = 50
SECRET_SELECTION = "{0}{1}{2}".format(string.ascii_letters, string.digits, string.punctuation)
secret_key = ''.join([random.SystemRandom().choice(SECRET_SELECTION) for i in range(SECRET_KEY_LENGTH)])

config_filename = '/opt/OpenvStorage/config/ovs.json'
with open(config_filename, 'r') as config_file:
    contents = json.loads(config_file.read())
contents['webapps']['main']['secret'] = secret_key
with open(config_filename, 'w') as config_file:
    config_file.write(json.dumps(contents, indent=4))

os.chdir('/opt/OpenvStorage/webapps/api')
check_output('export PYTHONPATH=/opt/OpenvStorage:$PYTHONPATH; python manage.py syncdb --noinput', shell=True)

run_level_regex = '^[KS][0-9]{2}(.*)'

# Create web certificates
if not os.path.exists('/opt/OpenvStorage/config/ssl/server.crt'):
    check_output('mkdir -p /opt/OpenvStorage/config/ssl', shell=True)
    os.chdir('/opt/OpenvStorage/config/ssl')
    check_output('echo `openssl rand -base64 32` >> passphrase', shell=True)
    check_output('openssl genrsa -des3 -out server.key -passout file:passphrase', shell=True)
    check_output('openssl req -new -key server.key -out server.csr -passin file:passphrase -batch', shell=True)
    check_output('cp server.key server.key.org', shell=True)
    check_output('openssl rsa -passin file:passphrase -in server.key.org -out server.key', shell=True)
    check_output('openssl x509 -req -days 356 -in server.csr -signkey server.key -out server.crt', shell=True)
    check_output('rm -f server.key.org', shell=True)

# Versioning
version = sys.argv[1]
filenames = ['/opt/OpenvStorage/webapps/frontend/app/main.js', '/opt/OpenvStorage/webapps/frontend/offline/ovs.appcache']
for filename in filenames:
    if os.path.exists(filename):
        with open(filename, 'r') as original:
            contents = original.read()
        contents = re.sub("'version=[^']+?'", "'version={0}'".format(version), contents)
        with open(filename, 'w') as changed:
            changed.write(contents)

# Disable default nginx site if it's configured
if os.path.exists('/etc/nginx/sites-enabled/default'):
    os.remove('/etc/nginx/sites-enabled/default')

# Setup nginx site
if not os.path.exists("/etc/nginx/sites-enabled/openvstorage.conf") and os.path.exists('/etc/nginx/sites-available/openvstorage.conf'):
    check_output("ln -s /etc/nginx/sites-available/openvstorage.conf /etc/nginx/sites-enabled/openvstorage.conf", shell=True)
if not os.path.exists("/etc/nginx/sites-enabled/openvstorage_ssl.conf") and os.path.exists('/etc/nginx/sites-available/openvstorage_ssl.conf'):
    check_output("ln -s /etc/nginx/sites-available/openvstorage_ssl.conf /etc/nginx/sites-enabled/openvstorage_ssl.conf", shell=True)

# Check conflicts with apache2 running on port 80 (most likely devstack/openstack gui)
try:
    running = check_output('ps aux | grep {0} | grep -v grep'.format(openstack_webservice_name), shell=True)
except CalledProcessError:
    running = False
if running:
    if os.path.exists('/etc/nginx/sites-enabled/openvstorage.conf'):
        os.remove('/etc/nginx/sites-enabled/openvstorage.conf')
    if os.path.exists('/etc/nginx/conf.d/openvstorage.conf'):
        os.remove('/etc/nginx/conf.d/openvstorage.conf')

# Restart Nginx
check_output("service nginx restart", shell=True)

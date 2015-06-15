#!/usr/bin/env python2
# Copyright 2014 CloudFounders NV
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

import os
import random
import re
import string
import sys
from ConfigParser import RawConfigParser
from subprocess import check_output, CalledProcessError

# Remove existing enabled sites, taking control over nginx
check_output('rm -f /etc/nginx/sites-enabled/default', shell=True)

# Cleanup *.pyc files
check_output('chown -R ovs:ovs /opt/OpenvStorage', shell=True)
check_output('find /opt/OpenvStorage -name *.pyc -exec rm -rf {} \;', shell=True)

SECRET_KEY_LENGTH = 50
SECRET_SELECTION = "{0}{1}{2}".format(string.ascii_letters, string.digits, string.punctuation)
secret_key = ''.join([random.SystemRandom().choice(SECRET_SELECTION) for i in range(SECRET_KEY_LENGTH)])

config_filename = '/opt/OpenvStorage/config/ovs.cfg'
config = RawConfigParser()
config.read(config_filename)
if config.get('webapps', 'main.secret') != 'pl4c3h0ld3r':
    config.set('webapps', 'main.secret', secret_key)
    with open(config_filename, 'wb') as config_file:
        config.write(config_file)

os.chdir('/opt/OpenvStorage/webapps/api')
check_output('export PYTHONPATH=/opt/OpenvStorage; python manage.py syncdb --noinput', shell=True)

run_level_regex = '^[KS][0-9]{2}(.*)'
service_configured = True
for run_level in range(7):
    if 'nginx' not in [re.match(run_level_regex, run_entry).groups()[0] for run_entry in os.listdir('/etc/rc{0}.d'.format(run_level)) if re.match(run_level_regex, run_entry)]:
        service_configured &= False
if service_configured is False:
    check_output('service nginx stop', shell=True)
    check_output('update-rc.d nginx disable', shell=True)

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

# Check conflicts with apache2 running on port 80 (most likely devstack/openstack gui)
try:
    running = check_output('ps aux | grep apache2 | grep -v grep', shell=True)
except CalledProcessError:
    running = False
if running:
    if os.path.exists('/etc/nginx/sites-enabled/openvstorage.conf'):
        os.remove('/etc/nginx/sites-enabled/openvstorage.conf')

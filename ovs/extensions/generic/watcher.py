#!/usr/bin/python2
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

import sys
import time
import uuid
import os
import logging
from ovs.log.logHandler import LogHandler

logging.basicConfig()
logger = LogHandler('extensions', name='watcher')


def _log(target, entry):
    """
    Logs an entry
    """
    logger.debug('[{0}] {1}'.format(target, entry))


def services_running(target):
    key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))
    value = str(time.time())

    if target == 'framework':
        # Volatile
        _log(target, 'Testing volatile store...')
        max_tries = 5
        tries = 0
        while tries < max_tries:
            try:
                from ovs.extensions.storage.volatilefactory import VolatileFactory
                volatile = VolatileFactory.get_client()
                volatile.set(key, value)
                if volatile.get(key) == value:
                    volatile.delete(key)
                    break
                volatile.delete(key)
            except Exception as message:
                _log(target, '  Error during volatile store test: {0}'.format(message))
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))  # Get another key
            time.sleep(1)
            tries += 1
        if tries == max_tries:
            _log(target, '  Volatile store not working correctly')
            return False
        _log(target, '  Volatile store OK after {0} tries'.format(tries))

        # Persistent
        _log(target, 'Testing persistent store...')
        max_tries = 5
        tries = 0
        while tries < max_tries:
            try:
                from ovs.extensions.storage.persistentfactory import PersistentFactory
                persistent = PersistentFactory.get_client()
                persistent.set(key, value)
                if persistent.get(key) == value:
                    persistent.delete(key)
                    break
                persistent.delete(key)
            except Exception as message:
                _log(target, '  Error during persistent store test: {0}'.format(message))
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))  # Get another key
            time.sleep(1)
            tries += 1
        if tries == max_tries:
            _log(target, '  Persistent store not working correctly')
            return False
        _log(target, '  Persistent store OK after {0} tries'.format(tries))

    if target == 'volumedriver':
        # Arakoon, voldrv cluster
        _log(target, 'Testing arakoon (voldrv)...')
        max_tries = 5
        tries = 0
        while tries < max_tries:
            try:
                from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
                cluster = ArakoonManagement().getCluster('voldrv')
                client = cluster.getClient()
                client.set(key, value)
                if client.get(key) == value:
                    client.delete(key)
                    break
                client.delete(key)
            except Exception as message:
                _log(target, '  Error during arakoon (voldrv) test: {0}'.format(message))
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))  # Get another key
            time.sleep(1)
            tries += 1
        if tries == max_tries:
            _log(target, '  Arakoon (voldrv) not working correctly')
            return False
        _log(target, '  Arakoon (voldrv) OK')

    if target in ['framework', 'volumedriver']:
        # RabbitMQ
        _log(target, 'Test rabbitMQ...')
        import pika
        from configobj import ConfigObj
        from ovs.plugin.provider.configuration import Configuration
        rmq_ini = ConfigObj(os.path.join(Configuration.get('ovs.core.cfgdir'), 'rabbitmqclient.cfg'))
        rmq_nodes = rmq_ini.get('main')['nodes'] if type(rmq_ini.get('main')['nodes']) == list else [rmq_ini.get('main')['nodes']]
        rmq_servers = map(lambda m: rmq_ini.get(m)['location'], rmq_nodes)
        good_node = False
        for server in rmq_servers:
            try:
                connection_string = '{0}://{1}:{2}@{3}/%2F'.format(Configuration.get('ovs.core.broker.protocol'),
                                                                   Configuration.get('ovs.core.broker.login'),
                                                                   Configuration.get('ovs.core.broker.password'),
                                                                   server)
                connection = pika.BlockingConnection(pika.URLParameters(connection_string))
                channel = connection.channel()
                channel.basic_publish('', 'ovs-watcher', str(time.time()),
                                      pika.BasicProperties(content_type='text/plain', delivery_mode=1))
                connection.close()
                good_node = True
                break
            except Exception as message:
                _log(target, '  Error during rabbitMQ test on node {0}: {1}'.format(server, message))
        if good_node is False:
            _log(target, '  No working rabbitMQ node could be found')
            return False
        _log(target, '  RabbitMQ test OK')
        _log(target, 'All tests OK')
        return True


if __name__ == '__main__':
    target = sys.argv[1]
    mode = sys.argv[2]
    _log(target, 'Starting service')
    if mode == 'wait':
        _log(target, 'Waiting for master services')
        while True:
            if services_running(target):
                _log(target, 'Master services available')
                sys.exit(0)
            time.sleep(5)
    if mode == 'check':
        _log(target, 'Checking master services')
        while True:
            if not services_running(target):
                _log(target, 'One of the master services is unavailable')
                sys.exit(1)
            time.sleep(5)
    _log(target, 'Invalid parameter')
    time.sleep(60)
    sys.exit(1)

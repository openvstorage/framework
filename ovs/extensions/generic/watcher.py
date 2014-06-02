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


def services_running():
    key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))
    value = str(time.time())

    # 1. Volatile
    print 'Testing volatile store...'
    try:
        from ovs.extensions.storage.volatilefactory import VolatileFactory
        volatile = VolatileFactory.get_client()
        volatile.set(key, value)
        if volatile.get(key) != value:
            print '  Volatile store not working correctly'
            return False
        print '  Volatile store OK'
    except Exception as message:
        print '  Error during volatile store test: {0}'.format(message)
        return False
    # 2. Persistent
    print 'Testing persistent store...'
    try:
        from ovs.extensions.storage.persistentfactory import PersistentFactory
        persistent = PersistentFactory.get_client()
        persistent.set(key, value)
        if persistent.get(key) != value:
            print '  Persistent store not working correctly'
            return False
        print '  Persistent store OK'
    except Exception as message:
        print '  Error during persistent store test: {0}'.format(message)
        return False
    # 3. Arakoon, voldrv cluster
    print 'Testing arakoon (voldrv)...'
    try:
        from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
        cluster = ArakoonManagement().getCluster('voldrv')
        client = cluster.getClient()
        client.set(key, value)
        if client.get(key) != value:
            print '  Arakoon (voldrv) not working correctly'
            return False
        print '  Arakoon (voldrv) OK'
    except Exception as message:
        print '  Error during arakoon (voldrv) test: {0}'.format(message)
        return False
    # 4. RabbitMQ
    print 'Test rabbitMQ...'
    good_node = False
    try:
        import pika
        from configobj import ConfigObj
        from ovs.plugin.provider.configuration import Configuration
        rmq_ini = ConfigObj(os.path.join(Configuration.get('ovs.core.cfgdir'), 'rabbitmqclient.cfg'))
        rmq_nodes = rmq_ini.get('main')['nodes'] if type(rmq_ini.get('main')['nodes']) == list else [rmq_ini.get('main')['nodes']]
        rmq_servers = map(lambda m: rmq_ini.get(m)['location'], rmq_nodes)
        for server in rmq_servers:
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
        print '  RabbitMQ test OK'
    except Exception as message:
        if good_node is False:
            print '  Error during rabbitMQ test: {0}'.format(message)
            return False
    print 'All tests OK'
    return True


if __name__ == '__main__':
    print 'Starting service'
    if sys.argv[1] == 'wait':
        print 'Waiting for master services'
        while True:
            if services_running():
                print 'Master services available'
                sys.exit(0)
            time.sleep(5)
    if sys.argv[1] == 'check':
        print 'Checking master services'
        while True:
            if not services_running():
                print 'One of the master services is unavailable'
                sys.exit(1)
            time.sleep(5)
    print 'Invalid parameter'
    time.sleep(60)
    sys.exit(1)

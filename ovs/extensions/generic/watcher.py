#!/usr/bin/env python2
# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Watcher module for framework and volumedriver
"""

import os
import sys
import time
import pika
import uuid
import argparse
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import NoGuarantee
from ovs.extensions.db.arakooninstaller import ArakoonInstaller, ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.storage.persistent.pyrakoonstore import PyrakoonStore
from ovs.extensions.storage.persistentfactory import PersistentFactory


class WatcherTypes(object):
    """
    Defines allowed types of the watcher target
    """
    FWK = 'framework'
    CONFIG = 'config'
    VOLDRV = 'volumedriver'

    @staticmethod
    def list():
        # type: () -> List[str]
        """
        Lists all allowed targets
        :return
        """
        return [WatcherTypes.FWK, WatcherTypes.CONFIG, WatcherTypes.VOLDRV]


class Watcher(object):
    """
    Watcher class
    """
    LOG_CONTENTS = None

    def __init__(self, target, mode, level=0):
        # type: () -> None
        self._logger = Logger('extensions-generic')
        self.target = target
        self.mode = mode
        self.level = level  # Minimal level to log

    def log_message(self, entry, level=None, show_level=0):
        # type: (str, int, int) -> None
        """
        Logs an entry if above threshold level
        """
        level = level or self.level  # Picks self.level if not overriden per call
        if level >= show_level:  # 0 = debug, 1 = info, 2 = error
            self._logger.debug('[{0}] {1}'.format(self.target, entry))

    def _test_store(self, store_type, key=None, value=None):
        # type: (str, str, str) -> bool
        """
        Test specified store type
        :param store_type: name of the store type
        :type: str
        :param key: key content to test
        :type key: str
        :param value: value to put
        :type value: str
        :return: boolean
        """
        # Volatile
        self.log_message('Testing {0} store...'.format(store_type))
        max_tries = 5
        tries = 0
        while tries < max_tries:
            if store_type == 'arakoon_voldrv':
                try:
                    cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
                    configuration = Configuration.get('/ovs/arakoon/{0}/config'.format(cluster_name), raw=True)
                    client = PyrakoonStore(cluster=cluster_name, configuration=configuration)
                    client.nop()
                    break
                except Exception as message:
                    self.log_message('  Error during arakoon (voldrv) test: {0}'.format(message), 2)
            else:
                try:
                    if store_type == 'volatile':
                        from ovs.extensions.storage.volatilefactory import VolatileFactory
                        VolatileFactory.store = None
                        volatile = VolatileFactory.get_client()
                        volatile.set(key, value)
                        if volatile.get(key) == value:
                            volatile.delete(key)
                            break
                        volatile.delete(key)
                    elif store_type == 'persistent':
                        persistent = PersistentFactory.get_client()
                        persistent.nop()
                        break
                except Exception as message:
                    self.log_message('  Error during {0} store test: {1}'.format(store_type, message), 3)
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))  # Get another key
            time.sleep(1)
            tries += 1
        if tries == max_tries:
            self.log_message('  {0} store not working correctly'.format(store_type), 2)
            return False
        self.log_message('  {0} store OK after {1} tries'.format(store_type, tries))

    def services_running(self):
        # type: () -> bool
        """
        Check if all services are running
        :return: Boolean
        """
        try:
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))
            value = str(time.time())
            if self.target in [WatcherTypes.CONFIG, WatcherTypes.FWK]:
                self.log_message('Testing configuration store...')
                try:
                    Configuration.list('/')
                except Exception as ex:
                    self.log_message('  Error during configuration store test: {0}'.format(ex), 2)
                    return False

                with open(Configuration.CACC_LOCATION) as config_file:
                    contents = config_file.read()
                config = ArakoonClusterConfig(cluster_id=Configuration.ARAKOON_NAME, load_config=False)
                config.read_config(contents=contents)
                client = ArakoonInstaller.build_client(config)
                contents = client.get(ArakoonInstaller.INTERNAL_CONFIG_KEY, consistency=NoGuarantee())
                if Watcher.LOG_CONTENTS != contents:
                    try:
                        config.read_config(contents=contents)  # Validate whether the contents are not corrupt
                    except Exception as ex:
                        self.log_message('  Configuration stored in configuration store seems to be corrupt: {0}'.format(ex), 2)
                        return False
                    temp_filename = '{0}~'.format(Configuration.CACC_LOCATION)
                    with open(temp_filename, 'w') as config_file:
                        config_file.write(contents)
                        config_file.flush()
                        os.fsync(config_file)
                    os.rename(temp_filename, Configuration.CACC_LOCATION)
                    Watcher.LOG_CONTENTS = contents
                self.log_message('  Configuration store OK', 0)

            if self.target == WatcherTypes.FWK:
                self._test_store('volatile', key, value)
                self._test_store('persistent')

            if self.target == WatcherTypes.VOLDRV:
                # Arakoon, voldrv cluster
                self._test_store('arakoon_voldrv')

            if self.target in [WatcherTypes.FWK, WatcherTypes.VOLDRV]:
                # RabbitMQ
                self.log_message('Test rabbitMQ...', 0)
                messagequeue = Configuration.get('/ovs/framework/messagequeue')
                rmq_servers = messagequeue['endpoints']
                good_node = False
                for server in rmq_servers:
                    try:
                        connection_string = '{0}://{1}:{2}@{3}/%2F'.format(messagequeue['protocol'],
                                                                           messagequeue['user'],
                                                                           messagequeue['password'],
                                                                           server)
                        connection = pika.BlockingConnection(pika.URLParameters(connection_string))
                        channel = connection.channel()
                        channel.basic_publish('', 'ovs-watcher', str(time.time()),
                                              pika.BasicProperties(content_type='text/plain', delivery_mode=1))
                        connection.close()
                        good_node = True
                    except Exception as message:
                        self.log_message('  Error during rabbitMQ test on node {0}: {1}'.format(server, message), 2)
                if good_node is False:
                    self.log_message('  No working rabbitMQ node could be found', 2)
                    return False
                self.log_message('  RabbitMQ test OK')
                self.log_message('All tests OK')

            return True
        except Exception as ex:
            self.log_message('Unexpected exception: {0}'.format(ex), 2)
            return False

    def start(self):
        # type: () -> None
        """
        Start the ovs framework watcher
        """
        if self.mode == 'wait':
            watcher.log_message('Waiting for master services', 1)
            while True:
                if watcher.services_running():
                    watcher.log_message('Master services available', 1)
                    sys.exit(0)
                time.sleep(5)

        if self.mode == 'check':
            watcher.log_message('Checking master services', 1)
            while True:
                if not watcher.services_running():
                    watcher.log_message('One of the master services is unavailable', 1)
                    sys.exit(1)
                time.sleep(5)

        watcher.log_message('Invalid parameter', 1)
        time.sleep(60)
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='framework-watcher', description='Framework watcher service')
    subparsers = parser.add_subparsers(dest='given_target', help='Possible options for the framework watcher manager service')

    parser_setup = subparsers.add_parser(name='framework', help='Run framework related watcher service')
    parser_setup.add_argument('mode', help="", choices=['wait', 'check'])

    arguments = parser.parse_args()

    watcher = Watcher(target=arguments.given_target, mode=arguments.mode)
    watcher.start()

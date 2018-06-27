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
import uuid
import logging
import argparse
from ovs.extensions.generic.logger import Logger
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

    def __init__(self):
        # type: () -> None
        """
        Dummy init method
        """
        self._logger = Logger('extensions-generic')

    def log_message(self, log_target, entry, level):
        # type: (str, str, int) -> None
        """
        Logs an entry
        """
        if level > 0:  # 0 = debug, 1 = info, 2 = error
            self._logger.debug('[{0}] {1}'.format(log_target, entry))

    def _test_store(self, store_type, target, key=None, value=None):
        # Volatile
        self.log_message(target, 'Testing {0} store...'.format(store_type), 0)
        max_tries = 5
        tries = 0
        while tries < max_tries:
            if store_type == 'arakoon_voldrv':
                try:
                    from ovs.extensions.generic.configuration import Configuration
                    from ovs_extensions.storage.persistent.pyrakoonstore import PyrakoonStore
                    cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
                    configuration = Configuration.get('/ovs/arakoon/{0}/config'.format(cluster_name), raw=True)
                    client = PyrakoonStore(cluster=cluster_name, configuration=configuration)
                    client.nop()
                    break
                except Exception as message:
                    self.log_message(target, '  Error during arakoon (voldrv) test: {0}'.format(message), 2)
            else:
                try:
                    try:
                        logging.disable(logging.WARNING)
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
                    finally:
                        logging.disable(logging.NOTSET)
                except Exception as message:
                    self.log_message(target, '  Error during {0} store test: {1}'.format(store_type, message), 2)
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))  # Get another key
            time.sleep(1)
            tries += 1
        if tries == max_tries:
            self.log_message(target, '  {0} store not working correctly'.format(store_type), 2)
            return False
        self.log_message(target, '  {0} store OK after {1} tries'.format(store_type, tries), 0)

    def services_running(self, target):
        """
        Check all services are running
        :param target: Target to check
        :return: Boolean
        """
        try:
            key = 'ovs-watcher-{0}'.format(str(uuid.uuid4()))
            value = str(time.time())
            if target not in WatcherTypes.list():
                self.log_message(target, 'Target not found in allowed ', 2)

            if target in [WatcherTypes.CONFIG, WatcherTypes.FWK]:
                self.log_message(target, 'Testing configuration store...', 0)
                from ovs.extensions.generic.configuration import Configuration
                try:
                    Configuration.list('/')
                except Exception as ex:
                    self.log_message(target, '  Error during configuration store test: {0}'.format(ex), 2)
                    return False

                from ovs.extensions.db.arakooninstaller import ArakoonInstaller, ArakoonClusterConfig
                from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import NoGuarantee

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
                        self.log_message(target, '  Configuration stored in configuration store seems to be corrupt: {0}'.format(ex), 2)
                        return False
                    temp_filename = '{0}~'.format(Configuration.CACC_LOCATION)
                    with open(temp_filename, 'w') as config_file:
                        config_file.write(contents)
                        config_file.flush()
                        os.fsync(config_file)
                    os.rename(temp_filename, Configuration.CACC_LOCATION)
                    Watcher.LOG_CONTENTS = contents
                self.log_message(target, '  Configuration store OK', 0)

            if target == WatcherTypes.FWK:
                self._test_store('volatile', target, key, value)
                self._test_store('persistent', target)

            if target == WatcherTypes.VOLDRV:
                # Arakoon, voldrv cluster
                self._test_store('arakoon_voldrv', target)

            if target in [WatcherTypes.FWK, WatcherTypes.VOLDRV]:
                # RabbitMQ
                self.log_message(target, 'Test rabbitMQ...', 0)
                import pika
                from ovs.extensions.generic.configuration import Configuration
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
                        self.log_message(target, '  Error during rabbitMQ test on node {0}: {1}'.format(server, message), 2)
                if good_node is False:
                    self.log_message(target, '  No working rabbitMQ node could be found', 2)
                    return False
                self.log_message(target, '  RabbitMQ test OK', 0)
                self.log_message(target, 'All tests OK', 0)

            return True
        except Exception as ex:
            self.log_message(target, 'Unexpected exception: {0}'.format(ex), 2)
            return False


if __name__ == '__main__':
    watcher = Watcher()

    parser = argparse.ArgumentParser(prog='framework-watcher', description='Framework watcher service')
    subparsers = parser.add_subparsers(dest='given_target', help='Possible options for the framework watcher manager service')

    parser_setup = subparsers.add_parser(name='framework', help='Run framework related watcher service')
    parser_setup.add_argument('mode', help="", choices=['wait', 'check', 'stop_pre', 'start_post'], type=str)

    arguments = parser.parse_args()

    given_target = arguments.given_target
    mode = arguments.mode
    if mode == 'wait':
        watcher.log_message(given_target, 'Waiting for master services', 1)
        while True:
            if watcher.services_running(given_target):
                watcher.log_message(given_target, 'Master services available', 1)
                sys.exit(0)
            time.sleep(5)
    if mode == 'check':
        watcher.log_message(given_target, 'Checking master services', 1)
        while True:
            if not watcher.services_running(given_target):
                watcher.log_message(given_target, 'One of the master services is unavailable', 1)
                sys.exit(1)
            time.sleep(5)
    watcher.log_message(given_target, 'Invalid parameter', 1)
    time.sleep(60)
    sys.exit(1)

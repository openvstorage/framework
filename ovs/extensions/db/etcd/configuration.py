# Copyright 2015 iNuron NV
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

"""
Generic module for managing configuration in Etcd
"""

import json
import time
import etcd
import random
import string
import logging
from ovs.log.logHandler import LogHandler

logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logger = LogHandler.get('extensions', name='etcdconfiguration')


def log_slow_calls(f):
    """
    Wrapper to print duration when call takes > 1s
    :param f: Function to wrap
    :return: Wrapped function
    """
    def new_function(*args, **kwargs):
        """
        Execute function
        :return: Function output
        """
        start = time.time()
        try:
            return f(*args, **kwargs)
        finally:
            key_info = ''
            if 'key' in kwargs:
                key_info = ' (key: {0})'.format(kwargs['key'])
            elif len(args) > 0:
                key_info = ' (key: {0})'.format(args[0])
            duration = time.time() - start
            if duration > 1:
                logger.warning('Call to {0}{1} took {2}s'.format(f.__name__, key_info, duration))
    new_function.__name__ = f.__name__
    new_function.__module__ = f.__module__
    return new_function


class EtcdConfiguration(object):
    """
    Configuration class using Etcd.

    Uses a special key format to specify the path within etcd, and specify a path inside the json data
    object that might be stored inside the etcd key.
    key  = <etcd path>[|<json path>]
    etcd path = slash-delimited path
    json path = dot-delimited path

    Examples:
        > EtcdConfiguration.set('/foo', 1)
        > print EtcdConfiguration.get('/foo')
        < 1
        > EtcdConfiguration.set('/foo', {'bar': 1})
        > print EtcdConfiguration.get('/foo')
        < {u'bar': 1}
        > print EtcdConfiguration.get('/foo|bar')
        < 1
        > EtcdConfiguration.set('/bar|a.b', 'test')
        > print EtcdConfiguration.get('/bar')
        < {u'a': {u'b': u'test'}}
    """

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get(key, raw=False):
        """
        Get value from etcd
        :param key: Key to get
        :param raw: Raw data if True else json format
        :return: Value for key
        """
        key_entries = key.split('|')
        data = EtcdConfiguration._get(key_entries[0], raw)
        if len(key_entries) == 1:
            return data
        temp_data = data
        for entry in key_entries[1].split('.'):
            temp_data = temp_data[entry]
        return temp_data

    @staticmethod
    def set(key, value, raw=False):
        """
        Set value in etcd
        :param key: Key to store
        :param value: Value to store
        :param raw: Raw data if True else json format
        :return: None
        """
        key_entries = key.split('|')
        if len(key_entries) == 1:
            EtcdConfiguration._set(key_entries[0], value, raw)
            return
        try:
            data = EtcdConfiguration._get(key_entries[0], raw)
        except etcd.EtcdKeyNotFound:
            data = {}
        temp_config = data
        entries = key_entries[1].split('.')
        for entry in entries[:-1]:
            if entry in temp_config:
                temp_config = temp_config[entry]
            else:
                temp_config[entry] = {}
                temp_config = temp_config[entry]
        temp_config[entries[-1]] = value
        EtcdConfiguration._set(key_entries[0], data, raw)

    @staticmethod
    def delete(key, remove_root=False, raw=False):
        """
        Delete key - value from etcd
        :param key: Key to delete
        :param remove_root: Remove root
        :param raw: Raw data if True else json format
        :return: None
        """
        key_entries = key.split('|')
        if len(key_entries) == 1:
            EtcdConfiguration._delete(key_entries[0], recursive=True)
            return
        data = EtcdConfiguration._get(key_entries[0], raw)
        temp_config = data
        entries = key_entries[1].split('.')
        if len(entries) > 1:
            for entry in entries[:-1]:
                if entry in temp_config:
                    temp_config = temp_config[entry]
                else:
                    temp_config[entry] = {}
                    temp_config = temp_config[entry]
            del temp_config[entries[-1]]
        if len(entries) == 1 and remove_root is True:
            del data[entries[0]]
        EtcdConfiguration._set(key_entries[0], data, raw)

    @staticmethod
    def exists(key, raw=False):
        """
        Check if key exists in etcd
        :param key: Key to check
        :param raw: Process raw data
        :return: True if exists
        """
        try:
            EtcdConfiguration.get(key, raw)
            return True
        except (KeyError, etcd.EtcdKeyNotFound):
            return False

    @staticmethod
    def dir_exists(key):
        """
        Check if directory exists in etcd
        :param key: Directory to check
        :return: True if exists
        """
        return EtcdConfiguration._dir_exists(key)

    @staticmethod
    def list(key):
        """
        List all keys in tree
        :param key: Key to list
        :return: Generator object
        """
        return EtcdConfiguration._list(key)

    @staticmethod
    def initialize_host(host_id):
        """
        Initialize keys when setting up a host
        :param host_id: ID of the host
        :return: None
        """
        base_config = {'/storagedriver': {'rsp': '/var/rsp',
                                          'vmware_mode': 'ganesha'},
                       '/ports': {'storagedriver': [[26200, 26299]],
                                  'mds': [[26300, 26399]],
                                  'arakoon': [26400]},
                       '/setupcompleted': False,
                       '/versions': {'ovs': 4},
                       '/type': 'UNCONFIGURED'}
        for key, value in base_config.iteritems():
            EtcdConfiguration._set('/ovs/framework/hosts/{0}/{1}'.format(host_id, key), value, raw=False)

    @staticmethod
    def initialize(external_etcd=None):
        """
        Initialize general keys for all hosts in cluster
        :param external_etcd: ETCD runs on another host outside the cluster
        :return: None
        """
        cluster_id = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
        base_config = {'/cluster_id': cluster_id,
                       '/external_etcd': external_etcd,
                       '/registered': False,
                       '/memcache': {'endpoints': []},
                       '/messagequeue': {'endpoints': [],
                                         'protocol': 'amqp',
                                         'user': 'ovs',
                                         'port': 5672,
                                         'password': '0penv5tor4ge',
                                         'queues': {'storagedriver': 'volumerouter'}},
                       '/plugins/installed': {'backends': [],
                                              'generic': []},
                       '/stores': {'persistent': 'pyrakoon',
                                   'volatile': 'memcache'},
                       '/paths': {'cfgdir': '/opt/OpenvStorage/config',
                                  'basedir': '/opt/OpenvStorage',
                                  'ovsdb': '/opt/OpenvStorage/db'},
                       '/support': {'enablesupport': False,
                                    'enabled': True,
                                    'interval': 60},
                       '/storagedriver': {'mds_safety': 2,
                                          'mds_tlogs': 100,
                                          'mds_maxload': 75},
                       '/webapps': {'html_endpoint': '/',
                                    'oauth2': {'mode': 'local'}}}
        for key, value in base_config.iteritems():
            EtcdConfiguration._set('/ovs/framework/{0}'.format(key), value, raw=False)

    @staticmethod
    @log_slow_calls
    def _dir_exists(key):
        try:
            client = EtcdConfiguration._get_client()
            return client.get(key).dir
        except (KeyError, etcd.EtcdKeyNotFound):
            return False

    @staticmethod
    @log_slow_calls
    def _list(key):
        client = EtcdConfiguration._get_client()
        for child in client.get(key).children:
            if child.key != key:
                yield child.key.replace('{0}/'.format(key), '')

    @staticmethod
    @log_slow_calls
    def _delete(key, recursive):
        client = EtcdConfiguration._get_client()
        client.delete(key, recursive=recursive)

    @staticmethod
    @log_slow_calls
    def _get(key, raw):
        client = EtcdConfiguration._get_client()
        data = client.read(key).value
        if raw is True:
            return data
        return json.loads(data)

    @staticmethod
    @log_slow_calls
    def _set(key, value, raw):
        client = EtcdConfiguration._get_client()
        data = value
        if raw is False:
            data = json.dumps(value)
        client.write(key, data)
        try:
            from ovs.extensions.storage.persistentfactory import PersistentFactory
            client = PersistentFactory.get_client()
            client.set(key, value)
        except:
            pass

    @staticmethod
    def _get_client():
        return etcd.Client(port=2379, use_proxies=True)

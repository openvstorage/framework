# Copyright 2016 iNuron NV
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
Generic module for managing configuration in Etcd
"""

import copy
import etcd
import json
import time
import random
import signal
import string
import unittest
import logging
from itertools import groupby
from ovs.log.logHandler import LogHandler
try:
    from requests.packages.urllib3 import disable_warnings
except ImportError:
    import requests
    try:
        reload(requests)  # Required for 2.6 > 2.7 upgrade (new requests.packages module)
    except ImportError:
        pass  # So, this reload fails because of some FileNodeWarning that can't be found. But it did reload. Yay.
    from requests.packages.urllib3 import disable_warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import SNIMissingWarning
logging.getLogger('urllib3').setLevel(logging.WARNING)


def log_slow_calls(f):
    """
    Wrapper to print duration when call takes > 1s
    :param f: Function to wrap
    :return: Wrapped function
    """
    logger = LogHandler.get('extensions', name='etcdconfiguration')

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
    _unittest_data = {}
    base_config = {'cluster_id': None,
                   'external_etcd': None,
                   'plugins/installed': {'backends': [],
                                         'generic': []},
                   'paths': {'cfgdir': '/opt/OpenvStorage/config',
                             'basedir': '/opt/OpenvStorage',
                             'ovsdb': '/opt/OpenvStorage/db'},
                   'support': {'enablesupport': False,
                               'enabled': True,
                               'interval': 60},
                   'storagedriver': {'mds_safety': 2,
                                     'mds_tlogs': 100,
                                     'mds_maxload': 75},
                   'webapps': {'html_endpoint': '/',
                               'oauth2': {'mode': 'local'}}}

    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

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
    def initialize_host(host_id, port_info=None):
        """
        Initialize keys when setting up a host
        :param host_id: ID of the host
        :type host_id: str

        :param port_info: Information about ports to be used
        :type port_info: dict

        :return: None
        """
        if EtcdConfiguration.exists('/ovs/framework/hosts/{0}/setupcompleted'.format(host_id)):
            return
        if port_info is None:
            port_info = {}

        mds_port_range = port_info.get('mds', [26300, 26399])
        arakoon_start_port = port_info.get('arakoon', 26400)
        storagedriver_port_range = port_info.get('storagedriver', [26200, 26299])

        host_config = {'storagedriver': {'rsp': '/var/rsp',
                                         'vmware_mode': 'ganesha'},
                       'ports': {'storagedriver': [storagedriver_port_range],
                                 'mds': [mds_port_range],
                                 'arakoon': [arakoon_start_port]},
                       'setupcompleted': False,
                       'versions': {'ovs': 4},
                       'type': 'UNCONFIGURED'}
        for key, value in host_config.iteritems():
            EtcdConfiguration._set('/ovs/framework/hosts/{0}/{1}'.format(host_id, key), value, raw=False)

    @staticmethod
    def initialize(external_etcd=None, logging_target=None):
        """
        Initialize general keys for all hosts in cluster
        :param external_etcd: ETCD runs on another host outside the cluster
        :param logging_target: Configures (overwrites) logging configuration
        """
        cluster_id = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
        if EtcdConfiguration.exists('/ovs/framework/cluster_id'):
            return

        messagequeue_cfg = {'endpoints': [],
                            'metadata': {'internal': True},
                            'protocol': 'amqp',
                            'user': 'ovs',
                            'password': '0penv5tor4ge',
                            'queues': {'storagedriver': 'volumerouter'}}

        base_cfg = copy.deepcopy(EtcdConfiguration.base_config)
        base_cfg.update({'cluster_id': cluster_id,
                         'external_etcd': external_etcd,
                         'arakoon_clusters': {},
                         'stores': {'persistent': 'pyrakoon',
                                    'volatile': 'memcache'},
                         'messagequeue': {'protocol': 'amqp',
                                          'queues': {'storagedriver': 'volumerouter'}},
                         'logging': {'type': 'console'}})
        if logging_target is not None:
            base_cfg['logging'] = logging_target
        if EtcdConfiguration.exists('/ovs/framework/memcache') is False:
            base_cfg['memcache'] = {'endpoints': [],
                                    'metadata': {'internal': True}}
        if EtcdConfiguration.exists('/ovs/framework/messagequeue') is False:
            base_cfg['messagequeue'] = messagequeue_cfg
        else:
            messagequeue_info = EtcdConfiguration.get('/ovs/framework/messagequeue')
            for key, value in messagequeue_cfg.iteritems():
                if key not in messagequeue_info:
                    base_cfg['messagequeue'][key] = value
        for key, value in base_cfg.iteritems():
            EtcdConfiguration._set('/ovs/framework/{0}'.format(key), value, raw=False)

    @staticmethod
    @log_slow_calls
    def _dir_exists(key):
        key = EtcdConfiguration._coalesce_dashes(key=key)

        # Unittests
        if hasattr(unittest, 'running_tests') and getattr(unittest, 'running_tests') is True:
            stripped_key = key.strip('/')
            current_dict = EtcdConfiguration._unittest_data
            for part in stripped_key.split('/'):
                if part not in current_dict or not isinstance(current_dict[part], dict):
                    return False
                current_dict = current_dict[part]
            return True

        # Real implementation
        try:
            client = EtcdConfiguration._get_client()
            return client.get(key).dir
        except (KeyError, etcd.EtcdKeyNotFound):
            return False

    @staticmethod
    @log_slow_calls
    def _list(key):
        key = EtcdConfiguration._coalesce_dashes(key=key)

        # Unittests
        if hasattr(unittest, 'running_tests') and getattr(unittest, 'running_tests') is True:
            data = EtcdConfiguration._unittest_data
            ends_with_dash = key.endswith('/')
            starts_with_dash = key.startswith('/')
            stripped_key = key.strip('/')
            for part in stripped_key.split('/'):
                if part not in data:
                    raise etcd.EtcdKeyNotFound('Key not found: {0}'.format(key))
                data = data[part]
            if data:
                for sub_key in data:
                    if ends_with_dash is True:
                        yield '/{0}/{1}'.format(stripped_key, sub_key)
                    else:
                        yield sub_key if starts_with_dash is True else '/{0}'.format(sub_key)
            elif starts_with_dash is False or ends_with_dash is True:
                yield '/{0}'.format(stripped_key)
            return

        # Real implementation
        client = EtcdConfiguration._get_client()
        for child in client.get(key).children:
            if child.key is not None and child.key != key:
                yield child.key.replace('{0}/'.format(key), '')

    @staticmethod
    @log_slow_calls
    def _delete(key, recursive):
        key = EtcdConfiguration._coalesce_dashes(key=key)

        # Unittests
        if hasattr(unittest, 'running_tests') and getattr(unittest, 'running_tests') is True:
            stripped_key = key.strip('/')
            data = EtcdConfiguration._unittest_data
            for part in stripped_key.split('/')[:-1]:
                if part not in data:
                    raise etcd.EtcdKeyNotFound('Key not found : {0}'.format(key))
                data = data[part]
            key_to_remove = stripped_key.split('/')[-1]
            if key_to_remove in data:
                del data[key_to_remove]
            return

        # Real implementation
        client = EtcdConfiguration._get_client()
        client.delete(key, recursive=recursive)

    @staticmethod
    @log_slow_calls
    def _get(key, raw):
        key = EtcdConfiguration._coalesce_dashes(key=key)

        # Unittests
        if hasattr(unittest, 'running_tests') and getattr(unittest, 'running_tests') is True:
            if key in ['', '/']:
                return
            stripped_key = key.strip('/')
            data = EtcdConfiguration._unittest_data
            for part in stripped_key.split('/')[:-1]:
                if part not in data:
                    raise etcd.EtcdKeyNotFound('Key not found : {0}'.format(key))
                data = data[part]
            last_part = stripped_key.split('/')[-1]
            if last_part not in data:
                raise etcd.EtcdKeyNotFound('Key not found : {0}'.format(key))
            data = data[last_part]
            if isinstance(data, dict):
                data = None
        else:
            # Real implementation
            client = EtcdConfiguration._get_client()
            data = client.read(key).value

        if raw is True:
            return data
        return json.loads(data)

    @staticmethod
    @log_slow_calls
    def _set(key, value, raw):
        key = EtcdConfiguration._coalesce_dashes(key=key)
        data = value
        if raw is False:
            data = json.dumps(value)

        # Unittests
        if hasattr(unittest, 'running_tests') and getattr(unittest, 'running_tests') is True:
            stripped_key = key.strip('/')
            ut_data = EtcdConfiguration._unittest_data
            for part in stripped_key.split('/')[:-1]:
                if part not in ut_data:
                    ut_data[part] = {}
                ut_data = ut_data[part]

            ut_data[stripped_key.split('/')[-1]] = data
            return

        # Real implementation
        client = EtcdConfiguration._get_client()
        client.write(key, data)
        try:
            def _escape(*args, **kwargs):
                _ = args, kwargs
                raise RuntimeError()
            from ovs.extensions.storage.persistentfactory import PersistentFactory
            client = PersistentFactory.get_client()
            signal.signal(signal.SIGALRM, _escape)
            signal.alarm(0.5)  # Wait only 0.5 seconds. This is a backup and should not slow down the system
            client.set(key, value)
            signal.alarm(0)
        except:
            pass

    @staticmethod
    def _get_client():
        return etcd.Client(port=2379, use_proxies=True)

    @staticmethod
    def _coalesce_dashes(key):
        """
        Remove multiple dashes, eg: //ovs//framework/ becomes /ovs/framework/
        :param key: Key to convert
        :type key: str

        :return: Key without multiple dashes after one another
        :rtype: str
        """
        return ''.join(k if k == '/' else ''.join(group) for k, group in groupby(key))

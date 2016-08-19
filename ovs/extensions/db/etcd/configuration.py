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
Generic module for managing configuration in Etcd
"""
import etcd
import time
import logging
from itertools import groupby
from ovs.log.log_handler import LogHandler
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
    Helper for configuration management in Etcd
    """

    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get_configuration_path(key):
        """
        Retrieve the full configuration path for specified key
        :param key: Key to retrieve full configuration path for
        :type key: str
        :return: Configuration path
        :rtype: str
        """
        return 'etcd://127.0.0.1:2379{0}'.format(key)

    @staticmethod
    @log_slow_calls
    def dir_exists(key):
        """
        Verify whether the directory exists
        :param key: Directory to check for existence
        :type key: str
        :return: True if directory exists, false otherwise
        :rtype: bool
        """
        key = EtcdConfiguration._coalesce_dashes(key=key)
        try:
            client = EtcdConfiguration._get_client()
            return client.get(key).dir
        except etcd.EtcdKeyNotFound:
            return False

    @staticmethod
    @log_slow_calls
    def list(key):
        """
        List all keys starting with specified key
        :param key: Key to list
        :type key: str
        :return: Generator with all keys
        :rtype: generator
        """
        key = EtcdConfiguration._coalesce_dashes(key=key)
        client = EtcdConfiguration._get_client()
        for child in client.get(key).children:
            if child.key is not None and child.key != key:
                yield child.key.replace('{0}/'.format(key), '')

    @staticmethod
    @log_slow_calls
    def delete(key, recursive):
        """
        Delete the specified key
        :param key: Key to delete
        :type key: str
        :param recursive: Delete the specified key recursively
        :type recursive: bool
        :return: None
        """
        key = EtcdConfiguration._coalesce_dashes(key=key)
        client = EtcdConfiguration._get_client()
        client.delete(key, recursive=recursive)

    @staticmethod
    @log_slow_calls
    def get(key):
        """
        Retrieve the value for specified key
        :param key: Key to retrieve
        :type key: str
        :return: Value of key
        :rtype: str
        """
        key = EtcdConfiguration._coalesce_dashes(key=key)
        client = EtcdConfiguration._get_client()
        return client.read(key, quorum=True).value

    @staticmethod
    @log_slow_calls
    def set(key, value):
        """
        Set a value for specified key
        :param key: Key to set
        :type key: str
        :param value: Value to set for key
        :type value: str
        :return: None
        """
        key = EtcdConfiguration._coalesce_dashes(key=key)
        client = EtcdConfiguration._get_client()
        client.write(key, value)

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

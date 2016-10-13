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
Generic module for managing configuration in Arakoon
"""
from ConfigParser import RawConfigParser
from ovs.extensions.db.arakoon.pyrakoon.client import PyrakoonClient
from ovs.extensions.generic.toolbox import Toolbox


class ArakoonConfiguration(object):
    """
    Helper for configuration management in Arakoon
    """

    CACC_LOCATION = '/opt/OpenvStorage/config/arakoon_cacc.ini'
    client = None

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
        import urllib
        parser = RawConfigParser()
        with open(ArakoonConfiguration.CACC_LOCATION) as config_file:
            parser.readfp(config_file)
        cluster_id = parser.get('global', 'cluster_id')
        return 'arakoon://{0}/{1}?{2}'.format(
            cluster_id,
            ArakoonConfiguration._clean_key(key),
            urllib.urlencode({'ini': ArakoonConfiguration.CACC_LOCATION})
        )

    @staticmethod
    def dir_exists(key):
        """
        Verify whether the directory exists
        :param key: Directory to check for existence
        :type key: str
        :return: True if directory exists, false otherwise
        :rtype: bool
        """
        key = ArakoonConfiguration._clean_key(key)
        client = ArakoonConfiguration._get_client()
        return any(client.prefix(key))

    @staticmethod
    def list(key):
        """
        List all keys starting with specified key
        :param key: Key to list
        :type key: str
        :return: Generator with all keys
        :rtype: generator
        """
        key = ArakoonConfiguration._clean_key(key)
        client = ArakoonConfiguration._get_client()
        entries = []
        for entry in client.prefix(key):
            if key == '' or entry.startswith(key.rstrip('/') + '/'):
                cleaned = Toolbox.remove_prefix(entry, key).strip('/').split('/')[0]
                if cleaned not in entries:
                    entries.append(cleaned)
                    yield cleaned

    @staticmethod
    def delete(key, recursive):
        """
        Delete the specified key
        :param key: Key to delete
        :type key: str
        :param recursive: Delete the specified key recursively
        :type recursive: bool
        :return: None
        """
        key = ArakoonConfiguration._clean_key(key)
        client = ArakoonConfiguration._get_client()
        if recursive is True:
            client.delete_prefix(key)
        else:
            client.delete(key)

    @staticmethod
    def get(key):
        """
        Retrieve the value for specified key
        :param key: Key to retrieve
        :type key: str
        :return: Value of key
        :rtype: str
        """
        key = ArakoonConfiguration._clean_key(key)
        client = ArakoonConfiguration._get_client()
        return client.get(key)

    @staticmethod
    def set(key, value):
        """
        Set a value for specified key
        :param key: Key to set
        :type key: str
        :param value: Value to set for key
        :type value: str
        :return: None
        """
        if isinstance(value, basestring):
            value = str(value)
        key = ArakoonConfiguration._clean_key(key)
        client = ArakoonConfiguration._get_client()
        client.set(key, value)

    @staticmethod
    def _get_client():
        if ArakoonConfiguration.client is None:
            parser = RawConfigParser()
            with open(ArakoonConfiguration.CACC_LOCATION) as config_file:
                parser.readfp(config_file)
            nodes = {}
            for node in parser.get('global', 'cluster').split(','):
                node = node.strip()
                nodes[node] = ([parser.get(node, 'ip')], parser.get(node, 'client_port'))
            ArakoonConfiguration.client = PyrakoonClient(parser.get('global', 'cluster_id'), nodes)
        return ArakoonConfiguration.client

    @staticmethod
    def _clean_key(key):
        return key.lstrip('/')

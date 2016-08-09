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
from ConfigParser import RawConfigParser
from threading import Lock
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonClient, ArakoonClientConfig


def locked():
    """
    Locking decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        def new_function(self, *args, **kw):
            """
            Executes the decorated function in a locked context
            """
            with self._lock:
                return f(self, *args, **kw)
        return new_function
    return wrap


class ArakoonConfiguration(object):
    """
    Helper for configuration management in Arakoon
    """

    CACC_LOCATION = '/opt/OpenvStorage/config/cacc.ini'
    _client = None
    _lock = Lock()

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get_configuration_path(key):
        return 'arakoon://{0}:{1}'.format(ArakoonConfiguration.CACC_LOCATION, key)

    @staticmethod
    @locked()
    def dir_exists(key):
        client = ArakoonConfiguration._get_client()
        return any(client.prefix(key))

    @staticmethod
    @locked()
    def list(key):
        client = ArakoonConfiguration._get_client()
        for entry in client.prefix(key):
            yield entry.replace(key, '')

    @staticmethod
    @locked()
    def delete(key, recursive):
        client = ArakoonConfiguration._get_client()
        if recursive is True:
            client.deletePrefix(key)
        else:
            client.delete(key)

    @staticmethod
    @locked()
    def get(key):
        client = ArakoonConfiguration._get_client()
        return client.get(key)

    @staticmethod
    @locked()
    def set(key, value):
        client = ArakoonConfiguration._get_client()
        client.set(key, value)

    @staticmethod
    def _get_client():
        if ArakoonConfiguration._client is None:
            parser = RawConfigParser()
            with open(ArakoonConfiguration.CACC_LOCATION) as config_file:
                parser.readfp(config_file)
            nodes = {}
            for node in parser.get('global', 'cluster').split(','):
                node = node.strip()
                nodes[node] = ([str(parser.get(node, 'ip'))], int(parser.get(node, 'client_port')))
            config = ArakoonClientConfig(str(parser.get('global', 'cluster_id')), nodes)
            ArakoonConfiguration._client = ArakoonClient(config)
        return ArakoonConfiguration._client

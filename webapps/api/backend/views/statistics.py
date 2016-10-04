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
Statistics module
"""

import datetime
import memcache
from backend.decorators import required_roles, load, log
from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.generic.configuration import Configuration


class MemcacheViewSet(viewsets.ViewSet):
    """
    Information about memcache instances
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'statistics/memcache'
    base_name = 'memcache'

    @staticmethod
    def _get_memcache_nodes():
        """
        Get the memcache nodes
        """
        return Configuration.get('/ovs/framework/memcache|endpoints')

    @staticmethod
    def _node_stats(host):
        """
        Returns a dict with information about a given memcache instance
        """
        host = memcache._Host(host)
        host.connect()
        host.send_cmd("stats")
        stats = {}
        while 1:
            line = host.readline().split(None, 2)
            if line[0] == "END":
                break
            _, key, value = line
            try:
                # Convert to native type, if possible
                value = int(value)
                if key == "uptime":
                    value = datetime.timedelta(seconds=value)
                elif key == "time":
                    value = datetime.datetime.fromtimestamp(value)
            except ValueError:
                pass
            stats[key] = value
        host.close_socket()
        return stats

    @log()
    @required_roles(['read'])
    @load()
    def list(self):
        """
        Returns statistics information
        """
        nodes = MemcacheViewSet._get_memcache_nodes()
        client = VolatileFactory.get_client('memcache')
        online_nodes = ['%s:%s' % (node.ip, node.port) for node in client._client.servers if node.deaduntil == 0]
        stats = {'nodes': [],
                 'offline': []}
        for node in nodes:
            if node in online_nodes:
                stat = MemcacheViewSet._node_stats(node)
                stat['node'] = node
                stats['nodes'].append(stat)
            else:
                stats['offline'].append(node)
        return Response(stats)

    @log()
    @required_roles(['read'])
    @load()
    def retrieve(self):
        """
        Returns statistics information
        """
        nodes = MemcacheViewSet._get_memcache_nodes()
        client = VolatileFactory.get_client('memcache')
        online_nodes = ['%s:%s' % (node.ip, node.port) for node in client._client.servers if node.deaduntil == 0]
        stats = {'nodes': [],
                 'offline': []}
        for node in nodes:
            if node in online_nodes:
                stat = MemcacheViewSet._node_stats(node)
                stat['node'] = node
                stats['nodes'].append(stat)
            else:
                stats['offline'].append(node)
        return Response(stats)

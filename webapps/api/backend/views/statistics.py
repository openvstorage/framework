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

"""
Statistics module
"""
import re
import datetime
import memcache
import os
from backend.serializers.memcached import MemcacheSerializer
from backend.decorators import required_roles, expose
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.tools import Tools


class MemcacheViewSet(viewsets.ViewSet):
    """
    Information about memcache instances
    """
    permission_classes = (IsAuthenticated,)

    @staticmethod
    def _get_memcache_nodes():
        """
        Get the memcache nodes
        """
        memcache_ini = Tools.inifile.open(os.path.join(Configuration.get('ovs.core.cfgdir'), 'memcacheclient.cfg'))
        nodes = memcache_ini.getValue('main', 'nodes').split(',')
        nodes = [node.strip() for node in nodes]
        return map(lambda m: memcache_ini.getValue(m, 'location'), nodes)

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

    @staticmethod
    def _dal_stats():
        """
        Creates a dict with DAL statistics
        """
        stats = {}
        volatile = VolatileFactory.get_client()
        keys = ['datalist', 'object_load', 'descriptor', 'relations']
        for key in keys:
            for hittype in ['hit', 'miss']:
                cachekey = 'ovs_stats_cache_%s_%s' % (key, hittype)
                stats['%s_%s' % (key, hittype)] = volatile.get(cachekey, default=0)
        return stats

    @expose(internal=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Returns statistics information
        """
        _ = request, format
        memcache_nodes = MemcacheViewSet._get_memcache_nodes()
        stats = {'dal': MemcacheViewSet._dal_stats(),
                 'nodes': []}
        for node in memcache_nodes:
            stat = MemcacheViewSet._node_stats(node)
            stat['node'] = node
            stats['nodes'].append(stat)
        return Response(stats)

    @expose(internal=True)
    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Returns statistics information
        """
        _ = request, format, pk
        memcache_nodes = MemcacheViewSet._get_memcache_nodes()
        stats = {'dal': MemcacheViewSet._dal_stats(),
                 'nodes': []}
        for node in memcache_nodes:
            stat = MemcacheViewSet._node_stats(node)
            stat['node'] = node
            stats['nodes'].append(stat)
        return Response(stats)

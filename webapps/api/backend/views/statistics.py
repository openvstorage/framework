# license see http://www.openvstorage.com/licenses/opensource/
"""
Statistics module
"""
import re
import datetime
import ConfigParser
from backend.serializers.memcached import MemcacheSerializer
from backend.decorators import required_roles, expose
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from jumpscale import j


class MemcacheViewSet(viewsets.ViewSet):
    """
    Information about memcache instances
    """
    permission_classes = (IsAuthenticated,)

    @staticmethod
    def _get_memcachelocation():
        """
        Get the memcache location from hrd
        """
        return '{}:{}'.format(j.application.config.get('ovscore.memcache.localnode.ip'),
                              j.application.config.get('ovscore.memcache.localnode.port'))

    @staticmethod
    def _get_instance(host):
        """
        Returns a class with more information about a given memcache instance
        """
        class Stats:
            """
            Placeholder class for statistics
            """
            pass

        import memcache
        host = memcache._Host(host)
        host.connect()
        host.send_cmd("stats")
        stats = Stats()
        while 1:
            line = host.readline().split(None, 2)
            if line[0] == "END":
                break
            stat, key, value = line
            try:
                # Convert to native type, if possible
                value = int(value)
                if key == "uptime":
                    value = datetime.timedelta(seconds=value)
                elif key == "time":
                    value = datetime.datetime.fromtimestamp(value)
            except ValueError:
                pass
            setattr(stats, key, value)
        host.close_socket()
        return stats

    @expose(internal=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Returns statistics information
        """
        _ = request, format
        match = re.match("([.\w]+:\d+)", MemcacheViewSet._get_memcachelocation())
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        stats = MemcacheViewSet._get_instance(match.group(1))
        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)

    @expose(internal=True)
    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Returns statistics information
        """
        _ = request, format, pk
        match = re.match("([.\w]+:\d+)", MemcacheViewSet._get_memcachelocation())
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        stats = MemcacheViewSet._get_instance(match.group(1))
        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)

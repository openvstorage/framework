import re
import datetime
import settings
from backend.serializers.memcached import MemcacheSerializer
from backend.decorators import required_roles
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated


class MemcacheViewSet(viewsets.ViewSet):
    """
    Information about memcache instances
    """
    permission_classes = (IsAuthenticated,)

    def _get_instance(self, host):
        class Stats:
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

    @required_roles(['view'])
    def list(self, request, format=None):
        match = re.match("([.\w]+:\d+)", settings.CACHES['default']['LOCATION'])
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        stats = self._get_instance(match.group(1))
        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)

    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        match = re.match("([.\w]+:\d+)", settings.CACHES['default']['LOCATION'])
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        stats = self._get_instance(match.group(1))
        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)

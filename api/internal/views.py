import re
import datetime
from backend.serializers.memcached import MemcacheSerializer
from rest_framework.views import APIView
from rest_framework import status, permissions
from rest_framework.response import Response
from rest_framework.reverse import reverse
from django.conf import settings


class APIRoot(APIView):
    def get(self, request, format=None):
        return Response({'memcached': reverse('memcached', request=request, format=format)})


class Memcached(APIView):
    """
    Provides a snapshot of the memcached instance configured in the Django backend.
    """
    permission_classes = (permissions.IsAuthenticated,)

    def get(self, request, format=None):
        import memcache

        # Get first memcached URI
        match = re.match("([.\w]+:\d+)", settings.CACHES['default']['LOCATION'])
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)

        host = memcache._Host(match.group(1))
        host.connect()
        host.send_cmd("stats")

        class Stats:
            pass

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

        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)
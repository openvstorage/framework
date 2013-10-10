import re
import datetime
from backend.serializers.memcached import MemcacheSerializer
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.conf import settings


@api_view(['GET'])
def memcached(request, format=None):
    """
    List all snippets, or create a new snippet.
    """
    if request.method == 'GET':
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
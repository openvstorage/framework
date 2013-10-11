import re
import datetime
import settings
from backend.serializers.user import UserSerializer, PasswordSerializer
from backend.serializers.memcached import MemcacheSerializer
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovsdal.exceptions import ObjectNotFoundException
from ovsdal.hybrids.user import User
from ovsapi.user import User as APIUser
from django.http import Http404


class UserViewSet(viewsets.ViewSet):
    """
    Manage users
    """
    permission_classes = (IsAuthenticated,)

    def _get_object(self, guid):
        try:
            return User(guid)
        except ObjectNotFoundException:
            raise Http404

    def list(self, request, format=None):
        users = APIUser.get_users()
        serializer = UserSerializer(users, many=True)
        return Response(serializer.data)

    def retrieve(self, request, pk=None, format=None):
        user = self._get_object(pk)
        serializer = UserSerializer(user)
        return Response(serializer.data)

    def create(self, request, format=None):
        serializer = UserSerializer(User(), request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action()
    def set_password(self, request, pk=None, format=None):
        user = self._get_object(pk)
        serializer = PasswordSerializer(data=request.DATA)
        if serializer.is_valid():
            user.password = serializer.data['password']
            user.save()
            return Response(UserSerializer(user).data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


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

    def list(self, request, format=None):
        match = re.match("([.\w]+:\d+)", settings.CACHES['default']['LOCATION'])
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        stats = self._get_instance(match.group(1))
        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)

    def retrieve(self, request, pk=None, format=None):
        match = re.match("([.\w]+:\d+)", settings.CACHES['default']['LOCATION'])
        if not match:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        stats = self._get_instance(match.group(1))
        serializer = MemcacheSerializer(stats)
        return Response(serializer.data)


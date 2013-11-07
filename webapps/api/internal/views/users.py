from backend.serializers.user import PasswordSerializer
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.user import User
from ovs.dal.lists.userlist import UserList
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

    @required_roles(['view', 'system'])
    def list(self, request, format=None):
        users = UserList.get_users()
        serializer = FullSerializer(User, instance=users, many=True)
        return Response(serializer.data)

    @required_roles(['view', 'system'])
    def retrieve(self, request, pk=None, format=None):
        user = self._get_object(pk)
        serializer = FullSerializer(User, instance=user)
        return Response(serializer.data)

    @required_roles(['view', 'create', 'system'])
    def create(self, request, format=None):
        serializer = FullSerializer(User, User(), request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @required_roles(['view', 'delete', 'system'])
    def delete(self, request, pk=None, format=None):
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        else:
            user = self._get_object(pk)
            user.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)

    @required_roles(['view', 'update', 'system'])
    @action()
    def set_password(self, request, pk=None, format=None):
        user = self._get_object(pk)
        serializer = PasswordSerializer(data=request.DATA)
        if serializer.is_valid():
            user.password = serializer.data['password']
            user.save()
            return Response(FullSerializer(User, user).data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

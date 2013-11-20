"""
Module for users
"""
from backend.serializers.user import PasswordSerializer
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles
from backend.toolbox import Toolbox
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.user import User
from ovs.dal.lists.userlist import UserList
from django.http import Http404
import hashlib


class UserViewSet(viewsets.ViewSet):
    """
    Manage users
    """
    permission_classes = (IsAuthenticated,)

    @staticmethod
    def _get_object(guid):
        """
        Gets a user object, raises a 404 in case it doesn't exist
        """
        try:
            return User(guid)
        except ObjectNotFoundException:
            raise Http404

    @required_roles(['view', 'system'])
    def list(self, request, format=None):
        """
        Lists all available users
        """
        _ = request, format
        users = UserList.get_users()
        serializer = FullSerializer(User, instance=users, many=True)
        return Response(serializer.data)

    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Retreives a single user instance.
        Only the currently logged in user is accessible, or all if the logged in user has a
        system role
        """
        _ = format
        user = UserViewSet._get_object(pk)
        loggedin_user = User(request.user.username)
        if user.username == loggedin_user.username or Toolbox.is_user_in_roles(loggedin_user, ['system']):
            serializer = FullSerializer(User, instance=user)
            return Response(serializer.data)
        return Response(status=status.HTTP_400_BAD_REQUEST)

    @required_roles(['view', 'create', 'system'])
    def create(self, request, format=None):
        """
        Creates a user
        """
        _ = format
        serializer = FullSerializer(User, User(), request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @required_roles(['view', 'delete', 'system'])
    def delete(self, request, pk=None, format=None):
        """
        Deletes a user
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        else:
            user = UserViewSet._get_object(pk)
            user.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)

    @action()
    @required_roles(['view'])
    def set_password(self, request, pk=None, format=None):
        """
        Sets the password of a given user. A logged in user can only changes its own password,
        or all passwords if he's member of a system role
        """
        _ = format
        user = UserViewSet._get_object(pk)
        loggedin_user = User(request.user.username)
        if user.username == loggedin_user.username or Toolbox.is_user_in_roles(loggedin_user, ['update', 'system']):
            serializer = PasswordSerializer(data=request.DATA)
            if serializer.is_valid():
                if user.password == hashlib.sha256(str(serializer.data['current_password'])).hexdigest():
                    user.password = hashlib.sha256(str(serializer.data['new_password'])).hexdigest()
                    user.save()
                    return Response(FullSerializer(User, user).data, status=status.HTTP_202_ACCEPTED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

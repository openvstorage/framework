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
Django Views module for main API
"""
from rest_framework.views import APIView
from rest_framework import status
from rest_framework import parsers
from rest_framework import renderers
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from ovs.dal.lists.userlist import UserList


class ObtainAuthToken(APIView):
    """
    Custom implementation of the Django REST framework ObtainAuthToken class, making
    use of our own model/token system
    """
    throttle_classes = ()
    permission_classes = ()
    parser_classes = (parsers.FormParser, parsers.MultiPartParser, parsers.JSONParser,)
    renderer_classes = (renderers.JSONRenderer,)
    serializer_class = AuthTokenSerializer
    model = Token

    def post(self, request):
        """
        Handles authentication post for Django REST framework
        """
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            user = UserList.get_user_by_username(serializer.object['user'].username)
            if user is not None:
                return Response({'token': user.guid})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

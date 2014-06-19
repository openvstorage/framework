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
Contains various decorator
"""

import math

from ovs.dal.lists.userlist import UserList
from rest_framework.response import Response
from toolbox import Toolbox
from rest_framework.exceptions import PermissionDenied, NotAuthenticated
from rest_framework import status
from django.http import Http404
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import FullSerializer
from ovs.log.logHandler import LogHandler

logger = LogHandler('api')


def required_roles(roles):
    """
    Role validation decorator
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(*args, **kw):
            """
            Wrapped function
            """
            django_user = args[1].user
            user = UserList.get_user_by_username(django_user.username)
            if user is None:
                raise NotAuthenticated()
            if not Toolbox.is_user_in_roles(user, roles):
                raise PermissionDenied('This call requires roles: %s' % (', '.join(roles)))
            return f(*args, **kw)
        return new_function
    return wrap


def validate(object_type):
    """
    Parameter/object validation decorator
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, request, pk=None, format=None):
            """
            Wrapped function
            """
            _ = self, format
            if pk is None:
                raise Http404
            else:
                try:
                    obj = object_type(pk)
                except ObjectNotFoundException:
                    raise Http404('Given object not found')
                return f(self, request=request, obj=obj)
        return new_function
    return wrap


def expose(internal=False, customer=False):
    """
    Used to mark a method on a ViewSet that should be included for which API
    """
    def decorator(func):
        modes = []
        if internal:
            modes.append('internal')
        if customer:
            modes.append('customer')
        func.api_mode = modes
        return func
    return decorator


def get_list(object_type, default_sort=None):
    """
    List decorator
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, request, *args, **kwargs):
            """
            Wrapped function
            """
            _ = self

            # 1. Pre-loading request data
            sort = request.QUERY_PARAMS.get('sort')
            if sort is None and default_sort is not None:
                sort = default_sort
            sort = None if sort is None else reversed(sort.split(','))
            page = request.QUERY_PARAMS.get('page')
            page = int(page) if page is not None and page.isdigit() else None
            contents = request.QUERY_PARAMS.get('contents')
            contents = None if contents is None else contents.split(',')

            # 2. Construct hints for decorated function (so it can provide full objects if required)
            if 'hints' not in kwargs:
                kwargs['hints'] = {}
            kwargs['hints']['full'] = sort is not None or contents is not None

            # 3. Fetch data
            data_list = f(self, request=request, *args, **kwargs)
            guid_list = isinstance(data_list, list) and len(data_list) > 0 and isinstance(data_list[0], basestring)

            # 4. Sorting
            if sort is not None:
                if guid_list is True:
                    data_list = [object_type(guid) for guid in data_list]
                    guid_list = False  # The list is converted to objects
                for sort_item in sort:
                    desc = sort_item[0] == '-'
                    field = sort_item[1 if desc else 0:]
                    data_list.sort(key=lambda e: Toolbox.extract_key(e, field), reverse=desc)

            # 5. Paging
            if page is not None:
                max_page = int(math.ceil(len(data_list) / 10.0))
                if page > max_page:
                    page = max_page
                page -= 1
                data_list = data_list[page * 10: (page + 1) * 10]

            # 6. Serializing
            if contents is not None:
                if guid_list is True:
                    data_list = [object_type(guid) for guid in data_list]
                data = FullSerializer(object_type, contents=contents, instance=data_list, many=True).data
            else:
                if guid_list is False:
                    data_list = [item.guid for item in data_list]
                data = data_list

            # 7. Building response
            return Response(data, status=status.HTTP_200_OK)

        return new_function
    return wrap


def get_object(object_type):
    """
    Object decorator
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, request, *args, **kwargs):
            """
            Wrapped function
            """
            _ = self

            # 1. Pre-loading request data
            contents = request.QUERY_PARAMS.get('contents')
            contents = None if contents is None else contents.split(',')

            # 5. Serializing
            obj = f(self, request, *args, **kwargs)
            return Response(FullSerializer(object_type, contents=contents, instance=obj).data, status=status.HTTP_200_OK)

        return new_function
    return wrap


def celery_task():
    """
    Object decorator
    """

    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, *args, **kwargs):
            """
            Wrapped function
            """
            _ = self
            task = f(self, *args, **kwargs)
            return Response(task.id, status=status.HTTP_200_OK)

        return new_function

    return wrap


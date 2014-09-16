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
import re
import inspect
from ovs.dal.lists.userlist import UserList
from rest_framework.response import Response
from toolbox import Toolbox
from rest_framework.exceptions import PermissionDenied, NotAuthenticated, NotAcceptable
from rest_framework import status
from django.http import Http404
from django.conf import settings
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import FullSerializer
from ovs.log.logHandler import LogHandler

logger = LogHandler('api')
regex = re.compile('^(.*; )?version=(?P<version>([0-9]+|\*)?)(;.*)?$')


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
            request = args[1]
            if not hasattr(request, 'user') or not hasattr(request, 'client'):
                raise NotAuthenticated()
            user = UserList.get_user_by_username(request.user.username)
            if user is None:
                raise NotAuthenticated()
            if not Toolbox.is_token_in_roles(request.token, roles):
                raise PermissionDenied('This call requires roles: %s' % (', '.join(roles)))
            return f(*args, **kw)
        return new_function
    return wrap


def load(object_type=None, min_version=settings.VERSION[0], max_version=settings.VERSION[-1]):
    """
    Parameter discovery decorator
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, request, **kwargs):
            """
            Wrapped function
            """
            new_kwargs = {}
            # Find out the arguments of the decorated function
            function_info = inspect.getargspec(f)
            if function_info.defaults is None:
                mandatory_vars = function_info.args[1:]
                optional_vars = []
            else:
                mandatory_vars = function_info.args[1:-len(function_info.defaults)]
                optional_vars = function_info.args[len(mandatory_vars) + 1:]
            # Check versioning
            version = regex.match(request.META['HTTP_ACCEPT']).groupdict()['version']
            versions = (max(min_version, settings.VERSION[0]), min(max_version, settings.VERSION[-1]))
            if version == '*':  # If accepting all versions, it defaults to the highest one
                version = settings.VERSION[-1]
            version = int(version)
            if version < versions[0] or version > versions[1]:
                raise NotAcceptable('API version requirements: {0} <= <version> <= {1}'.format(versions[0], versions[1]))
            if 'version' in mandatory_vars:
                new_kwargs['version'] = version
            # Fill request parameter, if available
            if 'request' in mandatory_vars:
                new_kwargs['request'] = request
                mandatory_vars.remove('request')
            # Fill main object, if required
            if 'pk' in kwargs and object_type is not None:
                typename = object_type.__name__.lower()
                try:
                    instance = object_type(kwargs['pk'])
                    if typename in mandatory_vars:
                        new_kwargs[typename] = instance
                        mandatory_vars.remove(typename)
                except ObjectNotFoundException:
                    raise Http404()
            # Fill mandatory parameters
            for name in mandatory_vars:
                if name in kwargs:
                    new_kwargs[name] = kwargs[name]
                else:
                    if name not in request.DATA:
                        if name not in request.QUERY_PARAMS:
                            raise NotAcceptable('Invalid data passed: {0} is missing'.format(name))
                        new_kwargs[name] = request.QUERY_PARAMS[name]
                    else:
                        new_kwargs[name] = request.DATA[name]
            # Try to fill optional parameters
            for name in optional_vars:
                if name in kwargs:
                    new_kwargs[name] = kwargs[name]
                else:
                    if name in request.DATA:
                        new_kwargs[name] = request.DATA[name]
                    elif name in request.QUERY_PARAMS:
                        new_kwargs[name] = request.QUERY_PARAMS[name]
            # Call the function
            return f(self, **new_kwargs)
        return new_function
    return wrap


def return_list(object_type, default_sort=None):
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
            sort = None if sort is None else [s for s in reversed(sort.split(','))]
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
            items_pp = 10
            total_items = len(data_list)
            page_metadata = {'total_items': total_items,
                             'current_page': 1,
                             'max_page': 1,
                             'start_number': min(1, total_items),
                             'end_number': total_items}
            if page is not None:
                max_page = int(math.ceil(total_items / (items_pp * 1.0)))
                if page > max_page:
                    page = max_page
                if page == 0:
                    start_number = -1
                    end_number = 0
                else:
                    start_number = (page - 1) * items_pp  # Index - e.g. 0 for page 1, 10 for page 2
                    end_number = start_number + items_pp  # Index - e.g. 10 for page 1, 20 for page 2
                data_list = data_list[start_number: end_number]
                page_metadata = dict(page_metadata.items() + {'current_page': max(1, page),
                                                              'max_page': max(1, max_page),
                                                              'start_number': start_number + 1,
                                                              'end_number': min(total_items, end_number)}.items())

            # 6. Serializing
            if contents is not None:
                if guid_list is True:
                    data_list = [object_type(guid) for guid in data_list]
                data = FullSerializer(object_type, contents=contents, instance=data_list, many=True).data
            else:
                if guid_list is False:
                    data_list = [item.guid for item in data_list]
                data = data_list

            result = {'data': data,
                      '_paging': page_metadata,
                      '_contents': contents,
                      '_sorting': [s for s in reversed(sort)] if sort else sort}

            # 7. Building response
            return Response(result, status=status.HTTP_200_OK)

        return new_function
    return wrap


def return_object(object_type):
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


def return_task():
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

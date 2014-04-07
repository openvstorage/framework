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
from ovs.dal.lists.userlist import UserList
from toolbox import Toolbox
from rest_framework.exceptions import PermissionDenied, NotAuthenticated
from django.http import Http404
from ovs.dal.exceptions import ObjectNotFoundException


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

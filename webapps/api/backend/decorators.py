# license see http://www.openvstorage.com/licenses/opensource/
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
                    raise Http404
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

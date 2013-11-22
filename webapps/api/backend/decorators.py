# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains various decorator
"""
from ovs.dal.hybrids.user import User
from toolbox import Toolbox
from django.core.exceptions import PermissionDenied


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
            user = User(django_user.username)
            if not Toolbox.is_user_in_roles(user, roles):
                raise PermissionDenied('This call requires roles: %s' % (', '.join(roles)))
            return f(*args, **kw)
        return new_function
    return wrap


def internal():
    """
    Used to mark a method on a ViewSet that should be included for the internal API
    """
    def decorator(func):
        modes = getattr(func, 'api_mode', [])
        modes.append('internal')
        func.api_mode = modes
        return func
    return decorator


def customer():
    """
    Used to mark a method on a ViewSet that should be included for the customer API
    """
    def decorator(func):
        modes = getattr(func, 'api_mode', [])
        modes.append('customer')
        func.api_mode = modes
        return func
    return decorator

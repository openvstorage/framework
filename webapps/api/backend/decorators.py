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

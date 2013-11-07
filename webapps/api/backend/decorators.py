from ovs.dal.hybrids.user import User
from toolbox import Toolbox
from django.core.exceptions import PermissionDenied


def required_roles(roles):
    """ role validation decorator """
    def wrap(f):
        def new_function(*args, **kw):
            django_user = args[1].user
            user = User(django_user.username)
            if not Toolbox.is_user_in_roles(user, roles):
                raise PermissionDenied('This call requires roles: %s' % (', '.join(roles)))
            return f(*args, **kw)
        return new_function
    return wrap
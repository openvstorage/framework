from ovs.dal.hybrids.user import User
from django.core.exceptions import PermissionDenied


def required_roles(roles):
    """ role validation decorator """
    def wrap(f):
        def new_function(*args, **kw):
            django_user = args[1].user
            user = User(django_user.username)
            user_roles = [rolegroup.role.code for rolegroup in user.group.rolegroups]

            for required_role in roles:
                if required_role not in user_roles:
                    raise PermissionDenied('This call requires roles: %s' % (', '.join(roles)))
            return f(*args, **kw)
        return new_function
    return wrap
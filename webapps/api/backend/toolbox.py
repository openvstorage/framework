# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains various helping classes
"""
import re


class Toolbox:
    """
    This class contains generic methods
    """
    @staticmethod
    def is_uuid(string):
        """
        Checks whether a given string is a valid guid
        """
        regex = re.compile('^[0-9a-f]{22}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        return regex.match(string)

    @staticmethod
    def is_user_in_roles(user, roles):
        """
        Checks whether a user is member of a set of roles
        """
        user_roles = [j.role.code for j in user.group.roles]
        for required_role in roles:
            if required_role not in user_roles:
                return False
        return True

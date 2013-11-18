import re


class Toolbox:
    @staticmethod
    def is_uuid(string):
        regex = re.compile('^[0-9a-f]{22}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        return regex.match(string)

    @staticmethod
    def is_user_in_roles(user, roles):
        user_roles = [rolegroup.role.code for rolegroup in user.group.rolegroups]
        for required_role in roles:
            if required_role not in user_roles:
                return False
        return True
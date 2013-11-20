"""
Model migration module
"""

import hashlib
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.group import Group
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.rolegroup import RoleGroup


class Model():
    """
    Handles all model related migrations
    """

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version):
        """
        Migrates from any version to any version, running all migrations required
        If previous_version is for example 0 (0.0.1) and this script is at
        verison 3 (0.0.3) it will execute two steps:
          - 0.0.1 > 0.0.2
          - 0.0.2 > 0.0.3
        @param previous_version: The previous version from which to start the migration.
        """

        working_version = previous_version

        # Version 0.0.1 introduced:
        if working_version < 1:
            # Create groups
            admin_group = Group()
            admin_group.name = 'administrators'
            admin_group.description = 'Administrative group. Have full access'
            admin_group.save()
            user_group = Group()
            user_group.name = 'users'
            user_group.description = 'User group, have limited rights'
            user_group.save()

            # Create users
            admin = User()
            admin.username = 'admin'
            admin.password = hashlib.sha256('admin').hexdigest()
            admin.email = 'nobody@example.net'
            admin.is_active = True
            admin.group = admin_group
            admin.save()
            user = User()
            user.username = 'user'
            user.password = hashlib.sha256('user').hexdigest()
            user.email = 'nobody@example.net'
            user.is_active = True
            user.group = user_group
            user.save()

            # Create roles
            view_role = Role()
            view_role.code = 'view'
            view_role.name = 'Viewer'
            view_role.description = 'Can view objects'
            view_role.save()
            create_role = Role()
            create_role.code = 'create'
            create_role.name = 'Create'
            create_role.description = 'Can create objects'
            create_role.save()
            update_role = Role()
            update_role.code = 'update'
            update_role.name = 'Update'
            update_role.description = 'Can update objects'
            update_role.save()
            delete_role = Role()
            delete_role.code = 'delete'
            delete_role.name = 'Delete'
            delete_role.description = 'Can delete objects'
            delete_role.save()
            system_role = Role()
            system_role.code = 'system'
            system_role.name = 'System'
            system_role.description = 'Can change system settings'
            system_role.save()

            # Attach groups to roles
            mapping = [
                (admin_group, [view_role, create_role, update_role, delete_role, system_role]),
                (user_group, [view_role, create_role, update_role, delete_role])
            ]
            for setting in mapping:
                for role in setting[1]:
                    rolegroup = RoleGroup()
                    rolegroup.group = setting[0]
                    rolegroup.role = role
                    rolegroup.save()

            # We're now at version 0.0.1
            working_version = 1

        # Version 0.0.2 introduced:
        if working_version < 2:
            # Execute some code that upgrades to version 2
            # working_version = 2
            pass

        return working_version

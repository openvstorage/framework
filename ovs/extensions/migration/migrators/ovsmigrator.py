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
OVS migration module
"""

import hashlib
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.group import Group
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.j_rolegroup import RoleGroup
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.branding import Branding


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version):
        """
        Migrates from any version to any version, running all migrations required
        If previous_version is for example 0 and this script is at
        verison 3 it will execute two steps:
          - 1 > 2
          - 2 > 3
        @param previous_version: The previous version from which to start the migration.
        """

        working_version = previous_version

        # Version 0.0.1 introduced:
        if working_version < 1:
            # Create groups
            admin_group = Group()
            admin_group.name = 'administrators'
            admin_group.description = 'Administrators'
            admin_group.save()
            viewers_group = Group()
            viewers_group.name = 'viewers'
            viewers_group.description = 'Viewers'
            viewers_group.save()

            # Create users
            admin = User()
            admin.username = 'admin'
            admin.password = hashlib.sha256('admin').hexdigest()
            admin.is_active = True
            admin.group = admin_group
            admin.save()

            # Create internal OAuth 2 clients
            admin_client = Client()
            admin_client.ovs_type = 'FRONTEND'
            admin_client.grant_type = 'PASSWORD'
            admin_client.user = admin
            admin_client.save()

            # Create roles
            read_role = Role()
            read_role.code = 'read'
            read_role.name = 'Read'
            read_role.description = 'Can read objects'
            read_role.save()
            write_role = Role()
            write_role.code = 'write'
            write_role.name = 'Write'
            write_role.description = 'Can write objects'
            write_role.save()
            manage_role = Role()
            manage_role.code = 'manage'
            manage_role.name = 'Manage'
            manage_role.description = 'Can manage the system'
            manage_role.save()

            # Attach groups to roles
            mapping = [
                (admin_group, [read_role, write_role, manage_role]),
                (viewers_group, [read_role])
            ]
            for setting in mapping:
                for role in setting[1]:
                    rolegroup = RoleGroup()
                    rolegroup.group = setting[0]
                    rolegroup.role = role
                    rolegroup.save()
                for user in setting[0].users:
                    for role in setting[1]:
                        roleclient = RoleClient()
                        roleclient.client = user.clients[0]
                        roleclient.role = role
                        roleclient.save()

            # Add backends
            for backend_type_info in [('Ceph', 'ceph_s3'), ('Amazon', 'amazon_s3'), ('Swift', 'swift_s3'),
                                      ('Local', 'local'), ('Distributed', 'distributed')]:
                backend_type = BackendType()
                backend_type.name = backend_type_info[0]
                backend_type.code = backend_type_info[1]
                backend_type.save()

            # Add service types
            for service_type_info in ['MetadataServer']:
                service_type = ServiceType()
                service_type.name = service_type_info
                service_type.save()

            # Brandings
            branding = Branding()
            branding.name = 'Default'
            branding.description = 'Default bootstrap theme'
            branding.css = 'bootstrap-default.min.css'
            branding.productname = 'Open vStorage'
            branding.is_default = True
            branding.save()
            slate = Branding()
            slate.name = 'Slate'
            slate.description = 'Dark bootstrap theme'
            slate.css = 'bootstrap-slate.min.css'
            slate.productname = 'Open vStorage'
            slate.is_default = False
            slate.save()

            # We're now at version 0.0.1
            working_version = 1

        # Version 0.0.2 introduced:
        if working_version < 2:
            # Execute some code that upgrades to version 2
            # working_version = 2
            pass

        return working_version

# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
OVS migration module
"""

import hashlib
import random
import string


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
        Migrates from a given version to the current version. It uses previous_version to be smart
        wherever possible, but the code should be able migrate any version towards the expected version.
        When this is not possible, the code can set a minimum version and raise when it is not met.
        :param previous_version: The previous version from which to start the migration.
        :type previous_version: float
        """

        working_version = previous_version

        if working_version == 0:
            # Initial version:
            # * Set the version to THIS RELEASE version
            #   Version  9: Fargo Alpha, Beta, RC
            #   Version 10: Fargo RTM

            from ovs.dal.hybrids.user import User
            from ovs.dal.hybrids.group import Group
            from ovs.dal.hybrids.role import Role
            from ovs.dal.hybrids.client import Client
            from ovs.dal.hybrids.j_rolegroup import RoleGroup
            from ovs.dal.hybrids.j_roleclient import RoleClient
            from ovs.dal.hybrids.backendtype import BackendType
            from ovs.dal.hybrids.servicetype import ServiceType
            from ovs.dal.hybrids.branding import Branding
            from ovs.dal.lists.backendtypelist import BackendTypeList

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
            admin_pw_client = Client()
            admin_pw_client.ovs_type = 'INTERNAL'
            admin_pw_client.grant_type = 'PASSWORD'
            admin_pw_client.user = admin
            admin_pw_client.save()
            admin_cc_client = Client()
            admin_cc_client.ovs_type = 'INTERNAL'
            admin_cc_client.grant_type = 'CLIENT_CREDENTIALS'
            admin_cc_client.client_secret = ''.join(random.choice(string.ascii_letters +
                                                                  string.digits +
                                                                  '|_=+*#@!/-[]{}<>.?,\'";:~')
                                                    for _ in range(128))
            admin_cc_client.user = admin
            admin_cc_client.save()

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
                        for client in user.clients:
                            roleclient = RoleClient()
                            roleclient.client = client
                            roleclient.role = role
                            roleclient.save()

            # Add backends
            for backend_type_info in [('Ceph', 'ceph_s3'), ('Amazon', 'amazon_s3'), ('Swift', 'swift_s3'),
                                      ('Local', 'local'), ('Distributed', 'distributed'), ('ALBA', 'alba')]:
                code = backend_type_info[1]
                backend_type = BackendTypeList.get_backend_type_by_code(code)
                if backend_type is None:
                    backend_type = BackendType()
                backend_type.name = backend_type_info[0]
                backend_type.code = code
                backend_type.save()

            # Add service types
            for service_type_info in [ServiceType.SERVICE_TYPES.MD_SERVER, ServiceType.SERVICE_TYPES.ALBA_PROXY, ServiceType.SERVICE_TYPES.ARAKOON]:
                service_type = ServiceType()
                service_type.name = service_type_info
                service_type.save()

            # Branding
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

            return 9

        # From here on, all actual migration should happen to get to the expected state for THIS RELEASE
        if working_version < 10:
            raise RuntimeError('Cannot upgrade to Fargo')

        return working_version

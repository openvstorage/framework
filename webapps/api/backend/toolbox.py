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
Contains various helping classes
"""

import re


class Toolbox(object):
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
    def is_client_in_roles(client, roles):
        """
        Checks whether a client is member of a set of roles
        """
        user_roles = [j.role.code for j in client.roles]
        for required_role in roles:
            if required_role not in user_roles:
                return False
        return True

    @staticmethod
    def is_token_in_roles(token, roles):
        """
        Checks whether a token is member of a set of roles
        """
        user_roles = [j.role.code for j in token.roles]
        for required_role in roles:
            if required_role not in user_roles:
                return False
        return True

    @staticmethod
    def compare(version_1, version_2):
        version_1 = [int(v) for v in version_1.split('.')]
        version_2 = [int(v) for v in version_2.split('.')]
        for i in xrange(max(len(version_1), len(version_2))):
            n_1 = 0
            n_2 = 0
            if i < len(version_1):
                n_1 = version_1[i]
            if i < len(version_2):
                n_2 = version_2[i]
            if n_1 != n_2:
                return n_1 - n_2
        return 0

    @staticmethod
    def access_granted(client, client_rights=None, user_rights=None):
        granted = False
        # Implicitly denied access
        if Toolbox.is_client_in_roles(client, ['manage']):
            # Implicitly granted access
            granted = True
        if user_rights is not None:
            for right in user_rights:
                if right.user_guid == client.user_guid:
                    if right.grant is False:
                        # Explicit deny
                        return False
                    granted = True
        if client.ovs_type != 'INTERNAL':
            if client_rights is not None:
                for right in client_rights:
                    if right.client_guid == client.guid:
                        if right.grant is False:
                            # Explicit deny
                            return False
                        granted = True
        return granted

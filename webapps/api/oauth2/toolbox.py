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
OAuth2Toolbox
"""
import time
import random
import string
from ovs.dal.hybrids.bearertoken import BearerToken
from ovs.dal.hybrids.j_rolebearertoken import RoleBearerToken


class OAuth2Toolbox(object):
    """
    OAuth2Toolbox
    """
    EXPIRATION_USER = 60 * 60 * 12
    EXPIRATION_CLIENT = 60 * 60

    @staticmethod
    def generate_tokens(client, generate_access=False, generic_refresh=False, scopes=None):
        """
        Generates tokens for a client with a specific scope (or default scope)
        """
        access_token = None
        refresh_token = None
        allowed_roles = [j.role for j in client.roles]
        roles = [s for s in scopes] if scopes is not None else allowed_roles
        if any(set(roles) - set(allowed_roles)):
            raise ValueError('invalid_scope')
        if generate_access is True:
            access_token = BearerToken()
            access_token.access_token = OAuth2Toolbox.create_hash(64)
            access_token.expiration = int(time.time() + 3600)
            access_token.client = client
            access_token.save()
            for role in roles:
                link = RoleBearerToken()
                link.role = role
                link.token = access_token
                link.save()
        if generic_refresh is True:
            refresh_token = BearerToken()
            refresh_token.refresh_token = OAuth2Toolbox.create_hash(128)
            refresh_token.expiration = int(time.time() + 86400)
            refresh_token.client = client
            refresh_token.save()
            for role in roles:
                link = RoleBearerToken()
                link.role = role
                link.token = refresh_token
                link.save()
        return access_token, refresh_token

    @staticmethod
    def clean_tokens(client):
        """
        Cleans expired tokens
        """
        for token in client.tokens:
            if token.expiration < time.time():
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()

    @staticmethod
    def create_hash(length):
        """
        Create a random hash of 'length' characters
        :param length: Length of the hash
        :type length: int
        :return: A hash of 'length' characters
        :rtype: str
        """
        return ''.join(random.choice(string.ascii_letters +
                                     string.digits +
                                     '|_=+*#@!/-[]{}<>.?,;:~')
                       for _ in range(length))

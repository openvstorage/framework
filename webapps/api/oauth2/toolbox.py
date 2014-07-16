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
Toolbox
"""
import time
import random
import string
from ovs.dal.hybrids.bearertoken import BearerToken
from ovs.dal.hybrids.j_rolebearertoken import RoleBearerToken


class Toolbox(object):
    """
    Toolbox
    """

    @staticmethod
    def generate_tokens(client, generate_access=False, generic_refresh=False, scopes=None):
        """
        Generates tokens for a client with a specific scope (or default scope)
        """
        access_token = None
        refresh_token = None
        allowed_scopes = [j.scope for j in client.scopes]
        if scopes is None:
            scopes = allowed_scopes
        elif any(set(scopes) - set(allowed_scopes)):
            raise ValueError('invalid_scope')
        if generate_access is True:
            access_token = BearerToken()
            access_token.access_token = Toolbox._create_hash(64)
            access_token.expiration = int(time.time() + 3600)
            access_token.client = client
            access_token.save()
            for scope in scopes:
                link = RoleBearerToken()
                link.scope = scope
                link.token = access_token
                link.save()
        if generic_refresh is True:
            refresh_token = BearerToken()
            refresh_token.refresh_token = Toolbox._create_hash(128)
            refresh_token.expiration = int(time.time() + 86400)
            refresh_token.client = client
            refresh_token.save()
            for scope in scopes:
                link = RoleBearerToken()
                link.scope = scope
                link.token = refresh_token
                link.save()
        return access_token, refresh_token

    @staticmethod
    def clean_tokens(client):
        """
        Cleans expired tokens
        """
        for token in client.tokens[:]:
            if token.expiration < time.time():
                for scope in token.scopes:
                    scope.delete()  # These are the junction objects
                token.delete()

    @staticmethod
    def _create_hash(length):
        return ''.join(random.choice(string.ascii_letters +
                                     string.digits +
                                     '|_=+*#@!/-[]{}<>.?,\'";:~')
                       for _ in range(length))

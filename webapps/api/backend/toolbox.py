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
    def is_client_in_roles(client, roles):
        """
        Checks whether a user is member of a set of roles
        """
        user_roles = [j.role.code for j in client.roles]
        for required_role in roles:
            if required_role not in user_roles:
                return False
        return True

    @staticmethod
    def extract_key(obj, key):
        """
        Extracts a sortable tuple from the object using the given keys
        """
        def clean_list(l):
            """
            Cleans a given tuple, removing empty elements, and convert to an integer where possible
            """
            while True:
                try:
                    l.remove('')
                except ValueError:
                    break
            for i in xrange(len(l)):
                try:
                    l[i] = int(l[i])
                except ValueError:
                    pass
            return l

        regex = re.compile(r'(\d+)')
        value = obj
        for subkey in key.split('.'):
            if '[' in subkey:
                # We're using a dict
                attribute = subkey.split('[')[0]
                dictkey = subkey.split('[')[1][:-1]
                value = getattr(value, attribute)[dictkey]
            else:
                # Normal property
                value = getattr(value, subkey)
            if value is None:
                break
        value = '' if value is None else str(value)
        sorting_key = tuple(clean_list(regex.split(value)))
        return sorting_key

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

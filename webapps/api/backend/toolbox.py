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
import math
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from ovs.dal.dataobjectlist import DataObjectList


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

    @staticmethod
    def extract_key(obj, keys):
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
        sorting_key = ()
        for key in keys:
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
            sorting_key += tuple(clean_list(regex.split(value)))
        return sorting_key

    @staticmethod
    def handle_list(dataobjectlist, request):
        """
        Processes/prepares a data object list based on request parameters.
        """
        # Sorting
        sort = request.QUERY_PARAMS.get('sort')
        if sort:
            desc = sort[0] == '-'
            sort = sort[1 if desc else 0:]
            dataobjectlist.sort(key=lambda e: Toolbox.extract_key(e, sort.split(',')), reverse=desc)
            # Paging
        page = request.QUERY_PARAMS.get('page')
        if page is not None and page.isdigit():
            page = int(page)
            max_page = int(math.ceil(len(dataobjectlist) / 10.0))
            if page > max_page:
                page = max_page
            page -= 1
            dataobjectlist = dataobjectlist[page * 10: (page + 1) * 10]
            # Preparing data
        full = request.QUERY_PARAMS.get('full')
        contents = request.QUERY_PARAMS.get('contents')
        contents = None if contents is None else contents.split(',')
        if full is not None:
            serializer = FullSerializer
        else:
            if isinstance(dataobjectlist, DataObjectList):
                dataobjectlist = dataobjectlist.reduced
            serializer = SimpleSerializer
        return dataobjectlist, serializer, contents

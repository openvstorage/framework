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
    def human_compare(object_a, object_b, keys):
        """
        Compares (sorts) two strings on a for humans logic way. E.g.
        ['x-10', 'x-1', 'x-2'] will become ['x-1', 'x-2', 'x-10']
        """
        key = keys[0]
        item_a = object_a
        item_b = object_b
        for subkey in key.split('.'):
            if '[' in subkey:
                attribute = subkey.split('[')[0]
                dictkey = subkey.split('[')[1][:-1]
                item_a = getattr(item_a, attribute)[dictkey]
                item_b = getattr(item_b, attribute)[dictkey]
            else:
                item_a = getattr(item_a, subkey)
                item_b = getattr(item_b, subkey)
            if item_a is None or item_b is None:
                break
        if item_a is None and item_b is not None:
            return -1
        if item_a is None and item_b is None:
            return 0
        if item_a is not None and item_b is None:
            return 1
        part_a = re.sub(r'\d', '', str(item_a))
        part_b = re.sub(r'\d', '', str(item_b))
        if part_a == part_b:
            part_a = int(re.sub(r'\D', '', str(item_a)))
            part_b = int(re.sub(r'\D', '', str(item_b)))
            if part_a == part_b:
                if len(keys) > 1:
                    keys.pop(0)
                    return Toolbox.human_compare(object_a, object_b, keys)
                else:
                    return 0
            return 1 if part_a > part_b else -1
        return 1 if part_a > part_b else -1

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
            dataobjectlist.sort(cmp=lambda a, b: Toolbox.human_compare(a, b, sort.split(',')), reverse=desc)
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
            dataobjectlist = dataobjectlist.reduced
            serializer = SimpleSerializer
        return dataobjectlist, serializer, contents

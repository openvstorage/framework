#!/usr/bin/python2
#  Copyright 2014 CloudFounders NV
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
Authentication test module
"""
import os
import sys
import json
import uuid
import time
import hashlib
from unittest import TestCase
from django.http import HttpResponse
from rest_framework.exceptions import Throttled
from ovs.extensions.generic import fakesleep
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.group import Group
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.j_rolegroup import RoleGroup
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.rolelist import RoleList
from oauth2.toolbox import Toolbox as OAuth2Toolbox
from tests.mockups import Serializers


class Decorators(TestCase):
    """
    The decorators test suite will validate all backend decorators
    """

    factory = None
    initial_data = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()
        VolatileFactory.store.clean()

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
        admin_npg = User()
        admin_npg.username = 'admin_npg'
        admin_npg.password = hashlib.sha256('admin_npg').hexdigest()
        admin_npg.is_active = True
        admin_npg.group = admin_group
        admin_npg.save()
        admin_na = User()
        admin_na.username = 'admin_na'
        admin_na.password = hashlib.sha256('admin_na').hexdigest()
        admin_na.is_active = False
        admin_na.group = admin_group
        admin_na.save()
        user = User()
        user.username = 'user'
        user.password = hashlib.sha256('user').hexdigest()
        user.is_active = True
        user.group = viewers_group
        user.save()
        sort_combinations = [('bb', 'aa'), ('aa', 'cc'), ('bb', 'dd'), ('aa', 'bb')]  # No logical ordering
        for username, password in sort_combinations:
            sort_user = User()
            sort_user.username = username
            sort_user.password = password
            sort_user.is_active = True
            sort_user.group = viewers_group
            sort_user.save()

        # Create internal OAuth 2 clients
        admin_client = Client()
        admin_client.ovs_type = 'FRONTEND'
        admin_client.grant_type = 'PASSWORD'
        admin_client.user = admin
        admin_client.save()
        admin_na_client = Client()
        admin_na_client.ovs_type = 'FRONTEND'
        admin_na_client.grant_type = 'PASSWORD'
        admin_na_client.user = admin_na
        admin_na_client.save()
        user_client = Client()
        user_client.ovs_type = 'FRONTEND'
        user_client.grant_type = 'PASSWORD'
        user_client.user = user
        user_client.save()

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

        Decorators.initial_data = PersistentFactory.store._read(), VolatileFactory.store._read()

        sys.modules['backend.serializers.serializers'] = Serializers

        sys.path.append('/opt/OpenvStorage')
        sys.path.append('/opt/OpenvStorage/webapps')
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
        from django.conf import settings
        settings.VERSION = (1, 2, 3)
        from django.test import RequestFactory
        Decorators.factory = RequestFactory()

        fakesleep.monkey_patch()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()

        PersistentFactory.store._save(Decorators.initial_data[0])
        VolatileFactory.store._save(Decorators.initial_data[1])

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        fakesleep.monkey_restore()

    def test_ratelimit(self):
        """
        Validates whether the rate limiter behaves correctly
        """
        from backend.decorators import limit

        @limit(amount=2, per=2, timeout=2)
        def the_function(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            output['value'] = input_value
            return HttpResponse(json.dumps(input_value))

        output = {'value': None}
        request = Decorators.factory.post('/users/')
        with self.assertRaises(KeyError):
            # Should raise a KeyError complaining about the HTTP_X_REAL_IP
            the_function(1, request)
        request.META['HTTP_X_REAL_IP'] = '127.0.0.1'
        response = the_function(2, request)
        self.assertEqual(output['value'], 2)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '2')
        response = the_function(3, request)
        self.assertEqual(output['value'], 3)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '3')
        with self.assertRaises(Throttled) as context:
            the_function(4, request)
        self.assertEqual(context.exception.status_code, 429)
        self.assertEqual(output['value'], 3)
        with self.assertRaises(Throttled) as context:
            the_function(4, request)
        self.assertEqual(context.exception.status_code, 429)
        self.assertEqual(output['value'], 3)
        time.sleep(5)
        response = the_function(6, request)
        self.assertEqual(output['value'], 6)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '6')

    def test_required_roles(self):
        """
        Validates whether the required_roles decorator works
        """
        from backend.decorators import required_roles
        from rest_framework.exceptions import NotAuthenticated, PermissionDenied

        @required_roles(['read', 'write', 'manage'])
        def the_function(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            output['value'] = input_value
            return HttpResponse(json.dumps(input_value))

        output = {'value': None}
        user = UserList.get_user_by_username('user')
        request = Decorators.factory.get('/')
        with self.assertRaises(NotAuthenticated) as context:
            the_function(1, request)
        self.assertEqual(context.exception.status_code, 401)
        request.client = type('Client', (), {})
        request.user = type('User', (), {})
        request.user.username = 'foobar'
        with self.assertRaises(NotAuthenticated) as context:
            the_function(2, request)
        self.assertEqual(context.exception.status_code, 401)
        access_token, _ = OAuth2Toolbox.generate_tokens(user.clients[0], generate_access=True, scopes=RoleList.get_roles_by_codes(['read']))
        access_token.expiration = int(time.time() + 86400)
        access_token.save()
        request.user.username = 'user'
        request.token = access_token
        with self.assertRaises(PermissionDenied) as context:
            the_function(3, request)
        self.assertEqual(context.exception.status_code, 403)
        self.assertEqual(context.exception.detail, 'This call requires roles: read, write, manage')
        user = UserList.get_user_by_username('admin')
        access_token, _ = OAuth2Toolbox.generate_tokens(user.clients[0], generate_access=True, scopes=RoleList.get_roles_by_codes(['read', 'write', 'manage']))
        access_token.expiration = int(time.time() + 86400)
        access_token.save()
        request.username = 'admin'
        request.token = access_token
        response = the_function(4, request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '4')

    def test_load(self):
        """
        Validates whether the load decorator works
        """
        from backend.decorators import load
        from rest_framework.exceptions import NotAcceptable
        from django.http import Http404

        @load(User, min_version=2, max_version=2)
        def the_function_1(input_value, request, user, version, mandatory, optional='default'):
            """
            Decorated function
            """
            output['value'] = {'request': request,
                               'mandatory': mandatory,
                               'optional': optional,
                               'version': version,
                               'user': user}
            return HttpResponse(json.dumps(input_value))

        @load(User)
        def the_function_2(input_value, request, user, pk, version):
            """
            Decorated function
            """
            output['value'] = {'request': request,
                               'user': user,
                               'pk': pk,
                               'version': version}
            return HttpResponse(json.dumps(input_value))

        output = {'value': None}
        user = UserList.get_user_by_username('user')
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        with self.assertRaises(NotAcceptable) as context:
            the_function_1(1, request)
        self.assertEqual(context.exception.status_code, 406)
        self.assertEqual(context.exception.detail, 'API version requirements: {0} <= <version> <= {1}. Got {2}'.format(2, 2, 1))
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        with self.assertRaises(Http404):
            the_function_1(2, request, pk=str(uuid.uuid4()))
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {}
        with self.assertRaises(NotAcceptable) as context:
            the_function_1(3, request, pk=user.guid)
        self.assertEqual(context.exception.status_code, 406)
        self.assertEqual(context.exception.detail, 'Invalid data passed: mandatory is missing')
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {'mandatory': 'mandatory'}
        request.QUERY_PARAMS = {}
        response = the_function_1(4, request, pk=user.guid)
        self.assertEqual(response.status_code, 200)
        self.assertDictContainsSubset({'mandatory': 'mandatory',
                                       'optional': 'default',
                                       'user': user}, output['value'])
        self.assertIn('request', output['value'].keys())
        self.assertEqual(json.loads(response.content), 4)
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {'mandatory': 'mandatory',
                                'optional': 'optional'}
        response = the_function_1(5, request, pk=user.guid)
        self.assertEqual(response.status_code, 200)
        self.assertDictContainsSubset({'mandatory': 'mandatory',
                                       'optional': 'optional',
                                       'version': 2,
                                       'user': user}, output['value'])
        self.assertIn('request', output['value'].keys())
        self.assertEqual(json.loads(response.content), 5)
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {'mandatory': 'mandatory',
                                'optional': 'optional'}
        response = the_function_2(6, request, pk=user.guid)
        self.assertEqual(response.status_code, 200)
        self.assertDictContainsSubset({'pk': user.guid,
                                       'version': 3,
                                       'user': user}, output['value'])
        self.assertIn('request', output['value'].keys())
        self.assertEqual(json.loads(response.content), 6)

    def test_return_task(self):
        """
        Validates whether the return_task decorator will return a task ID
        """
        from backend.decorators import return_task

        @return_task()
        def the_function(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            return type('Task', (), {'id': input_value})

        response = the_function(1)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, 1)

    def test_return_object(self):
        """
        Validates whether the return_object decorator works:
        * Parses the 'contents' parameter, and passes it into the serializer
        """
        from backend.decorators import return_object

        @return_object(User)
        def the_function(input_value, *args, **kwargs):
            """
            Return a fake User object that would be serialized
            """
            _ = args, kwargs
            return type('User', (), {'input_value': input_value})

        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        request.QUERY_PARAMS = {}
        response = the_function(1, request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['instance'].input_value, 1)
        self.assertIsNone(response.data['contents'])
        request.QUERY_PARAMS['contents'] = 'foo,bar'
        response = the_function(2, request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['instance'].input_value, 2)
        self.assertEqual(response.data['contents'], ['foo', 'bar'])

    def test_return_list(self):
        """
        Validates whether the return_list decorator works correctly:
        * Parsing:
          * Parses the 'sort' parameter, optionally falling back to value specified by decorator
          * Parses the 'page' parameter
          * Parses the 'contents' parameter
        * Passes the 'full' hint to the decorated function, indicating whether full objects are usefull
        * If sorting is requested:
          * Loads a possibly returned list of guids
          * Sorts the returned list
        * Contents:
          * If contents are specified: Runs the list trough the serializer
          * Else, return the guid list
        """
        from backend.decorators import return_list
        from ovs.dal.datalist import DataList
        from ovs.dal.dataobjectlist import DataObjectList

        @return_list(User)
        def the_function_1(*args, **kwargs):
            """
            Returns a list of all Users.
            """
            output_values['args'] = args
            output_values['kwargs'] = kwargs
            return data_list_users

        @return_list(User, default_sort='username,password')
        def the_function_2(*args, **kwargs):
            """
            Returns a guid list of all Users.
            """
            output_values['args'] = args
            output_values['kwargs'] = kwargs
            return data_list_userguids

        # Username/password combinations: [('bb', 'aa'), ('aa', 'cc'), ('bb', 'dd'), ('aa', 'bb')]
        output_values = {}
        users = DataList({'object': User,
                          'data': DataList.select.GUIDS,
                          'query': {'type': DataList.where_operator.OR,
                                    'items': [('username', DataList.operator.EQUALS, 'aa'),
                                              ('username', DataList.operator.EQUALS, 'bb')]}}).data
        data_list_users = DataObjectList(users, User)
        self.assertEqual(len(data_list_users), 4)
        guid_table = {}
        for user in data_list_users:
            if user.username not in guid_table:
                guid_table[user.username] = {}
            guid_table[user.username][user.password] = user.guid
        data_list_userguids = [user.guid for user in data_list_users]
        request = Decorators.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        for function in [the_function_1, the_function_2]:
            request.QUERY_PARAMS = {}
            response = function(1, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], function.__name__ == 'the_function_2')
            self.assertEqual(len(response.data), len(data_list_users))
            if function.__name__ == 'the_function_2':
                self.assertListEqual(response.data, [guid_table['aa']['bb'],
                                                     guid_table['aa']['cc'],
                                                     guid_table['bb']['aa'],
                                                     guid_table['bb']['dd']])
            request.QUERY_PARAMS['sort'] = 'username,-password'
            response = function(2, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data), len(data_list_users))
            self.assertListEqual(response.data, [guid_table['aa']['cc'],
                                                 guid_table['aa']['bb'],
                                                 guid_table['bb']['dd'],
                                                 guid_table['bb']['aa']])
            request.QUERY_PARAMS['sort'] = '-username,-password'
            response = function(3, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data), len(data_list_users))
            self.assertListEqual(response.data, [guid_table['bb']['dd'],
                                                 guid_table['bb']['aa'],
                                                 guid_table['aa']['cc'],
                                                 guid_table['aa']['bb']])
            request.QUERY_PARAMS['sort'] = 'password,username'
            response = function(4, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data), len(data_list_users))
            self.assertListEqual(response.data, [guid_table['bb']['aa'],
                                                 guid_table['aa']['bb'],
                                                 guid_table['aa']['cc'],
                                                 guid_table['bb']['dd']])
            request.QUERY_PARAMS['contents'] = ''
            response = function(5, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data), len(data_list_users))
            if function.__name__ == 'the_function_1':
                self.assertIsInstance(response.data['instance'], DataObjectList)
                self.assertIsInstance(response.data['instance'][0], User)
                self.assertIn(response.data['instance'][0].username, ['aa', 'bb'])
            else:
                self.assertIsInstance(response.data['instance'], list)
                self.assertIsInstance(response.data['instance'][0], User)
                self.assertIn(response.data['instance'][0].username, ['aa', 'bb'])

if __name__ == '__main__':
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Decorators)
    unittest.TextTestRunner(verbosity=2).run(suite)

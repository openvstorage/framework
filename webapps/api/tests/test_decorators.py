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
Authentication test module
"""
import json
import time
import uuid
import hashlib
import unittest
from django.conf import settings
from django.http import HttpResponse
from django.test import RequestFactory
from api.backend.decorators import limit, required_roles, return_list, return_object, return_task
# noinspection PyUnresolvedReferences
from api.backend.toolbox import ApiToolbox  # Required for the tests
from api.oauth2.toolbox import OAuth2Toolbox
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.group import Group
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.hybrids.j_rolegroup import RoleGroup
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.hybrids.user import User
from ovs.dal.lists.rolelist import RoleList
from ovs.dal.lists.userlist import UserList
from ovs.dal.tests.helpers import DalHelper
from ovs_extensions.api.exceptions import \
    HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException,\
    HttpTooManyRequestsException, HttpUnauthorizedException, HttpUpgradeNeededException


class Decorators(unittest.TestCase):
    """
    The decorators test suite will validate all backend decorators
    """
    original_versions = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        DalHelper.setup(fake_sleep=True)

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
        for name, description in sort_combinations:
            machine = TestMachine()
            machine.name = name
            machine.description = description
            machine.save()

        # Create internal OAuth 2 clients
        admin_client = Client()
        admin_client.ovs_type = 'INTERNAL'
        admin_client.grant_type = 'PASSWORD'
        admin_client.user = admin
        admin_client.save()
        admin_na_client = Client()
        admin_na_client.ovs_type = 'INTERNAL'
        admin_na_client.grant_type = 'PASSWORD'
        admin_na_client.user = admin_na
        admin_na_client.save()
        user_client = Client()
        user_client.ovs_type = 'INTERNAL'
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

        cls.original_versions = settings.VERSION
        settings.VERSION = (1, 2, 3)
        cls.factory = RequestFactory()

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        DalHelper.teardown(fake_sleep=True)
        settings.VERSION = cls.original_versions

    def test_ratelimit(self):
        """
        Validates whether the rate limiter behaves correctly
        """
        @limit(amount=2, per=2, timeout=2)
        def the_function(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            output['value'] = input_value
            return HttpResponse(json.dumps(input_value))

        output = {'value': None}
        request = self.factory.post('/users/')
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
        with self.assertRaises(HttpTooManyRequestsException) as context:
            the_function(4, request)
        self.assertEqual(context.exception.status_code, 429)
        self.assertEqual(output['value'], 3)
        with self.assertRaises(HttpTooManyRequestsException) as context:
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
        @required_roles(['read', 'write', 'manage'])
        def the_function_rr(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            output['value'] = input_value
            return HttpResponse(json.dumps(input_value))

        time.sleep(180)
        output = {'value': None}
        request = self.factory.get('/')
        with self.assertRaises(HttpUnauthorizedException) as context:
            the_function_rr(1, request)
        self.assertEqual(context.exception.status_code, 401)

        time.sleep(180)
        request.client = type('Client', (), {})
        request.user = type('User', (), {})
        request.user.username = 'foobar'
        with self.assertRaises(HttpUnauthorizedException) as context:
            the_function_rr(2, request)
        self.assertEqual(context.exception.status_code, 401)

        time.sleep(180)
        user = UserList.get_user_by_username('user')
        access_token, _ = OAuth2Toolbox.generate_tokens(user.clients[0], generate_access=True, scopes=RoleList.get_roles_by_codes(['read']))
        access_token.expiration = int(time.time() + 86400)
        access_token.save()
        request.user.username = 'user'
        request.token = access_token
        with self.assertRaises(HttpForbiddenException) as context:
            the_function_rr(3, request)
        self.assertEqual(context.exception.status_code, 403)
        self.assertEqual(context.exception.error, 'invalid_roles')
        self.assertEqual(context.exception.error_description, 'This call requires roles: read, write, manage')

        time.sleep(180)
        user = UserList.get_user_by_username('admin')
        access_token, _ = OAuth2Toolbox.generate_tokens(user.clients[0], generate_access=True, scopes=RoleList.get_roles_by_codes(['read', 'write', 'manage']))
        access_token.expiration = int(time.time() + 86400)
        access_token.save()
        request.username = 'admin'
        request.token = access_token
        response = the_function_rr(4, request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '4')

    def test_load(self):
        """
        Validates whether the load decorator works
        """
        from api.backend import decorators
        # Reload this module, because the 'load' decorator gets loaded with default min_version and max_version of 6 and 9 (currently) and in this test we overrule these versions with 1 and 3
        reload(decorators)

        @decorators.load(User, min_version=2, max_version=2)
        def the_function_tl_1(input_value, request, user, version, mandatory, optional='default'):
            """
            Decorated function
            """
            output['value'] = {'request': request,
                               'mandatory': mandatory,
                               'optional': optional,
                               'version': version,
                               'user': user}
            return HttpResponse(json.dumps(input_value))

        @decorators.load(User)
        def the_function_tl_2(input_value, request, user, pk, version):
            """
            Decorated function
            """
            output['value'] = {'request': request,
                               'user': user,
                               'pk': pk,
                               'version': version}
            return HttpResponse(json.dumps(input_value))

        time.sleep(180)
        output = {'value': None}
        user = UserList.get_user_by_username('user')
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        with self.assertRaises(HttpUpgradeNeededException) as context:
            the_function_tl_1(1, request)
        self.assertEqual(context.exception.status_code, 426)
        self.assertEqual(context.exception.error, 'invalid_version')
        self.assertEqual(context.exception.error_description, 'API version requirements: {0} <= <version> <= {1}. Got {2}'.format(2, 2, 1))

        time.sleep(180)
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        with self.assertRaises(HttpNotFoundException):
            the_function_tl_1(2, request, pk=str(uuid.uuid4()))

        time.sleep(180)
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {}
        with self.assertRaises(HttpNotAcceptableException) as context:
            the_function_tl_1(3, request, pk=user.guid)
        self.assertEqual(context.exception.status_code, 406)
        self.assertEqual(context.exception.error, 'invalid_data')
        self.assertEqual(context.exception.error_description, 'Invalid data passed: mandatory is missing')

        time.sleep(180)
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {'mandatory': 'mandatory'}
        request.QUERY_PARAMS = {}
        response = the_function_tl_1(4, request, pk=user.guid)
        self.assertEqual(response.status_code, 200)
        self.assertDictContainsSubset({'mandatory': 'mandatory',
                                       'optional': 'default',
                                       'user': user}, output['value'])
        self.assertIn('request', output['value'].keys())
        self.assertEqual(json.loads(response.content), 4)

        time.sleep(180)
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {'mandatory': 'mandatory',
                                'optional': 'optional'}
        response = the_function_tl_1(5, request, pk=user.guid)
        self.assertEqual(response.status_code, 200)
        self.assertDictContainsSubset({'mandatory': 'mandatory',
                                       'optional': 'optional',
                                       'version': 2,
                                       'user': user}, output['value'])
        self.assertIn('request', output['value'].keys())
        self.assertEqual(json.loads(response.content), 5)

        time.sleep(180)
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {'mandatory': 'mandatory',
                                'optional': 'optional'}
        response = the_function_tl_2(6, request, pk=user.guid)
        self.assertEqual(response.status_code, 200)
        self.assertDictContainsSubset({'pk': user.guid,
                                       'version': 3,
                                       'user': user}, output['value'])
        self.assertIn('request', output['value'].keys())
        self.assertEqual(json.loads(response.content), 6)

    # noinspection PyUnresolvedReferences
    def test_return_task(self):
        """
        Validates whether the return_task decorator will return a task ID
        """
        @return_task()
        def the_function_rt(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            return type('Task', (), {'id': input_value})

        response = the_function_rt(1)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, 1)

    # noinspection PyUnresolvedReferences
    def test_return_object(self):
        """
        Validates whether the return_object decorator works:
        * Parses the 'contents' parameter, and passes it into the serializer
        """
        @return_object(User)
        def the_function_ro(input_value, *args, **kwargs):
            """
            Return a fake User object that would be serialized
            """
            _ = args, kwargs
            user = User()
            user.username = input_value
            return user

        time.sleep(180)
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        request.QUERY_PARAMS = {}
        response = the_function_ro('a', request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['instance'].username, 'a')
        self.assertIsNone(response.data['contents'])
        request.QUERY_PARAMS['contents'] = 'foo,bar'
        response = the_function_ro('b', request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['instance'].username, 'b')
        self.assertEqual(response.data['contents'], ['foo', 'bar'])

    def test_return_list(self):
        """
        Validates whether the return_list decorator works correctly:
        * Parsing:
          * Parses the 'sort' parameter, optionally falling back to value specified by decorator
          * Parses the 'page' parameter
          * Parses the 'contents' parameter
        * Passes the 'full' hint to the decorated function, indicating whether full objects are useful
        * If sorting is requested:
          * Loads a possibly returned list of guids
          * Sorts the returned list
        * Contents:
          * If contents are specified: Runs the list trough the serializer
          * Else, return the guid list
        """
        @return_list(TestMachine)
        def the_function_rl_1(*args, **kwargs):
            """
            Returns a list of all Machines.
            """
            output_values['args'] = args
            output_values['kwargs'] = kwargs
            return data_list_machines

        @return_list(TestMachine, default_sort='name,description')
        def the_function_rl_2(*args, **kwargs):
            """
            Returns a guid list of all Machines.
            """
            output_values['args'] = args
            output_values['kwargs'] = kwargs
            return data_list_machineguids

        @return_list(TestMachine)
        def the_function_rl_3(*args, **kwargs):
            """
            Returns only the first two Machines of the list of all Machines
            """
            output_values['args'] = args
            output_values['kwargs'] = kwargs
            return DataList(TestMachine, guids=[guid_table['bb']['aa'], guid_table['aa']['cc']])

        @return_list(TestMachine)
        def the_function_rl_4(*args, **kwargs):
            """
            Returns only the first two guids of Machines of the list of all Machines
            """
            output_values['args'] = args
            output_values['kwargs'] = kwargs
            return [guid_table['bb']['aa'], guid_table['aa']['cc']]

        # Name/description combinations: [('bb', 'aa'), ('aa', 'cc'), ('bb', 'dd'), ('aa', 'bb')]
        output_values = {}
        data_list_machines = DataList(TestMachine, {'type': DataList.where_operator.OR,
                                                    'items': [('name', DataList.operator.EQUALS, 'aa'),
                                                              ('name', DataList.operator.EQUALS, 'bb')]})
        self.assertEqual(len(data_list_machines), 4)
        guid_table = {}
        for machine in data_list_machines:
            if machine.name not in guid_table:
                guid_table[machine.name] = {}
            guid_table[machine.name][machine.description] = machine.guid
        data_list_machineguids = [machine.guid for machine in data_list_machines]

        time.sleep(180)

        # Test sorting
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        for fct in [the_function_rl_1, the_function_rl_2]:
            request.QUERY_PARAMS = {}
            response = fct(1, request)
            self.assertEqual(response.status_code, 200)
            # Hinting is applied when requesting/applying sort
            self.assertEqual(output_values['kwargs']['hints']['full'], fct.__name__ == 'the_function_rl_2')
            self.assertEqual(len(response.data), len(data_list_machines))
            if fct.__name__ == 'the_function_rl_2':
                self.assertListEqual(response.data['data'], [guid_table['aa']['bb'],
                                                             guid_table['aa']['cc'],
                                                             guid_table['bb']['aa'],
                                                             guid_table['bb']['dd']])

            request.QUERY_PARAMS['sort'] = 'name,-description'
            response = fct(2, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            self.assertListEqual(response.data['data'], [guid_table['aa']['cc'],
                                                         guid_table['aa']['bb'],
                                                         guid_table['bb']['dd'],
                                                         guid_table['bb']['aa']])

            request.QUERY_PARAMS['sort'] = '-name,-description'
            response = fct(3, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            self.assertListEqual(response.data['data'], [guid_table['bb']['dd'],
                                                         guid_table['bb']['aa'],
                                                         guid_table['aa']['cc'],
                                                         guid_table['aa']['bb']])

            request.QUERY_PARAMS['sort'] = 'description,name'
            response = fct(4, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            self.assertListEqual(response.data['data'], [guid_table['bb']['aa'],
                                                         guid_table['aa']['bb'],
                                                         guid_table['aa']['cc'],
                                                         guid_table['bb']['dd']])

            request.QUERY_PARAMS['contents'] = ''
            response = fct(5, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            # Contents requested so data is fully serialized
            # Change with everything being offloaded to DataList makes sure that the instance that is serialized is always a DataList
            self.assertIsInstance(response.data['data']['instance'], DataList)
            self.assertIsInstance(response.data['data']['instance'][0], TestMachine)
            self.assertIn(response.data['data']['instance'][0].name, ['aa', 'bb'])

        # Test filtering
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        for fct in [the_function_rl_1, the_function_rl_2, the_function_rl_3, the_function_rl_4]:
            request.QUERY_PARAMS = {}
            # Test querying, not to be tested thoroughly (test_basic handles DataList queries)
            request.QUERY_PARAMS['query'] = json.dumps({'type': 'AND',
                                                        'items': [['description', 'EQUALS', 'aa']]})
            response = fct(1, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], fct.__name__ == 'the_function_rl_2')
            expected_items = [guid_table['bb']['aa']]
            self.assertEqual(len(response.data['data']), len(expected_items))
            self.assertListEqual(response.data['data'], expected_items)

            request.QUERY_PARAMS['query'] = json.dumps({'type': 'AND',
                                                        'items': [['description', 'EQUALS', 'dd']]})
            response = fct(2, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], fct.__name__ == 'the_function_rl_2')
            if fct.__name__ in ['the_function_rl_1', 'the_function_rl_2']:
                expected_items = [guid_table['bb']['dd']]
            else:
                expected_items = []  # Not found in the first two items
            self.assertEqual(len(response.data['data']), len(expected_items))
            self.assertListEqual(response.data['data'], expected_items)

            request.QUERY_PARAMS['query'] = json.dumps('rawr')
            with self.assertRaises(ValueError):
                # Can't capture the response as the exception will be raised in the same context
                response = fct(1, request)

        # Test pagination
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        for fct in [the_function_rl_1, the_function_rl_2, the_function_rl_3, the_function_rl_4]:
            request.QUERY_PARAMS = {}
            for arg_type in [int, str]:
                request.QUERY_PARAMS['page'] = 1 if arg_type == int else '1'
                request.QUERY_PARAMS['page_size'] = 2 if arg_type == int else '2'
                response = fct(3, request)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(output_values['kwargs']['hints']['full'], fct.__name__ == 'the_function_rl_2')
                if fct.__name__ == 'the_function_rl_2':
                    expected_items = [guid_table['aa']['bb'], guid_table['aa']['cc']]
                elif fct.__name__ in ['the_function_rl_3', 'the_function_rl_4']:
                    expected_items = [guid_table['bb']['aa'], guid_table['aa']['cc']]

                else:
                    expected_items = [machine.guid for machine in data_list_machines][0:2]
                self.assertEqual(len(response.data['data']), len(expected_items))
                self.assertListEqual(response.data['data'], expected_items)

                request.QUERY_PARAMS['page'] = 2
                request.QUERY_PARAMS['page_size'] = 2
                response = fct(4, request)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(output_values['kwargs']['hints']['full'], fct.__name__ == 'the_function_rl_2')
                if fct.__name__ == 'the_function_rl_2':
                    expected_items = [guid_table['bb']['aa'], guid_table['bb']['dd']]
                elif fct.__name__ in ['the_function_rl_3', 'the_function_rl_4']:
                    # Same items as page 1 because only 2 items in total and when calling a page higher than max,
                    #  it will go back to the result for the max page
                    expected_items = [guid_table['bb']['aa'], guid_table['aa']['cc']]
                else:
                    expected_items = [machine.guid for machine in data_list_machines][2:4]
                self.assertEqual(len(response.data['data']), len(expected_items))
                self.assertListEqual(response.data['data'], expected_items)

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
from functools import wraps
from django.conf import settings
from django.http import HttpResponse
from django.test import RequestFactory
from api.backend.decorators import limit, required_roles, return_list, return_object, return_task, RateLimiter
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
from ovs.dal.lists.grouplist import GroupList
from ovs.dal.lists.rolelist import RoleList
from ovs.dal.lists.userlist import UserList
from ovs.dal.tests.helpers import DalHelper
from ovs_extensions.api.exceptions import \
    HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException,\
    HttpTooManyRequestsException, HttpUnauthorizedException, HttpUpgradeNeededException


def data_holder_return_list(default_sort=None):
    """
    Wraps around the return_list decorator to access the DataHolder props
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        @wraps(f)
        def new_function(self, *args, **kw):
            # type: (DataHolder, *any, **any) -> any
            """
            Executes the decorated function in a locked context
            Kwargs get mutated. The hinting metadata is added!
            """
            return return_list(self.data_type, default_sort=default_sort)(f)(self, *args, **kw)
        return new_function
    return wrap


class DataHolder(object):
    """
    Simple data holder class
    Exposes methods to retrieve the data as the API would
    """
    def __init__(self, base_list, ordered_set=None):
        # type: (DataList, DataObject, Optional[List[any]]) -> None
        """
        Instantiate a data holder. It requires the data_type and a list to base the data returns off
        :param base_list: The DataList to base returns off
        :type base_list: DataList
        :param ordered_set: Intended ordered set. Used for returning a set amount of items in a particular order
        Defaults to the base_list
        :type ordered_set: Optional[List[any]]
        """
        self.base_list = base_list
        self.base_list_guids = base_list.guids[:]
        self.data_type = base_list._object_type
        self.ordered_set = ordered_set or base_list

        self.rate_limit_output = None
        self.output_values = {}

    @data_holder_return_list()
    def get_base_list(self, *args, **kwargs):
        """
        Returns a list of all Machines.
        """
        self.output_values['args'] = args
        self.output_values['kwargs'] = kwargs
        return self.base_list

    @data_holder_return_list()
    def get_base_list_guids(self, *args, **kwargs):
        """
        Returns a list of all Machines.
        """
        self.output_values['args'] = args
        self.output_values['kwargs'] = kwargs
        return self.base_list_guids

    @data_holder_return_list(default_sort='name,description')
    def get_base_list_guids_default_sorted(self, *args, **kwargs):
        """
        Returns a guid list of all Machines.
        """
        self.output_values['args'] = args
        self.output_values['kwargs'] = kwargs
        return self.base_list_guids

    @data_holder_return_list()
    def get_base_list_first_two(self, *args, **kwargs):
        """
        Returns only the first two Machines of the list of all Machines
        """
        self.output_values['args'] = args
        self.output_values['kwargs'] = kwargs
        return DataList(TestMachine, guids=[item.guid for item in self.ordered_set[:2]])

    @data_holder_return_list()
    def get_base_list_guids_first_two(self, *args, **kwargs):
        """
        Returns only the first two guids of Machines of the list of all Machines
        """
        self.output_values['args'] = args
        self.output_values['kwargs'] = kwargs
        return [item.guid for item in self.ordered_set[:2]]

    @limit(amount=2, per=2, timeout=2)
    def rate_limited_function(self, input_value, *args, **kwargs):
        """
        Decorated function
        """
        _ = args, kwargs
        self.rate_limit_output = input_value
        return HttpResponse(json.dumps(input_value))

    @staticmethod
    def get_admin_group():
        return next(group for group in GroupList.get_groups() if group.name == 'administrators')

    @staticmethod
    @return_list(User)
    def get_users_of_admin_group(*args, **kwargs):
        """
        Get the users of the admin group
        """
        admin_group = DataHolder.get_admin_group()
        return admin_group.users


class Decorators(unittest.TestCase):
    """
    The decorators test suite will validate all backend decorators
    """

    @staticmethod
    def set_up_api():
        """
        Setup for API mocking
        - Create group
        - Create users
        - Add OAuth clients
        - Add roles
        Requires the DAL to be setup!
        """
        # Admin user/group
        admin_group = Group()
        admin_group.name = 'administrators'
        admin_group.description = 'Administrators'
        admin_group.save()

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

        # Viewer user/group
        viewers_group = Group()
        viewers_group.name = 'viewers'
        viewers_group.description = 'Viewers'
        viewers_group.save()

        user = User()
        user.username = 'user'
        user.password = hashlib.sha256('user').hexdigest()
        user.is_active = True
        user.group = viewers_group
        user.save()

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
        mapping = [(admin_group, [read_role, write_role, manage_role]), (viewers_group, [read_role])]
        for group, roles in mapping:
            for role in roles:
                rolegroup = RoleGroup()
                rolegroup.group = group
                rolegroup.role = role
                rolegroup.save()
            for user in group.users:
                for role in roles:
                    for client in user.clients:
                        roleclient = RoleClient()
                        roleclient.client = client
                        roleclient.role = role
                        roleclient.save()

    def setUp(self):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        DalHelper.setup(fake_sleep=True)

        self.set_up_api()

        # No logical ordering for testing purposes
        machine_description_combinations = [('bb', 'aa'), ('aa', 'cc'), ('bb', 'dd'), ('aa', 'bb')]
        self.machines_by_name_description = {}
        self.machines_random_order = []
        for name, description in machine_description_combinations:
            machine = TestMachine()
            machine.name = name
            machine.description = description
            machine.save()
            self.machines_random_order.append(machine)
            if name not in self.machines_by_name_description:
                self.machines_by_name_description[name] = {}
            self.machines_by_name_description[name][description] = machine

        self.data_list_machines = DataList(TestMachine, {'type': DataList.where_operator.OR,
                                                         'items': [('name', DataList.operator.EQUALS, 'aa'),
                                                                   ('name', DataList.operator.EQUALS, 'bb')]})
        self.assertEqual(len(self.data_list_machines), 4)
        self.data_holder = DataHolder(self.data_list_machines, self.machines_random_order)

        self.original_versions = settings.VERSION
        settings.VERSION = (1, 2, 3)
        self.factory = RequestFactory()

    def tearDown(self):
        """
        Clean up the unittest
        """
        DalHelper.teardown(fake_sleep=True)
        settings.VERSION = self.original_versions

    def test_ratelimit(self):
        """
        Validates whether the rate limiter behaves correctly
        """
        request = self.factory.post('/users/')

        with self.assertRaises(KeyError):
            # Should raise a KeyError complaining about the HTTP_X_REAL_IP
            self.data_holder.rate_limited_function(1, request)

        request.META['HTTP_X_REAL_IP'] = '127.0.0.1'
        response = self.data_holder.rate_limited_function(2, request)
        self.assertEqual(self.data_holder.rate_limit_output, 2)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '2')

        response = self.data_holder.rate_limited_function(3, request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '3')

        for _ in xrange(0, 2):
            with self.assertRaises(HttpTooManyRequestsException) as context:
                self.data_holder.rate_limited_function(4, request)
            self.assertEqual(context.exception.status_code, 429)
            self.assertEqual(self.data_holder.rate_limit_output, 3,
                             'Decorated function shouldn\'t be called as the cooldown is still happening')

        # Simulate a wait period by clearing all calls
        rate_limit_info = RateLimiter.get_rate_limit_info(request, self.data_holder.rate_limited_function)
        rate_limit_info.calls = rate_limit_info.get_calls(time.time() + 5)  # Warp to the future (for calls, not cooldown)!
        rate_limit_info.save()
        with self.assertRaises(HttpTooManyRequestsException) as context:
            self.data_holder.rate_limited_function(5, request)
        self.assertEqual(context.exception.status_code, 429)
        self.assertEqual(self.data_holder.rate_limit_output, 3,
                         'Decorated function shouldn\'t be called as the cooldown is still happening because the timeout is still active')

        # Clearing the timeout
        rate_limit_info.clear_timeout()  # Clear the cooldown
        rate_limit_info.save()

        response = self.data_holder.rate_limited_function(6, request)
        self.assertEqual(self.data_holder.rate_limit_output, 6)
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

        output = {'value': None}
        request = self.factory.get('/')
        with self.assertRaises(HttpUnauthorizedException) as context:
            the_function_rr(1, request)
        self.assertEqual(context.exception.status_code, 401)

        request.client = type('Client', (), {})
        request.user = type('User', (), {})
        request.user.username = 'foobar'
        with self.assertRaises(HttpUnauthorizedException) as context:
            the_function_rr(2, request)
        self.assertEqual(context.exception.status_code, 401)

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

        output = {'value': None}
        user = UserList.get_user_by_username('user')
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        with self.assertRaises(HttpUpgradeNeededException) as context:
            the_function_tl_1(1, request)
        self.assertEqual(context.exception.status_code, 426)
        self.assertEqual(context.exception.error, 'invalid_version')
        self.assertEqual(context.exception.error_description, 'API version requirements: {0} <= <version> <= {1}. Got {2}'.format(2, 2, 1))

        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        with self.assertRaises(HttpNotFoundException):
            the_function_tl_1(2, request, pk=str(uuid.uuid4()))

        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=*')
        request.DATA = {}
        request.QUERY_PARAMS = {}
        with self.assertRaises(HttpNotAcceptableException) as context:
            the_function_tl_1(3, request, pk=user.guid)
        self.assertEqual(context.exception.status_code, 406)
        self.assertEqual(context.exception.error, 'invalid_data')
        self.assertEqual(context.exception.error_description, 'Invalid data passed: mandatory is missing')

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

    # @todo test relation listing which sparked the keyerror in the first place
    def test_return_list(self):
        """
        Validates whether the return_list decorator works correctly:
        * Parsing:
          * Parses the 'contents' parameter
        * Passes the 'full' hint to the decorated function, indicating whether full objects are useful
        * Contents:
          * If contents are specified: Runs the list trough the serializer
          * Else, return the guid list
        """
        data_holder = self.data_holder
        data_list_machines = self.data_list_machines
        output_values = data_holder.output_values

        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        for fct, has_hinting, returns_guids in [(data_holder.get_base_list, False, False),
                                                (data_holder.get_base_list_guids, False, True)]:
            request.QUERY_PARAMS = {}
            response = fct(0, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            guid_data = response.data['data']
            self.assertEqual(len(guid_data), len(data_list_machines))
            # No contents mean no serialization. Only guids will be returned
            self.assertIsInstance(guid_data, list)
            self.assertIsInstance(guid_data[0], str)

            request.QUERY_PARAMS['contents'] = ''
            response = fct(1, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True, 'Full objects are required as contents is passed')
            # Contents requested so data is fully serialized
            # Change with everything being offloaded to DataList makes sure that the instance that is serialized is always a DataList
            # The serializer data property is returned by Django (which is a dict)
            serialized_data = response.data['data']
            serialized_data_instance = serialized_data['instance']
            self.assertIsInstance(serialized_data, dict)
            self.assertIsInstance(serialized_data_instance, DataList)
            self.assertIsInstance(serialized_data_instance[0], TestMachine)
            # The guids should be identical as no filtering was done. Added sort as the ordering does not matter here
            self.assertEqual(data_list_machines.guids.sort(), serialized_data_instance.guids.sort())

    def test_return_list_relation(self):
        """
        DataObject relations are DataList which are created with keys based of the relational object instead of a query
        """
        # Special case. Relational lists are created with keys instead of queries.
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        admin_group = DataHolder.get_admin_group()
        admin_group_users = admin_group.users

        request.QUERY_PARAMS = {}
        response = self.data_holder.get_users_of_admin_group(request)
        self.assertEqual(response.status_code, 200)
        guid_data = response.data['data']
        self.assertEqual(len(guid_data), len(admin_group_users))
        self.assertIsInstance(guid_data, list)
        self.assertIsInstance(guid_data[0], str)

        request.QUERY_PARAMS = {'contents': ''}
        response = self.data_holder.get_users_of_admin_group(request)
        self.assertEqual(response.status_code, 200)
        # Contents requested so data is fully serialized
        # Change with everything being offloaded to DataList makes sure that the instance that is serialized is always a DataList
        # The serializer data property is returned by Django (which is a dict)
        serialized_data = response.data['data']
        serialized_data_instance = serialized_data['instance']
        self.assertIsInstance(serialized_data, dict)
        self.assertIsInstance(serialized_data_instance, DataList)
        self.assertIsInstance(serialized_data_instance[0], User)
        # The guids should be identical as no filtering was done. Added sort as the ordering does not matter here
        self.assertEqual(admin_group_users.guids.sort(), serialized_data_instance.guids.sort())

    def test_return_list_sorting(self):
        """
        Validates whether the return_list decorator works correctly:
        * Parsing:
          * Parses the 'sort' parameter, optionally falling back to value specified by decorator
        * Passes the 'full' hint to the decorated function, indicating whether full objects are useful
        * If sorting is requested:
          * Loads a possibly returned list of guids
          * Sorts the returned list
        """
        data_list_machines = self.data_list_machines
        data_holder = self.data_holder
        output_values = data_holder.output_values

        # Test sorting
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        # Hinting is applied when requesting/applying sort
        for fct, has_hinting, returns_guids in [(data_holder.get_base_list, False, False),
                                                (data_holder.get_base_list_guids_default_sorted, True, True)]:

            request.QUERY_PARAMS = {}
            response = fct(1, request)
            self.assertEqual(response.status_code, 200)
            # No changes were requested. There are the default requests
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            self.assertEqual(len(response.data), len(data_list_machines))
            if fct == data_holder.get_base_list:
                # No sorting was applied. The guids of gets returned (no contents asked for)
                output = data_list_machines.guids
            else:
                # data_holder.get_base_list_guids_default_sorted with default_sort='name,description'
                output = [self.machines_by_name_description['aa']['bb'].guid,
                          self.machines_by_name_description['aa']['cc'].guid,
                          self.machines_by_name_description['bb']['aa'].guid,
                          self.machines_by_name_description['bb']['dd'].guid]
            self.assertListEqual(response.data['data'], output)

            # Reverse sort on description.
            request.QUERY_PARAMS['sort'] = 'name,-description'
            response = fct(2, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            self.assertListEqual(response.data['data'], [self.machines_by_name_description['aa']['cc'].guid,
                                                         self.machines_by_name_description['aa']['bb'].guid,
                                                         self.machines_by_name_description['bb']['dd'].guid,
                                                         self.machines_by_name_description['bb']['aa'].guid])

            # Reverse sort on both name and description
            request.QUERY_PARAMS['sort'] = '-name,-description'
            response = fct(3, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            self.assertListEqual(response.data['data'], [self.machines_by_name_description['bb']['dd'].guid,
                                                         self.machines_by_name_description['bb']['aa'].guid,
                                                         self.machines_by_name_description['aa']['cc'].guid,
                                                         self.machines_by_name_description['aa']['bb'].guid])

            # First sort on description
            request.QUERY_PARAMS['sort'] = 'description,name'
            response = fct(4, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], True)
            self.assertEqual(len(response.data['data']), len(data_list_machines))
            self.assertListEqual(response.data['data'], [self.machines_by_name_description['bb']['aa'].guid,
                                                         self.machines_by_name_description['aa']['bb'].guid,
                                                         self.machines_by_name_description['aa']['cc'].guid,
                                                         self.machines_by_name_description['bb']['dd'].guid])

    def test_return_list_sorting_relation(self):
        """
        DataObject relations are DataList which are created with keys based of the relational object instead of a query
        See: https://github.com/openvstorage/framework/issues/2244
        """
        # Special case. Relational lists are created with keys instead of queries.
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        admin_group = DataHolder.get_admin_group()
        admin_group_users = admin_group.users

        request.QUERY_PARAMS = {'sort': 'username'}
        response = self.data_holder.get_users_of_admin_group(request)
        self.assertEqual(response.status_code, 200)
        guid_data = response.data['data']
        self.assertEqual(len(guid_data), len(admin_group_users))
        self.assertIsInstance(guid_data, list)
        self.assertIsInstance(guid_data[0], str)
        self.assertEqual([user.guid for user in sorted(admin_group_users, key=lambda u: u.username)],
                         guid_data)

    def test_return_list_filtering(self):
        """
        Validates whether the return_list decorator works correctly:
        * Parsing:
          * Parses the 'query' parameter
        * Passes the 'full' hint to the decorated function, indicating whether full objects are useful
        """
        data_list_machines = DataList(TestMachine, {'type': DataList.where_operator.OR,
                                                    'items': [('name', DataList.operator.EQUALS, 'aa'),
                                                              ('name', DataList.operator.EQUALS, 'bb')]})
        self.assertEqual(len(data_list_machines), 4)

        data_holder = DataHolder(data_list_machines, self.machines_random_order)
        output_values = data_holder.output_values

        # Test filtering
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')

        for fct, has_hinting, returns_guids in [(data_holder.get_base_list, False, False),
                                                (data_holder.get_base_list_guids_default_sorted, True, True),
                                                (data_holder.get_base_list_first_two, False, False),
                                                (data_holder.get_base_list_guids_first_two, False, True)]:

            request.QUERY_PARAMS = {'query': json.dumps({'type': 'AND',
                                                        'items': [['description', 'EQUALS', 'aa']]})}
            # Test querying, not to be tested thoroughly (test_basic handles DataList queries)
            response = fct(1, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            expected_items = [self.machines_by_name_description['bb']['aa'].guid]
            self.assertEqual(len(response.data['data']), len(expected_items))
            self.assertListEqual(response.data['data'], expected_items)

            request.QUERY_PARAMS['query'] = json.dumps({'type': 'AND',
                                                        'items': [['description', 'EQUALS', 'dd']]})
            response = fct(2, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            if fct in [data_holder.get_base_list,
                       data_holder.get_base_list_guids_default_sorted]:
                expected_items = [self.machines_by_name_description['bb']['dd'].guid]
            else:
                expected_items = []  # Not found in the first two items
            self.assertEqual(len(response.data['data']), len(expected_items))
            self.assertListEqual(response.data['data'], expected_items)

            request.QUERY_PARAMS['query'] = json.dumps('rawr')
            with self.assertRaises(ValueError):
                # Can't capture the response as the exception will be raised in the same context
                fct(1, request)

    def test_return_list_pagination(self):
        """
        Validates whether the return_list decorator works correctly:
        * Parsing:
          * Parses the 'page' parameter
        * Passes the 'full' hint to the decorated function, indicating whether full objects are useful
        """
        data_holder = self.data_holder
        data_list_machines = self.data_list_machines
        output_values = data_holder.output_values

        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')

        # Test pagination

        # Arguments
        fct, has_hinting, returns_guids = (data_holder.get_base_list, False, False)
        for arg_type in [int, str]:
            request.QUERY_PARAMS = {'page': 1 if arg_type == int else '1',
                                    'page_size': 2 if arg_type == int else '2'}
            response = fct(1, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            expected_items = [machine.guid for machine in data_list_machines][0:2]
            self.assertEqual(len(response.data['data']), len(expected_items))
            self.assertListEqual(response.data['data'], expected_items)

        # Pagination
        request = self.factory.get('/', HTTP_ACCEPT='application/json; version=1')
        for fct, has_hinting, returns_guids in [(data_holder.get_base_list, False, False),
                                                (data_holder.get_base_list_guids_default_sorted, True, True),
                                                (data_holder.get_base_list_first_two, False, False),
                                                (data_holder.get_base_list_guids_first_two, False, True)]:
            print fct
            request.QUERY_PARAMS = {'page': 1,
                                    'page_size': 2}
            response = fct(2, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            if fct == data_holder.get_base_list_guids_default_sorted:
                expected_items = [self.machines_by_name_description['aa']['bb'].guid,
                                  self.machines_by_name_description['aa']['cc'].guid]
            elif fct in [data_holder.get_base_list_first_two,
                         data_holder.get_base_list_guids_first_two]:
                expected_items = [machine.guid for machine in self.data_holder.ordered_set[0:2]]

            else:
                # Baselist
                expected_items = [machine.guid for machine in data_list_machines][0:2]
            self.assertEqual(len(response.data['data']), len(expected_items))
            print 'page1', response.data['data'], expected_items
            self.assertListEqual(response.data['data'], expected_items)

            request.QUERY_PARAMS = {'page': 2,
                                    'page_size': 2}
            response = fct(3, request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(output_values['kwargs']['hints']['full'], has_hinting)
            if fct == data_holder.get_base_list_guids_default_sorted:
                expected_items = [self.machines_by_name_description['bb']['aa'].guid,
                                  self.machines_by_name_description['bb']['dd'].guid]
            elif fct in [data_holder.get_base_list_first_two,
                         data_holder.get_base_list_guids_first_two]:
                # Same items as page 1 because only 2 items in total and when calling a page higher than max,
                #  it will go back to the result for the max page
                expected_items = [machine.guid for machine in self.machines_random_order[0:2]]
            else:
                # Baselist
                expected_items = [machine.guid for machine in data_list_machines][2:4]
            self.assertEqual(len(response.data['data']), len(expected_items))
            print 'page2', response.data['data'], expected_items
            self.assertListEqual(response.data['data'], expected_items)

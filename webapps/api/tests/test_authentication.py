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
import base64
import hashlib
import unittest
from django.http import HttpResponse
from api.middleware import OVSMiddleware
from api.oauth2.toolbox import OAuth2Toolbox
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.group import Group
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.hybrids.j_rolegroup import RoleGroup
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.user import User
from ovs.dal.lists.userlist import UserList
from ovs.dal.tests.helpers import DalHelper
from ovs_extensions.api.exceptions import HttpBadRequestException, HttpTooManyRequestsException, HttpUnauthorizedException
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System


class Authentication(unittest.TestCase):
    """
    The authentication test suite will validate all OAuth 2.0 related API code
    """
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

        storagerouter = StorageRouter()
        storagerouter.machine_id = 'storagerouter'
        storagerouter.ip = '127.0.0.1'
        storagerouter.machine_id = '1'
        storagerouter.rdma_capable = False
        storagerouter.name = 'storagerouter'
        storagerouter.save()

        from django.test import RequestFactory
        cls.factory = RequestFactory()

        Configuration.set('/ovs/framework/plugins/installed', {'generic': [],
                                                               'backends': []})
        Configuration.set('/ovs/framework/cluster_id', 'cluster_id')
        System._machine_id = {'none': '1'}

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        DalHelper.teardown(fake_sleep=True)

    def _assert_failure(self, view, request, status_code, error_code, exception):
        middleware = OVSMiddleware()
        with self.assertRaises(exception) as context:
            view(request)
        response = middleware.process_exception(request, context.exception)
        self.assertEqual(response.status_code, status_code)
        data = json.loads(response.content)
        self.assertIn('error', data)
        self.assertEqual(data['error'], error_code)

    def test_jsonresponse(self):
        """
        Validates whether the json response behave correctly
        """
        from api.oauth2.decorators import auto_response

        @auto_response()
        def the_function(return_value):
            """
            Decorated function
            """
            return return_value

        response = the_function({'test': 0})
        self.assertIsInstance(response, HttpResponse)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '{"test": 0}')
        response = the_function(0)
        self.assertIsInstance(response, HttpResponse)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '0')

    def test_ratelimit(self):
        """
        Validates whether the rate limiter behaves correctly
        """
        from api.oauth2.decorators import limit, auto_response

        @auto_response()
        @limit(amount=2, per=2, timeout=2)
        def the_function(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            output['value'] = input_value
            return input_value

        output = {'value': None}
        request = self.factory.post('/')
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
        response = OVSMiddleware().process_exception(request, context.exception)
        self.assertEqual(output['value'], 3)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(json.loads(response.content)['error'], 'rate_limit_reached')
        with self.assertRaises(HttpTooManyRequestsException) as context:
            the_function(5, request)
        response = OVSMiddleware().process_exception(request, context.exception)
        self.assertEqual(output['value'], 3)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(json.loads(response.content)['error'], 'rate_limit_timeout')
        time.sleep(5)
        response = the_function(6, request)
        self.assertEqual(output['value'], 6)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, '6')

    def test_grandtype_headers(self):
        """
        Validates whether not sending a grant_type will fail the call and the grant_type is checked
        """
        from api.oauth2.tokenview import OAuth2TokenView

        time.sleep(180)
        request = self.factory.post('/', HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_request', HttpBadRequestException)

        time.sleep(180)
        data = {'grant_type': 'foobar'}
        request = self.factory.post('/', HTTP_X_REAL_IP='127.0.0.1', data=data)
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'unsupported_grant_type', HttpBadRequestException)

    def test_resource_owner_password_credentials(self):
        """
        Validates the Resource Owner Password Credentials
        """
        from api.oauth2.tokenview import OAuth2TokenView

        time.sleep(180)
        data = {'grant_type': 'password'}
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_request', HttpBadRequestException)

        time.sleep(180)
        data.update({'username': 'admin_npg',
                     'password': 'foobar'})
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_client', HttpBadRequestException)

        time.sleep(180)
        data.update({'username': 'admin_na',
                     'password': 'admin_na'})
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'inactive_user', HttpBadRequestException)

        time.sleep(180)
        data.update({'username': 'admin_npg',
                     'password': 'admin_npg'})
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'unauthorized_client', HttpBadRequestException)

        time.sleep(180)
        # Test default expiration
        data.update({'username': 'admin',
                     'password': 'admin'})
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 200)
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content)

        result = {'access_token': response_content['access_token'],
                  'token_type': 'bearer',
                  'expires_in': OAuth2Toolbox.EXPIRATION_USER}
        self.assertDictEqual(response_content, result)

        # Test config changed expiration
        week_expiration = 60 * 60 * 24 * 7
        Configuration.set('ovs/framework/api/oauth|expiration_user', week_expiration)
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 200)
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content)
        result = {'access_token': response_content['access_token'],
                  'token_type': 'bearer',
                  'expires_in': week_expiration}
        self.assertDictEqual(response_content, result)

    def test_client_credentials(self):
        """
        Validates the Client Credentials
        """
        from api.oauth2.tokenview import OAuth2TokenView

        time.sleep(180)
        data = {'grant_type': 'client_credentials'}
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'missing_header', HttpBadRequestException)

        time.sleep(180)
        header = 'Basic {0}'.format(base64.encodestring('{0}:{1}'.format('foo', 'bar')))
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.2', HTTP_AUTHORIZATION=header)
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_client', HttpBadRequestException)

        time.sleep(180)
        admin_na = UserList.get_user_by_username('admin_na')
        admin_na_client = Client()
        admin_na_client.ovs_type = 'USER'
        admin_na_client.grant_type = 'PASSWORD'
        admin_na_client.client_secret = OAuth2Toolbox.create_hash(64)
        admin_na_client.user = admin_na
        admin_na_client.save()
        header = 'Basic {0}'.format(base64.encodestring('{0}:{1}'.format(admin_na_client.guid, admin_na_client.client_secret)))
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.3', HTTP_AUTHORIZATION=header)
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_grant', HttpBadRequestException)

        time.sleep(180)
        admin_na_client.grant_type = 'CLIENT_CREDENTIALS'
        admin_na_client.save()
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.4', HTTP_AUTHORIZATION=header)
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'inactive_user', HttpBadRequestException)

        time.sleep(180)
        admin = UserList.get_user_by_username('admin')
        admin_client = Client()
        admin_client.ovs_type = 'USER'
        admin_client.grant_type = 'CLIENT_CREDENTIALS'
        admin_client.client_secret = OAuth2Toolbox.create_hash(64)
        admin_client.user = admin
        admin_client.save()
        header = 'Basic {0}'.format(base64.encodestring('{0}:foobar'.format(admin_client.guid)))
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.5', HTTP_AUTHORIZATION=header)
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_client', HttpBadRequestException)

        time.sleep(180)
        # Test default expiration
        header = 'Basic {0}'.format(base64.encodestring('{0}:{1}'.format(admin_client.guid, admin_client.client_secret)))
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.6', HTTP_AUTHORIZATION=header)
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 200)
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content)
        result = {'access_token': response_content['access_token'],
                  'token_type': 'bearer',
                  'expires_in': OAuth2Toolbox.EXPIRATION_CLIENT}
        self.assertDictEqual(response_content, result)

        # Test config expiration
        day_expiration = 60 * 60 * 24 * 1
        Configuration.set('ovs/framework/api/oauth|expiration_client', day_expiration)
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.6', HTTP_AUTHORIZATION=header)
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 200)
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content)
        result = {'access_token': response_content['access_token'],
                  'token_type': 'bearer',
                  'expires_in': day_expiration}
        self.assertDictEqual(response_content, result)

    def test_specify_scopes(self):
        """
        Validates whether requested scopes are assigned
        """
        from api.oauth2.tokenview import OAuth2TokenView
        from api.view import MetadataView

        time.sleep(180)
        data = {'grant_type': 'password',
                'username': 'admin',
                'password': 'admin'}
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        self.assertIn('access_token', json.loads(response.content))
        access_token = json.loads(response.content)['access_token']

        time.sleep(180)
        header = 'Bearer {0}'.format(access_token)
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertListEqual(sorted(response_content['roles']), ['manage', 'read', 'write'])

        time.sleep(180)
        data['scope'] = 'read write'
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content)
        access_token = response_content['access_token']

        time.sleep(180)
        header = 'Bearer {0}'.format(access_token)
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertIn('roles', response_content)
        self.assertListEqual(sorted(response_content['roles']), ['read', 'write'])

        time.sleep(180)
        data = {'grant_type': 'password',
                'username': 'user',
                'password': 'user',
                'scope': 'read write manage'}
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        self._assert_failure(OAuth2TokenView.as_view(), request, 400, 'invalid_scope', HttpBadRequestException)

    def test_authentication_backend(self):
        """
        Validates the Authentication backend
        """
        from django.contrib.auth.models import User as DUser
        from api.oauth2.tokenview import OAuth2TokenView
        from api.oauth2.backend import OAuth2Backend

        time.sleep(180)
        backend = OAuth2Backend()
        data = {'grant_type': 'password',
                'username': 'admin',
                'password': 'admin'}
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        response_content = json.loads(response.content)
        access_token = response_content['access_token']
        request = self.factory.get('/')
        response = backend.authenticate(request)
        self.assertIsNone(response)
        time.sleep(180)
        header = 'Bearer foobar'
        request = self.factory.get('/', HTTP_AUTHORIZATION=header)
        with self.assertRaises(HttpUnauthorizedException) as context:
            backend.authenticate(request)
        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(str(context.exception.error), 'invalid_token')

        time.sleep(180)
        header = 'Bearer {0}'.format(access_token)
        request = self.factory.get('/', HTTP_AUTHORIZATION=header)
        user, extra = backend.authenticate(request)
        self.assertIsInstance(user, DUser)
        self.assertIsNone(extra)
        self.assertEqual(request.token.access_token, access_token)
        self.assertEqual(request.client.user.username, 'admin')

        time.sleep(180)
        user = UserList.get_user_by_username('admin')
        user.is_active = False
        user.save()
        request = self.factory.get('/', HTTP_AUTHORIZATION=header)
        with self.assertRaises(HttpUnauthorizedException) as context:
            backend.authenticate(request)
        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(str(context.exception.error), 'inactive_user')
        user.is_active = True
        user.save()

        time.sleep(int(response_content['expires_in']))
        request = self.factory.get('/', HTTP_AUTHORIZATION=header)
        with self.assertRaises(HttpUnauthorizedException) as context:
            backend.authenticate(request)
        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(str(context.exception.error), 'token_expired')

    def test_metadata(self):
        """
        Validates the authentication related information at the API root's metadata.
        - The 'roles' key is already checked in the Scope-related tests
        """
        from ovs.dal.lists.bearertokenlist import BearerTokenList
        from api.oauth2.tokenview import OAuth2TokenView
        from api.view import MetadataView

        def _raise_exception(argument):
            _ = argument
            raise RuntimeError('foobar')

        result_data = {'authenticated': False,
                       'authentication_state': None,
                       'username': None,
                       'userguid': None}

        time.sleep(180)
        data = {'grant_type': 'password',
                'username': 'admin',
                'password': 'admin'}
        request = self.factory.post('/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertIn('expires_in', response_content)
        self.assertIn('access_token', response_content)

        time.sleep(180)
        expiry = int(response_content['expires_in'])
        access_token = response_content['access_token']
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1')
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'unauthenticated'}.items()), response_content)

        time.sleep(180)
        header = 'Basic foobar'
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'invalid_authorization_type'}.items()), response_content)

        time.sleep(180)
        header = 'Bearer foobar'
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'invalid_token'}.items()), response_content)

        time.sleep(180)
        user = UserList.get_user_by_username('admin')
        header = 'Bearer {0}'.format(access_token)
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authenticated': True,
                                                                  'authentication_state': 'authenticated',
                                                                  'username': user.username,
                                                                  'userguid': user.guid}.items()), response_content)

        time.sleep(180)
        user.is_active = False
        user.save()
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'inactive_user'}.items()), response_content)
        user.is_active = True
        user.save()

        time.sleep(180)
        original_method = BearerTokenList.get_by_access_token
        BearerTokenList.get_by_access_token = staticmethod(_raise_exception)
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'unexpected_exception'}.items()), response_content)

        time.sleep(180)
        BearerTokenList.get_by_access_token = staticmethod(original_method)
        time.sleep(expiry)
        request = self.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'token_expired'}.items()), response_content)

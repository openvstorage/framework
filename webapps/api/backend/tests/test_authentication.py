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
import json
import time
import base64
import hashlib
from unittest import TestCase
from django.http import HttpResponse, HttpResponseBadRequest
from rest_framework.exceptions import AuthenticationFailed
from ovs.extensions.generic import fakesleep
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.dal.lists.userlist import UserList
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.group import Group
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.j_rolegroup import RoleGroup
from ovs.dal.hybrids.j_roleclient import RoleClient
from oauth2.toolbox import Toolbox as OAuth2Toolbox


class Authentication(TestCase):
    """
    The authentication test suite will validate all OAuth 2.0 related API code
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

        Authentication.initial_data = PersistentFactory.store._read(), VolatileFactory.store._read()

        from django.test import RequestFactory
        Authentication.factory = RequestFactory()

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

        PersistentFactory.store._save(Authentication.initial_data[0])
        VolatileFactory.store._save(Authentication.initial_data[1])

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        fakesleep.monkey_restore()

    def test_jsonresponse(self):
        """
        Validates whether the json response behave correctly
        """
        from oauth2.decorators import json_response

        @json_response()
        def the_function(return_type, return_value, return_code=None):
            """
            Decorated function
            """
            if return_code is None:
                return return_type, return_value
            else:
                return return_type, return_value, return_code

        response = the_function(HttpResponse, {'test': 0})
        self.assertIsInstance(response, HttpResponse, 'Should be a HttpResponse')
        self.assertEqual(response.status_code, 200, 'Should have the correct status code')
        self.assertEqual(response.content, '{"test": 0}', 'Should have the correct content')
        response = the_function(HttpResponseBadRequest, {'error': ['invalid']}, 429)
        self.assertIsInstance(response, HttpResponseBadRequest, 'Should be a HttpResponse')
        self.assertEqual(response.status_code, 429, 'Should have the correct status code')
        self.assertEqual(response.content, '{"error": ["invalid"]}', 'Should have the correct content')

    def test_ratelimit(self):
        """
        Validates whether the rate limiter behaves correctly
        """
        from oauth2.decorators import limit, json_response

        @json_response()
        @limit(amount=2, per=2, timeout=2)
        def the_function(input_value, *args, **kwargs):
            """
            Decorated function
            """
            _ = args, kwargs
            output['value'] = input_value
            return HttpResponse, input_value

        output = {'value': None}
        request = Authentication.factory.post('/oauth2/token/')
        with self.assertRaises(KeyError):
            # Should raise a KeyError complaining about the HTTP_X_REAL_IP
            the_function(1, request)
        request.META['HTTP_X_REAL_IP'] = '127.0.0.1'
        response = the_function(2, request)
        self.assertEqual(output['value'], 2, 'Function should be called')
        self.assertEqual(response.status_code, 200, 'Correct status code')
        self.assertEqual(response.content, '2', 'Correct response expected. Got {0}'.format(response.content))
        response = the_function(3, request)
        self.assertEqual(output['value'], 3, 'Function should be called')
        self.assertEqual(response.status_code, 200, 'Correct status code')
        self.assertEqual(response.content, '3', 'Correct response expected. Got {0}'.format(response.content))
        response = the_function(4, request)
        self.assertEqual(output['value'], 3, 'Function should not be called')
        self.assertEqual(response.status_code, 429, 'Rate limit status code')
        self.assertEqual(json.loads(response.content)['error_code'], 'rate_limit_reached', 'Correct response expected')
        response = the_function(5, request)
        self.assertEqual(output['value'], 3, 'Function should not be called')
        self.assertEqual(response.status_code, 429, 'Rate limit status code')
        self.assertEqual(json.loads(response.content)['error_code'], 'rate_limit_timeout', 'Correct response expected')
        time.sleep(5)
        response = the_function(6, request)
        self.assertEqual(output['value'], 6, 'Function should be called')
        self.assertEqual(response.status_code, 200, 'Correct status code')
        self.assertEqual(response.content, '6', 'Correct response expected. Got {0}'.format(response.content))

    def test_grandtype_headers(self):
        """
        Validates whether not sending a grant_type will fail the call and the grant_type is checked
        """
        from oauth2.tokenview import OAuth2TokenView

        request = Authentication.factory.post('/oauth2/token/', HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'invalid_request'}), 'Response should contain the correct parameter: {0}'.format(response.content))
        data = {'grant_type': 'foobar'}
        request = Authentication.factory.post('/oauth2/token/', HTTP_X_REAL_IP='127.0.0.1', data=data)
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'unsupported_grant_type'}), 'Response should contain the correct parameter: {0}'.format(response.content))

    def test_resource_owner_password_credentials(self):
        """
        Validates the Resource Owner Password Credentials
        """
        from oauth2.tokenview import OAuth2TokenView

        data = {'grant_type': 'password'}
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        # Fails because there's no username & password
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'invalid_request'}), 'Response should contain the correct parameter ({0})'.format(response.content))
        data.update({'username': 'admin_npg',
                     'password': 'foobar'})
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        # Fails because the password is wrong
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'invalid_client'}), 'The client should be invalid, wrong password ({0})'.format(response.content))
        data.update({'username': 'admin_na',
                     'password': 'admin_na'})
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        # Fails because the user is inactive
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'inactive_user'}), 'The client should be invalid, inactive ({0})'.format(response.content))
        data.update({'username': 'admin_npg',
                     'password': 'admin_npg'})
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        # Fails because there's no password grant
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'unauthorized_client'}), 'The client should be unauthorized ({0})'.format(response.content))
        data.update({'username': 'admin',
                     'password': 'admin'})
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        # Succeeds
        self.assertEqual(response.status_code, 200, 'Valid request')
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content, 'The response should contain an access token')
        result = {'access_token': response_content['access_token'],
                  'token_type': 'bearer',
                  'expires_in': 86400}
        self.assertDictEqual(response_content, result, 'The response should be like expected')

    def test_client_credentials(self):
        """
        Validates the Client Credentials
        """
        from oauth2.tokenview import OAuth2TokenView

        data = {'grant_type': 'client_credentials'}
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        # Fails because the HTTP_AUTHORIZATION header is missing
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'missing_header'}), 'Response should contain the correct parameter ({0})'.format(response.content))
        header = 'Basic {0}'.format(base64.encodestring('{0}:{1}'.format('foo', 'bar')))
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = OAuth2TokenView.as_view()(request)
        # Fails because there is no such client
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'invalid_client'}), 'The client should be invalid, no such client ({0})'.format(response.content))
        admin_na = UserList.get_user_by_username('admin_na')
        admin_na_client = Client()
        admin_na_client.ovs_type = 'USER'
        admin_na_client.grant_type = 'PASSWORD'
        admin_na_client.client_secret = OAuth2Toolbox.create_hash(64)
        admin_na_client.user = admin_na
        admin_na_client.save()
        header = 'Basic {0}'.format(base64.encodestring('{0}:{1}'.format(admin_na_client.guid, admin_na_client.client_secret)))
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = OAuth2TokenView.as_view()(request)
        # Fails because the grant is of type Resource Owner Password Credentials
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'invalid_grant'}), 'The grant is invalid ({0})'.format(response.content))
        admin_na_client.grant_type = 'CLIENT_CREDENTIALS'
        admin_na_client.save()
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = OAuth2TokenView.as_view()(request)
        # Fails because the grant is of type Resource Owner Password Credentials
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'inactive_user'}), 'Unauthorized client, inactive ({0})'.format(response.content))
        admin = UserList.get_user_by_username('admin')
        admin_client = Client()
        admin_client.ovs_type = 'USER'
        admin_client.grant_type = 'CLIENT_CREDENTIALS'
        admin_client.client_secret = OAuth2Toolbox.create_hash(64)
        admin_client.user = admin
        admin_client.save()
        header = 'Basic {0}'.format(base64.encodestring('{0}:{1}'.format(admin_client.guid, admin_client.client_secret)))
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = OAuth2TokenView.as_view()(request)
        # Succeeds
        self.assertEqual(response.status_code, 200, 'Valid request')
        response_content = json.loads(response.content)
        self.assertIn('access_token', response_content, 'The response should contain an access token')
        result = {'access_token': response_content['access_token'],
                  'token_type': 'bearer',
                  'expires_in': 3600}
        self.assertDictEqual(response_content, result, 'The response should be like expected')

    def test_specify_scopes(self):
        """
        Validates whether requested scopes are assigned
        """
        from oauth2.tokenview import OAuth2TokenView
        from view import MetadataView

        data = {'grant_type': 'password',
                'username': 'admin',
                'password': 'admin'}
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        access_token = json.loads(response.content)['access_token']
        header = 'Bearer {0}'.format(access_token)
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertListEqual(response_content['roles'], ['read', 'write', 'manage'], 'By default, all scopes are applied ({0})'.format(response_content['roles']))
        data['scope'] = 'read write'
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        access_token = json.loads(response.content)['access_token']
        header = 'Bearer {0}'.format(access_token)
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertListEqual(sorted(response_content['roles']), ['read', 'write'], 'Should only contain requested scopes ({0})'.format(sorted(response_content['roles'])))
        data = {'grant_type': 'password',
                'username': 'user',
                'password': 'user',
                'scope': 'read write manage'}
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        self.assertEqual(response.status_code, 400, 'Invalid request')
        self.assertEqual(response.content, json.dumps({'error': 'invalid_scope'}), 'The requested scope should be denied ({0})'.format(response.content))

    def test_authentication_backend(self):
        """
        Validates the Authentication backend
        """
        from django.contrib.auth.models import User as DUser
        from oauth2.tokenview import OAuth2TokenView
        from oauth2.backend import OAuth2Backend

        backend = OAuth2Backend()
        data = {'grant_type': 'password',
                'username': 'admin',
                'password': 'admin'}
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        response_content = json.loads(response.content)
        access_token = response_content['access_token']
        request = Authentication.factory.get('/')
        response = backend.authenticate(request)
        self.assertIsNone(response, 'When no header is given, there is no authentication')
        header = 'Bearer foobar'
        request = Authentication.factory.get('/', HTTP_AUTHORIZATION=header)
        with self.assertRaises(AuthenticationFailed) as context:
            backend.authenticate(request)
        self.assertEqual(context.exception.status_code, 401, 'Invalid request')
        self.assertEqual(str(context.exception.detail), 'invalid_token', 'Correct error should be raisen ({0})'.format(str(context.exception.detail)))
        header = 'Bearer {0}'.format(access_token)
        request = Authentication.factory.get('/', HTTP_AUTHORIZATION=header)
        user, extra = backend.authenticate(request)
        self.assertIsInstance(user, DUser, 'Authenticate should return a Django user')
        self.assertIsNone(extra, 'The second parameter should be None')
        self.assertEqual(request.token.access_token, access_token, 'Request should contain the access token')
        self.assertEqual(request.client.user.username, 'admin', 'The request should contain client (and user)')
        user = UserList.get_user_by_username('admin')
        user.is_active = False
        user.save()
        request = Authentication.factory.get('/', HTTP_AUTHORIZATION=header)
        with self.assertRaises(AuthenticationFailed) as context:
            backend.authenticate(request)
        self.assertEqual(context.exception.status_code, 401, 'Invalid request')
        self.assertEqual(str(context.exception.detail), 'inactive_user', 'Correct error should be raisen ({0})'.format(str(context.exception.detail)))
        time.sleep(int(response_content['expires_in']))
        request = Authentication.factory.get('/', HTTP_AUTHORIZATION=header)
        with self.assertRaises(AuthenticationFailed) as context:
            backend.authenticate(request)
        self.assertEqual(context.exception.status_code, 401, 'Invalid request')
        self.assertEqual(str(context.exception.detail), 'token_expired', 'Correct error should be raisen ({0})'.format(str(context.exception.detail)))

    def test_metadata(self):
        """
        Validates the authentication related information at the API root's metadata.
        - The 'roles' key is already checked in the Scope-related tests
        """
        from ovs.dal.lists.bearertokenlist import BearerTokenList
        from oauth2.tokenview import OAuth2TokenView
        from view import MetadataView

        def raise_exception(argument):
            _ = argument
            raise RuntimeError('foobar')

        result_data = {'authenticated': False,
                       'authentication_state': None,
                       'username': None,
                       'userguid': None}
        data = {'grant_type': 'password',
                'username': 'admin',
                'password': 'admin'}
        request = Authentication.factory.post('/oauth2/token/', data=data, HTTP_X_REAL_IP='127.0.0.1')
        response = OAuth2TokenView.as_view()(request)
        response_content = json.loads(response.content)
        expiry = int(response_content['expires_in'])
        access_token = response_content['access_token']
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1')
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'unauthenticated'}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))
        header = 'Basic foobar'
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'invalid_authorization_type'}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))
        header = 'Bearer foobar'
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'invalid_token'}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))
        user = UserList.get_user_by_username('admin')
        header = 'Bearer {0}'.format(access_token)
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authenticated': True,
                                                                  'username': user.username,
                                                                  'userguid': user.guid}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))
        time.sleep(180)  # Make sure to not hit the rate limit
        user.is_active = False
        user.save()
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'inactive_user'}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))
        original_method = BearerTokenList.get_by_access_token
        BearerTokenList.get_by_access_token = staticmethod(raise_exception)
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'unexpected_exception'}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))
        BearerTokenList.get_by_access_token = staticmethod(original_method)
        time.sleep(expiry)
        request = Authentication.factory.get('/', HTTP_X_REAL_IP='127.0.0.1', HTTP_AUTHORIZATION=header)
        response = MetadataView.as_view()(request)
        response_content = json.loads(response.content)
        self.assertDictContainsSubset(dict(result_data.items() + {'authentication_state': 'token_expired'}.items()),
                                      response_content, 'Unexpected metadata set ({0})'.format(response_content))

if __name__ == '__main__':
    import os
    import unittest
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    suite = unittest.TestLoader().loadTestsFromTestCase(Authentication)
    unittest.TextTestRunner(verbosity=2).run(suite)

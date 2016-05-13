# Copyright 2016 iNuron NV
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
Module for the OVS API client
"""
import time
import base64
import urllib
import hashlib
import requests


class ForbiddenException(RuntimeError):
    """
    Custom exception class
    """
    pass


class OVSClient(object):
    """
    Represents the OVS client
    """

    def __init__(self, ip, port, credentials=None, verify=False, version='*', raw_response=False):
        """
        Initializes the object with credentials and connection information
        """
        if credentials is not None and len(credentials) != 2:
            raise RuntimeError('Credentials should be None (no authentication) or a tuple containing client_id and client_secret (authenticated)')
        self.ip = ip
        self.port = port
        self.client_id = credentials[0] if credentials is not None else None
        self.client_secret = credentials[1] if credentials is not None else None
        self._url = 'https://{0}:{1}/api'.format(ip, port)
        self._key = hashlib.sha256('{0}{1}{2}{3}'.format(self.ip, self.port, self.client_id, self.client_secret)).hexdigest()
        self._token = None
        self._verify = verify
        self._version = version
        self._raw_response = raw_response
        try:
            from ovs.extensions.storage.volatilefactory import VolatileFactory
            self._volatile_client = VolatileFactory.get_client()
        except ImportError:
            self._volatile_client = None

    def _connect(self):
        """
        Authenticates to the api
        """
        headers = {'Accept': 'application/json',
                   'Authorization': 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(self.client_id, self.client_secret)).strip())}
        raw_response = requests.post(url='{0}/oauth2/token/'.format(self._url),
                                     data={'grant_type': 'client_credentials'},
                                     headers=headers,
                                     verify=self._verify)

        try:
            response = self._process(response=raw_response, overrule_raw=True)
        except RuntimeError:
            if self._raw_response is True:
                return raw_response
            raise
        if len(response.keys()) == 1 and 'error' in response:
            error = RuntimeError(response['error'])
            error.status_code = raw_response.status_code
            raise error
        self._token = response['access_token']

    def _prepare(self, **kwargs):
        """
        Prepares the call:
        * Authentication, if required
        * Preparing headers, returning them
        """
        if self.client_id is not None and self._token is None:
            self._connect()

        headers = {'Accept': 'application/json; version={0}'.format(self._version),
                   'Content-Type': 'application/json'}
        if self._token is not None:
            headers['Authorization'] = 'Bearer {0}'.format(self._token)

        params = ''
        if 'params' in kwargs and kwargs['params'] is not None:
            params = '?{0}'.format(urllib.urlencode(kwargs['params']))
        url = '{0}{{0}}{1}'.format(self._url, params)
        if self._volatile_client is not None:
            self._volatile_client.set(self._key, self._token)

        return headers, url

    def _process(self, response, overrule_raw=False):
        """
        Processes a call result
        """
        if self._raw_response is True and overrule_raw is False:
            return response

        if response.status_code == 403:
            raise ForbiddenException('No access to the requested API')
        if response.status_code == 404:
            raise RuntimeError('The requested API could not be located')
        if response.status_code == 405:
            raise RuntimeError('Requested method not allowed')
        if response.status_code == 406:
            raise RuntimeError('The request was unacceptable: {0}'.format(response.text))
        if response.status_code == 429:
            raise RuntimeError('The requested API has rate limiting: {0}'.format(response.text))
        if response.status_code == 500:
            raise RuntimeError('Received internal server error: {0}'.format(response.text))
        try:
            return_data = response.json()
            return return_data
        except:
            raise RuntimeError('Could not parse returned data: {0}: {1}'.format(response.status_code, response.text))

    def _call(self, api, params, function, **kwargs):
        if not api.endswith('/'):
            api = '{0}/'.format(api)
        if not api.startswith('/'):
            api = '/{0}'.format(api)
        if self._volatile_client is not None:
            self._token = self._volatile_client.get(self._key)
        first_connect = self._token is None
        headers, url = self._prepare(params=params)
        try:
            return self._process(function(url=url.format(api), headers=headers, verify=self._verify, **kwargs))
        except ForbiddenException:
            if first_connect is True:  # First connect, so no token was present yet, so no need to try twice without token
                raise
            self._token = None
            headers, url = self._prepare(params=params)
            return self._process(function(url=url.format(api), headers=headers, verify=self._verify, **kwargs))

    def get(self, api, params=None):
        """
        Executes a GET call
        :param api: Specification for to fill out in the URL, eg: /vpools/<vpool_guid>/sync_vmachines
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.get)

    def post(self, api, data=None, params=None):
        """
        Executes a POST call
        :param api: Specification for to fill out in the URL, eg: /vpools/<vpool_guid>/sync_vmachines
        :param data: Data to post
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.post, data=data)

    def put(self, api, data=None, params=None):
        """
        Executes a PUT call
        :param api: Specification for to fill out in the URL, eg: /vpools/<vpool_guid>/sync_vmachines
        :param data: Data to put
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.put, data=data)

    def patch(self, api, data=None, params=None):
        """
        Executes a PATCH call
        :param api: Specification for to fill out in the URL, eg: /vpools/<vpool_guid>/sync_vmachines
        :param data: Data to patch
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.patch, data=data)

    def wait_for_task(self, task_id, timeout=None):
        """
        Waits for a task to complete
        :param task_id: Task to wait for
        :param timeout: Time to wait for task before raising
        """
        start = time.time()
        finished = False
        while finished is False:
            if timeout is not None and timeout < (time.time() - start):
                raise RuntimeError('Waiting for task {0} has timed out.'.format(task_id))
            task_metadata = self.get('/tasks/{0}/'.format(task_id))
            finished = task_metadata['status'] in ('FAILURE', 'SUCCESS')
            if finished is False:
                time.sleep(1)
            else:
                return task_metadata['successful'], task_metadata['result']

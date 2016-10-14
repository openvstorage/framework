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
Module for the OVS API client
"""
import time
import base64
import urllib
import hashlib
import logging
import requests
from requests.packages.urllib3 import disable_warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import SNIMissingWarning
logging.getLogger('urllib3').setLevel(logging.WARNING)


class HttpException(RuntimeError):
    """
    Custom Http Exception class
    """
    def __init__(self, status_code, *args, **kwargs):
        super(HttpException, self).__init__(*args, **kwargs)
        self.status_code = status_code


class ForbiddenException(HttpException):
    """
    Custom exception class
    """
    def __init__(self, *args, **kwargs):
        super(ForbiddenException, self).__init__(403, *args, **kwargs)


class NotFoundException(HttpException):
    """
    Custom NotFound Exception
    """
    def __init__(self, *args, **kwargs):
        super(NotFoundException, self).__init__(404, *args, **kwargs)


class OVSClient(object):
    """
    Represents the OVS client
    """

    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

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
        if len(response.keys()) in [1, 2] and 'error' in response:
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
            self._volatile_client.set(self._key, self._token, 300)

        return headers, url

    def _process(self, response, overrule_raw=False):
        """
        Processes a call result
        """
        if self._raw_response is True and overrule_raw is False:
            return response

        status_code = response.status_code
        parsed_output = None
        try:
            parsed_output = response.json()
        except:
            pass

        if 200 <= status_code < 300:
            return parsed_output
        else:
            message = None
            if parsed_output is not None:
                if 'error_description' in parsed_output:
                    message = parsed_output['error_description']
                if 'error' in parsed_output:
                    if message is None:
                        message = parsed_output['error']
                    else:
                        message += ' ({0})'.format(parsed_output['error'])
            else:
                messages = {401: 'No access to the requested API',
                            403: 'No access to the requested API',
                            404: 'The requested API could not be found',
                            405: 'Requested method not allowed',
                            406: 'The request was unacceptable',
                            429: 'Rate limit was hit',
                            500: 'Internal server error'}
                if status_code in messages:
                    message = messages[status_code]
            if message is None:
                message = 'Unknown error'
            if status_code in [401, 403]:
                raise ForbiddenException(message)
            elif status_code == 404:
                raise NotFoundException(message)
            else:
                raise HttpException(status_code, message)

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
            if self._volatile_client is not None:
                self._volatile_client.delete(self._key)
            if first_connect is True:  # First connect, so no token was present yet, so no need to try twice without token
                raise
            self._token = None
            headers, url = self._prepare(params=params)
            return self._process(function(url=url.format(api), headers=headers, verify=self._verify, **kwargs))
        except Exception:
            if self._volatile_client is not None:
                self._volatile_client.delete(self._key)
            raise

    def get(self, api, params=None):
        """
        Executes a GET call
        :param api: Specification to fill out in the URL, eg: /vpools/<vpool_guid>/shrink_vpool
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.get)

    def post(self, api, data=None, params=None):
        """
        Executes a POST call
        :param api: Specification to fill out in the URL, eg: /vpools/<vpool_guid>/shrink_vpool
        :param data: Data to post
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.post, data=data)

    def put(self, api, data=None, params=None):
        """
        Executes a PUT call
        :param api: Specification to fill out in the URL, eg: /vpools/<vpool_guid>/shrink_vpool
        :param data: Data to put
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.put, data=data)

    def patch(self, api, data=None, params=None):
        """
        Executes a PATCH call
        :param api: Specification to fill out in the URL, eg: /vpools/<vpool_guid>/shrink_vpool
        :param data: Data to patch
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.patch, data=data)

    def delete(self, api, params=None):
        """
        Executes a PATH call
        :param api: Specification to fill out in the URL, eg: /vpools/<vpool_guid>/
        :param params: Additional query parameters, eg: _dynamics
        """
        return self._call(api=api, params=params, function=requests.delete)

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

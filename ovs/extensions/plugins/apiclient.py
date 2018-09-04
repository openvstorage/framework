# Copyright (C) 2018 iNuron NV
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
Generic module for calling the ASD-Manager
"""

import time
import base64
import inspect
import logging
import requests
from ovs_extensions.generic.exceptions import InvalidCredentialsError, NotFoundError
from ovs.extensions.generic.logger import Logger
try:
    from requests.packages.urllib3 import disable_warnings
except ImportError:
    try:
        reload(requests)  # Required for 2.6 > 2.7 upgrade (new requests.packages module)
    except ImportError:
        pass  # So, this reload fails because of some FileNodeWarning that can't be found. But it did reload. Yay.
    from requests.packages.urllib3 import disable_warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import SNIMissingWarning
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)


class APIClient(object):
    """
    Basic API client: passes username and password in the Authorization header
    """
    _logger = Logger('extensions-plugins')

    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

    def __init__(self, ip, port, credentials, timeout, log_min_duration=1):
        # type: (str, int, tuple, int, int) -> None
        """
        Initialize an API client
        :param ip: Endpoint for the API
        :type ip: str
        :param port: Port of the endpoint
        :type port: int
        :param credentials: Tuple with client id, client secret
        :type credentials: tuple
        :param timeout: Timeout in seconds
        :type timeout: int
        :param log_min_duration: The call will get logged as slow if it took longer than this amount of seconds
        :type log_min_duration: int
        :return: None
        :rtype: NoneType
        """
        username, password = credentials
        self._base_url = 'https://{0}:{1}'.format(ip, port)
        self._base_headers = {'Authorization': 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(username, password)).strip())}

        self.timeout = timeout
        self._log_min_duration = log_min_duration

    def _call(self, method, url, data=None, json=None, timeout=None, clean=False):
        # type: (callable, str, dict, dict, int, bool) -> any
        """
        Calls the provided function and adds headings
        :param method: Method to call
        :type method: callable
        :param url: Url to call
        :type url: str
        :param data: Data to provide. This parameter will not set the JSON header so data may be interpreted differently!
        :type data: dict
        :param json: Data to provide as JSON parameters. This parameter will set the JSOn header to data will be interpreted as a JSON string
        :type json: dict
        :param timeout: Timeout to wait for a reply of the server
        :type timeout: int
        :param clean: Should the data be cleaned (metdata entries stripped from the result)
        :type clean: bool
        :return: The response
        :rtype: any
        """
        if timeout is None:
            timeout = self.timeout

        # Refresh URL / headers
        self._base_url, self._base_headers = self._refresh()

        start = time.time()
        kwargs = {'url': '{0}/{1}'.format(self._base_url, url),
                  'headers': self._base_headers,
                  'verify': False,
                  'timeout': timeout}
        # Requests library can both take in 'data' or 'json' keyword.
        # When data is given: no extra heading is added and the data is serialized as json
        # When json is given: the 'Content type: Application/json' header is added.
        # The loop is to provide both options
        for key, val in [('json', json), ('data', data)]:
            if val is not None:
                kwargs[key] = val
        response = method(**kwargs)
        if response.status_code == 404:
            msg = 'URL not found: {0}'.format(kwargs['url'])
            self._logger.error('{0}. Response: {1}'.format(msg, response))
            raise NotFoundError(msg)
        try:
            data = response.json()
        except Exception:
            raise RuntimeError(response.content)
        internal_duration = data['_duration']
        if data.get('_success', True) is False:
            error_message = data.get('_error', 'Unknown exception: {0}'.format(data))
            if error_message == 'Invalid credentials':
                raise InvalidCredentialsError(error_message)
            raise RuntimeError(error_message)
        if clean is True:
            data = self.clean(data)
        duration = time.time() - start
        if duration > self._log_min_duration:
            self._logger.info('Request "{0}" took {1:.2f} seconds (internal duration {2:.2f} seconds)'.format(inspect.stack()[1][3], duration, internal_duration))
        return data

    def _refresh(self):
        # type: () -> Tuple[str, dict]
        """
        Refresh the endpoint and credentials
        This function is called before every request
        :return: The base URL and the 'Authorization' header
        :rtype: tuple
        """
        return self._base_url, self._base_headers

    def extract_data(self, response_data, old_key=None):
        # type: (dict, Optional[str]) -> any
        """
        Extract the data from the API
        For backwards compatibility purposes (older asd-managers might not wrap their data)
        :param response_data: Data of the response
        :type response_data: dict
        :param old_key: Old key (if any) to extract
        :type old_key: str
        :return: The data
        :rtype: any
        """
        if 'data' in response_data:
            return response_data['data']
        if old_key:
            if old_key not in response_data:
                raise KeyError('{0} not present in the response data. Format might have changed'.format(old_key))
            return response_data[old_key]
        # Revert back to cleaning the response
        return self.clean(response_data)

    @classmethod
    def clean(cls, data):
        # type: (dict) -> dict
        """
        Clean data of metadata keys
        :param data: Dict with data
        :type data: dict
        :return: Cleaned data
        :rtype: dict
        """
        data_copy = data.copy()
        for key in data.iterkeys():
            if key.startswith('_'):
                del data_copy[key]
            elif isinstance(data_copy[key], dict):
                data_copy[key] = cls.clean(data_copy[key])
        return data_copy

    def get(self, url, data=None, json=None, timeout=None, clean=False):
        """
        Executes a GET call
        :param url: Url to call
        :type url: str
        :param data: Data to provide. This parameter will not set the JSON header so data may be interpreted differently!
        :type data: dict
        :param json: Data to provide as JSON parameters. This parameter will set the JSOn header to data will be interpreted as a JSON string
        :type json: dict
        :param timeout: Timeout to wait for a reply of the server
        :type timeout: int
        :param clean: Should the data be cleaned (metadata entries stripped from the result)
        :type clean: bool
        """
        return self._call(method=requests.get, url=url, data=data, json=json, clean=clean, timeout=timeout)

    def post(self, url, data=None, json=None, timeout=None, clean=False):
        """
        Executes a POST call
        :param url: Url to call
        :type url: str
        :param data: Data to provide. This parameter will not set the JSON header so data may be interpreted differently!
        :type data: dict
        :param json: Data to provide as JSON parameters. This parameter will set the JSOn header to data will be interpreted as a JSON string
        :type json: dict
        :param timeout: Timeout to wait for a reply of the server
        :type timeout: int
        :param clean: Should the data be cleaned (metadata entries stripped from the result)
        :type clean: bool
        """
        return self._call(method=requests.post, url=url, data=data, json=json, clean=clean, timeout=timeout)

    def put(self, url, data=None, json=None, timeout=None, clean=False):
        """
        Executes a PUT call
        :param url: Url to call
        :type url: str
        :param data: Data to provide. This parameter will not set the JSON header so data may be interpreted differently!
        :type data: dict
        :param json: Data to provide as JSON parameters. This parameter will set the JSOn header to data will be interpreted as a JSON string
        :type json: dict
        :param timeout: Timeout to wait for a reply of the server
        :type timeout: int
        :param clean: Should the data be cleaned (metadata entries stripped from the result)
        :type clean: bool
        """
        return self._call(method=requests.put, url=url, data=data, json=json, clean=clean, timeout=timeout)

    def patch(self, url, data=None, json=None, timeout=None, clean=False):
        """
        Executes a PATCH call
        :param url: Url to call
        :type url: str
        :param data: Data to provide. This parameter will not set the JSON header so data may be interpreted differently!
        :type data: dict
        :param json: Data to provide as JSON parameters. This parameter will set the JSOn header to data will be interpreted as a JSON string
        :type json: dict
        :param timeout: Timeout to wait for a reply of the server
        :type timeout: int
        :param clean: Should the data be cleaned (metadata entries stripped from the result)
        :type clean: bool
        """
        return self._call(method=requests.patch, url=url, data=data, json=json, clean=clean, timeout=timeout)

    def delete(self, url, data=None, json=None, timeout=None, clean=False):
        """
        Executes a DELETE call
        :param url: Url to call
        :type url: str
        :param data: Data to provide. This parameter will not set the JSON header so data may be interpreted differently!
        :type data: dict
        :param json: Data to provide as JSON parameters. This parameter will set the JSOn header to data will be interpreted as a JSON string
        :type json: dict
        :param timeout: Timeout to wait for a reply of the server
        :type timeout: int
        :param clean: Should the data be cleaned (metadata entries stripped from the result)
        :type clean: bool
        """
        return self._call(method=requests.delete, url=url, data=data, json=json, clean=clean, timeout=timeout)

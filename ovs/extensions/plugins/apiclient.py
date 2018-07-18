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

    def _call(self, method, url, data=None, timeout=None, clean=False):
        if timeout is None:
            timeout = self.timeout

        # Refresh URL / headers
        self._base_url, self._base_headers = self._refresh()

        start = time.time()
        kwargs = {'url': '{0}/{1}'.format(self._base_url, url),
                  'headers': self._base_headers,
                  'verify': False,
                  'timeout': timeout}
        if data is not None:
            kwargs['data'] = data
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
            data = self._clean(data)
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

    @classmethod
    def _clean(cls, data):
        # type: (dict) -> dict
        """
        Cleans a response
        :param data: Response data
        :type data: dict
        :return: The cleaned data
        :rtype: dict
        """
        data_copy = data.copy()  # Shallow copy is good enough since keys are discarded
        for _key in data.keys():
            if _key.startswith('_'):
                del data_copy[_key]
            elif isinstance(data_copy[_key], dict):
                data_copy[_key] = cls._clean(data_copy[_key])
        return data_copy

# Copyright 2015 CloudFounders NV
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

import urllib2
import urllib
import json
import base64
import time


class OVSClient(object):
    """
    Represents the OVS client
    """

    def __init__(self, ip, port, client_id, client_secret):
        """
        Initializes the object with credentials and connection information
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self._url = 'https://{0}:{1}/api'.format(ip, port)
        self._token = None

    def _connect(self):
        """
        Authenticates to the api
        """
        headers = {'Accept': 'application/json',
                   'Authorization': 'basic {0}'.format(base64.encodestring('{0}:{1}'.format(self.client_id, self.client_secret)).strip())}
        request = urllib2.Request('{0}/oauth2/token/'.format(self._url),
                                  data=urllib.urlencode({'grant_type': 'client_credentials'}),
                                  headers=headers)
        response = urllib2.urlopen(request).read()
        response_data = json.loads(response)
        self._token = response_data['access_token']

    def call(self, api, post_data=None, get_data=None):
        """
        Executes an API call
        """
        if self._token is None:
            self._connect()

        headers = {'Accept': 'application/json; version=1',
                   'Authorization': 'Bearer {0}'.format(self._token)}
        url_params = ''
        if get_data is not None:
            url_params = '?{0}'.format(urllib.urlencode(get_data))
        request = urllib2.Request('{0}{1}{2}'.format(self._url, api, url_params),
                                  data=post_data,
                                  headers=headers)
        response = urllib2.urlopen(request).read()
        return json.loads(response)

    def wait_for_task(self, task_id, timeout=None):
        """
        Waits for a task to complete
        """
        if self._token is None:
            self._connect()

        start = time.time()
        task_metadata = {'ready': False}
        while task_metadata['ready'] is False:
            if timeout is not None and timeout < (time.time() - start):
                raise RuntimeError('Waiting for task {0} has timed out.'.format(task_id))
            task_metadata = self.call('/tasks/{0}/'.format(task_id))
            if task_metadata['ready'] is False:
                time.sleep(1)
        return task_metadata['successful'], task_metadata['result']

# Copyright 2014 CloudFounders NV
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
Contains various decorator
"""
import json
import time
from django.http import HttpResponse
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.log.logHandler import LogHandler

logger = LogHandler('api', 'oauth2')


def json_response():
    """
    Json response wrapper
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(*args, **kw):
            """
            Wrapped function
            """
            results = f(*args, **kw)
            return_type, data = results[0], results[1]
            if len(results) == 2:
                return return_type(json.dumps(data), content_type='application/json')
            else:
                status_code = results[2]
                return return_type(json.dumps(data), content_type='application/json', status=status_code)

        new_function.__name__ = f.__name__
        return new_function
    return wrap


def limit(amount, per, timeout):
    """
    Rate-limits the decorated call
    """
    def wrap(f):
        """
        Wrapper function
        """
        def new_function(self, request, *args, **kwargs):
            """
            Wrapped function
            """
            now = time.time()
            key = 'ovs_api_limit_{0}.{1}_{2}'.format(
                f.__module__, f.__name__,
                request.META['HTTP_X_REAL_IP']
            )
            client = VolatileFactory.get_client()
            mutex = VolatileMutex(key)
            try:
                mutex.acquire()
                rate_info = client.get(key, {'calls': [],
                                             'timeout': None})
                active_timeout = rate_info['timeout']
                if active_timeout is not None:
                    if active_timeout > now:
                        return HttpResponse, {'error_code': 'rate_limit_timeout',
                                              'error': 'Rate limit timeout ({0}s remaining)'.format(round(active_timeout - now, 2))}, 429
                    else:
                        rate_info['timeout'] = None
                rate_info['calls'] = [call for call in rate_info['calls'] if call > (now - per)] + [now]
                calls = len(rate_info['calls'])
                if calls > amount:
                    rate_info['timeout'] = now + timeout
                    client.set(key, rate_info)
                    return HttpResponse, {'error_code': 'rate_limit_reached',
                                          'error': 'Rate limit reached ({0} in last {1}s)'.format(calls, per)}, 429
                client.set(key, rate_info)
            finally:
                mutex.release()
            return f(self, request, *args, **kwargs)

        new_function.__name__ = f.__name__
        return new_function
    return wrap

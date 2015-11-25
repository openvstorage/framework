# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
from django.http import HttpResponse, HttpResponseServerError
from django.contrib.auth import authenticate, login
from rest_framework.request import Request
from rest_framework.exceptions import PermissionDenied
from django.core.handlers.wsgi import WSGIRequest
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('api', 'oauth2')


def _find_request(args):
    """
    Finds the "request" object in args
    """
    for item in args:
        if isinstance(item, Request) or isinstance(item, WSGIRequest):
            return item


def auto_response():
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
            if isinstance(results, tuple) or isinstance(results, list):
                return_type, data = results[0], results[1]
                if len(results) == 2:
                    if isinstance(data, dict):
                        return return_type(json.dumps(data), content_type='application/json')
                    return return_type(data)
                else:
                    status_code = results[2]
                    if isinstance(data, dict):
                        return return_type(json.dumps(data), content_type='application/json', status=status_code)
                    return return_type(data, status=status_code)
            elif isinstance(results, HttpResponse):
                return results
            else:
                logger.error('Got invalid function return data in auto_reponse')
                return HttpResponseServerError()

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
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
                        logger.warning('Call {0} is being throttled with a wait of {1}'.format(key, active_timeout - now))
                        return HttpResponse, {'error_code': 'rate_limit_timeout',
                                              'error': 'Rate limit timeout ({0}s remaining)'.format(round(active_timeout - now, 2))}, 429
                    else:
                        rate_info['timeout'] = None
                rate_info['calls'] = [call for call in rate_info['calls'] if call > (now - per)] + [now]
                calls = len(rate_info['calls'])
                if calls > amount:
                    rate_info['timeout'] = now + timeout
                    client.set(key, rate_info)
                    logger.warning('Call {0} is being throttled with a wait of {1}'.format(key, timeout))
                    return HttpResponse, {'error_code': 'rate_limit_reached',
                                          'error': 'Rate limit reached ({0} in last {1}s)'.format(calls, per)}, 429
                client.set(key, rate_info)
            finally:
                mutex.release()
            return f(self, request, *args, **kwargs)

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function
    return wrap


def log():
    """
    Task logger
    """

    def wrap(f):
        """
        Wrapper function
        """

        def new_function(self, request, *args, **kwargs):
            """
            Wrapped function
            """
            # Log the call
            metadata = {'meta': dict((str(key), str(value)) for key, value in request.META.iteritems()),
                        'request': dict((str(key), str(value)) for key, value in request.REQUEST.iteritems()),
                        'cookies': dict((str(key), str(value)) for key, value in request.COOKIES.iteritems())}
            _logger = LogHandler.get('log', name='api')
            _logger.info('[{0}.{1}] - {2} - {3} - {4} - {5}'.format(
                f.__module__,
                f.__name__,
                getattr(request, 'client').user_guid if hasattr(request, 'client') else None,
                json.dumps(list(args)),
                json.dumps(kwargs),
                json.dumps(metadata)
            ))

            # Call the function
            return f(self, request, *args, **kwargs)

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function

    return wrap


def authenticated():
    """
    Forces an authentication run
    """

    def wrap(f):
        """
        Wrapper function
        """

        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            request = _find_request(args)
            user = authenticate(request=request, native_django=True)
            if user is None:
                raise PermissionDenied('Authentication credentials were not provided.')
            login(request, user)
            return f(*args, **kwargs)

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function

    return wrap

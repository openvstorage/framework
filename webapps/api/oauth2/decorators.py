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
Contains various decorator
"""
import json
import time
from django.contrib.auth import authenticate, login
from django.core.handlers.wsgi import WSGIRequest
from django.http import HttpResponse
from functools import wraps
from rest_framework.request import Request
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpTooManyRequestsException
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.volatilefactory import VolatileFactory


def _find_request(args):
    """
    Finds the "request" object in args
    """
    for item in args:
        if isinstance(item, Request) or isinstance(item, WSGIRequest):
            return item


def auto_response(beautify=False):
    """
    Json response wrapper
    """

    def wrap(f):
        """
        Wrapper function
        """

        @wraps(f)
        def new_function(*args, **kw):
            """
            Wrapped function
            """
            results = f(*args, **kw)
            if isinstance(results, HttpResponse):
                return results
            if isinstance(results, dict):
                if beautify is True:
                    data = json.dumps(results, indent=4, sort_keys=True)
                else:
                    data = json.dumps(results)
                return HttpResponse(data, content_type='application/json')
            return HttpResponse(results)

        return new_function
    return wrap


def limit(amount, per, timeout):
    """
    Rate-limits the decorated call
    """
    logger = Logger('oauth2')

    def wrap(f):
        """
        Wrapper function
        """

        @wraps(f)
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
            mutex = volatile_mutex(key)
            try:
                mutex.acquire()
                rate_info = client.get(key, {'calls': [],
                                             'timeout': None})
                active_timeout = rate_info['timeout']
                if active_timeout is not None:
                    if active_timeout > now:
                        logger.warning('Call {0} is being throttled with a wait of {1}'.format(key, active_timeout - now))
                        raise HttpTooManyRequestsException(error='rate_limit_timeout',
                                                           error_description='Rate limit timeout ({0}s remaining)'.format(round(active_timeout - now, 2)))
                    else:
                        rate_info['timeout'] = None
                rate_info['calls'] = [call for call in rate_info['calls'] if call > (now - per)] + [now]
                calls = len(rate_info['calls'])
                if calls > amount:
                    rate_info['timeout'] = now + timeout
                    client.set(key, rate_info)
                    logger.warning('Call {0} is being throttled with a wait of {1}'.format(key, timeout))
                    raise HttpTooManyRequestsException(error='rate_limit_reached',
                                                       error_description='Rate limit reached ({0} in last {1}s)'.format(calls, per))
                client.set(key, rate_info)
            finally:
                mutex.release()
            return f(self, request, *args, **kwargs)

        return new_function
    return wrap


def log():
    """
    Task logger
    """
    logger = Logger('oauth2')

    def wrap(f):
        """
        Wrapper function
        """

        @wraps(f)
        def new_function(self, request, *args, **kwargs):
            """
            Wrapped function
            """
            # Log the call
            metadata = {'meta': dict((str(key), str(value)) for key, value in request.META.iteritems()),
                        'request': dict((str(key), str(value)) for key, value in request.REQUEST.iteritems()),
                        'cookies': dict((str(key), str(value)) for key, value in request.COOKIES.iteritems())}
            # Stripping password traces
            for mtype in metadata:
                for key in metadata[mtype]:
                    if 'password' in key:
                        metadata[mtype][key] = '**********************'
            logger.info('[{0}.{1}] - {2} - {3} - {4} - {5}'.format(
                f.__module__,
                f.__name__,
                getattr(request, 'client').user_guid if hasattr(request, 'client') else None,
                json.dumps(list(args)),
                json.dumps(kwargs),
                json.dumps(metadata)
            ))

            # Call the function
            return f(self, request, *args, **kwargs)

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

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            request = _find_request(args)
            user = authenticate(request=request, native_django=True)
            if user is None:
                raise HttpForbiddenException(error='missing_credentials',
                                             error_description='Authentication credentials were not provided.')
            login(request, user)
            return f(*args, **kwargs)

        return new_function

    return wrap

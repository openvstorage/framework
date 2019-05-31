# Copyright (C) 2019 iNuron NV
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

import json
import time
import logging
from api.backend.toolbox import ApiToolbox
from api_flask.response import ResponseOVS
from flask import request
from functools import wraps
from ovs.dal.lists.userlist import UserList
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException,\
    HttpTooManyRequestsException, HttpUnauthorizedException, HttpUpgradeNeededException


logger = logging.getLogger(__name__)


def log(log_slow=True):
    """
    Task logger
    :param log_slow: Indicates whether a slow call should be logged
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
            logging_start = time.time()

            method_args = list(args)[:]
            method_args = method_args[method_args.index(request) + 1:]

            # Log the call
            metadata = {'request': request.get_json,
                        'cookies': request.cookies}
            # Stripping password traces
            for mtype in metadata:
                for key in metadata[mtype]:  #todo check this
                    if 'password' in key:
                        metadata[mtype][key] = '**********************'
            logger.info('[{0}.{1}] - {2} - {3} - {4} - {5}'.format(
                f.__module__,
                f.__name__,
                getattr(request, 'client').user_guid if hasattr(request, 'client') else None,
                json.dumps(method_args),
                json.dumps(kwargs),
                json.dumps(metadata)
            ))
            logging_duration = time.time() - logging_start

            # Call the function
            start = time.time()
            return_value = f(*args, **kwargs)
            duration = time.time() - start
            if duration > 5 and log_slow is True:
                logger.warning('API call {0}.{1} took {2}s'.format(f.__module__, f.__name__, round(duration, 2)))
            if isinstance(return_value, ResponseOVS):
                return_value['timings']['logging'] = [logging_duration, 'Logging']
            return return_value

        return new_function

    return wrap



def required_roles(roles):
    """
    Role validation decorator
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
            start = time.time()
            if not hasattr(request, 'user') or not hasattr(request, 'client'):
                raise HttpUnauthorizedException(error='not_authenticated',
                                                error_description='Not authenticated')
            user = UserList.get_user_by_username(request.authorization.username)
            if user is None:
                raise HttpUnauthorizedException(error='not_authenticated',
                                                error_description='Not authenticated')
            if not ApiToolbox.is_token_in_roles(request.authorization.token, roles):
                raise HttpForbiddenException(error='invalid_roles',
                                             error_description='This call requires roles: {0}'.format(', '.join(roles)))
            duration = time.time() - start
            result = f(*args, **kw)
            if isinstance(result, ResponseOVS):
                result.timings['security'] = [duration, 'Security']
            return result

        return new_function
    return wrap
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

import os
import re
import json
import math
import time
import inspect
from django.conf import settings
from django.core.handlers.wsgi import WSGIRequest
from functools import wraps
from rest_framework import status
from rest_framework.request import Request
from api.backend.exceptions import HttpUnauthorizedException, HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException, HttpTooManyRequestsException
from api.backend.toolbox import ApiToolbox
from api.helpers import OVSResponse
from ovs.dal.dataobject import DataObject
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.helpers import DalToolbox
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.log.log_handler import LogHandler

if os.environ.get('RUNNING_UNITTESTS') == 'True':
    from api.backend.serializers.mockups import FullSerializer
else:
    from api.backend.serializers.serializers import FullSerializer


def _find_request(args):
    """
    Finds the "request" object in args
    """
    for item in args:
        if isinstance(item, Request) or isinstance(item, WSGIRequest):
            return item


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
            request = _find_request(args)
            if not hasattr(request, 'user') or not hasattr(request, 'client'):
                raise HttpUnauthorizedException(error_description='Not authenticated',
                                                error='not_authenticated')
            user = UserList.get_user_by_username(request.user.username)
            if user is None:
                raise HttpUnauthorizedException(error_description='Not authenticated',
                                                error='not_authenticated')
            if not ApiToolbox.is_token_in_roles(request.token, roles):
                raise HttpForbiddenException(error_description='This call requires roles: {0}'.format(', '.join(roles)),
                                             error='invalid_roles')
            duration = time.time() - start
            result = f(*args, **kw)
            if isinstance(result, OVSResponse):
                result.timings['security'] = [duration, 'Security']
            return result

        return new_function
    return wrap


def load(object_type=None, min_version=settings.VERSION[0], max_version=settings.VERSION[-1], validator=None):
    # type: (Type[DataObject], int, int, callable) -> callable
    """
    Parameter discovery decorator
    Able to inject a couple of keywords into the decorated function:
    - Instance of the passed object type. The name of the keyword is the object_type.lower()
    - version: The parsed API version of the client
    - raw_version: The unparsed API version of the client
    - request: The Request object (WSGIRequest or Request)
    - local_storagerouter: The storagerouter where the API request came in
    :param object_type: Type of object to load
    :type object_type: Type[DataObject]
    :param min_version: Minimum api version required to access this call
    :type min_version: int
    :param max_version: Maximum api version required to access this call
    :type max_version: int
    :param validator: Extra validation function to be executed
    :type validator: callable
    :return: The wrapped function
    :rtype: callable
    """
    regex = re.compile('^(.*; )?version=(?P<version>([0-9]+|\*)?)(;.*)?$')

    if validator is not None:
        f_info = inspect.getargspec(validator)
        if f_info.defaults is None:
            validation_mandatory_vars = f_info.args[1:]
            validation_optional_vars = []
        else:
            validation_mandatory_vars = f_info.args[1:-len(f_info.defaults)]
            validation_optional_vars = f_info.args[len(validation_mandatory_vars) + 1:]
    else:
        validation_mandatory_vars = []
        validation_optional_vars = []

    def validate_get_version(request):
        # type: (Union[WSGIRequest, Request]) -> Tuple[int, str]
        """
        Validate the version and return the parsed and non parsed version passed in the request
        :param request: API request object
        :type Request: Union[WSGIRequest, Request]
        :return: The parsed and non parsed request
        :rtype: Tuple[int, str]
        :exception: HttpNotAcceptableException when the version is not within the supported versions of the api
        """
        version_match = regex.match(request.META['HTTP_ACCEPT'])
        if version_match is not None:
            version = version_match.groupdict()['version']
        else:
            version = settings.VERSION[-1]
        raw_version = version
        versions = (max(min_version, settings.VERSION[0]), min(max_version, settings.VERSION[-1]))
        if version == '*':  # If accepting all versions, it defaults to the highest one
            version = versions[1]
        version = int(version)
        if version < versions[0] or version > versions[1]:
            raise HttpNotAcceptableException(
                error_description='API version requirements: {0} <= <version> <= {1}. Got {2}'.format(versions[0],
                                                                                                      versions[1],
                                                                                                      version),
                error='invalid_version')
        return version, raw_version

    def build_new_kwargs(original_function, request, instance, version, raw_version, passed_kwargs):
        # type: (callable, Union[WSGIRequest, Request], Generic[object_type], int, str, **any) -> Tuple[dict, dict]
        """
        Convert all positional arguments to keyword arguments
        :param original_function: The orignally decorated function
        :type original_function: callable
        :param request: API request object
        :type request: Union[WSGIRequest, Request]
        :param instance: Generic[object_type]
        :param version: Parsed API version
        :type version: int
        :param raw_version: Unparsed API version
        :type raw_version: str
        :param passed_args: Args passed to the original function
        :type passed_args: tuple
        :param passed_kwargs: Kwargs passed to the original function
        :type passed_kwargs: dict
        :return: The kwargs for the original function and the kwargs for the validator
        :rtype: Tuple[dict, dict]
        """
        function_metadata = original_function.ovs_metadata
        kwargs = {}
        validator_kwargs = {}
        empty = object()
        # Special reserved keywords
        reserved = {'version': version,
                    'raw_version': raw_version,
                    'request': request,
                    'local_storagerouter': StorageRouterList.get_by_machine_id(settings.UNIQUE_ID)}
        if instance is not None:
            reserved[object_type.__name__.lower()] = instance

        for mandatory_vars, optional_vars, new_kwargs in [(function_metadata['load']['mandatory'][:], function_metadata['load']['optional'][:], kwargs),
                                                          (validation_mandatory_vars[:], validation_optional_vars[:], validator_kwargs)]:
            for keyword, value in reserved.iteritems():
                if keyword in mandatory_vars:
                    new_kwargs[keyword] = value
                    mandatory_vars.remove(keyword)

            # The rest of the parameters
            post_data = request.DATA if hasattr(request, 'DATA') else request.POST
            query_params = request.QUERY_PARAMS if hasattr(request, 'QUERY_PARAMS') else request.GET
            # Used to detect if anything was passed. Can't use None as the value passed might be None
            data_containers = [passed_kwargs, post_data, query_params]
            for parameters, mandatory in ((mandatory_vars, True), (optional_vars, False)):
                for name in parameters:
                    val = empty
                    for container in data_containers:
                        val = container.get(name, empty)
                        if val != empty:
                            break
                    if val != empty:
                        # Embrace our design flaw. The query shouldn't be json dumped separately.
                        if name == 'query':
                            val = _try_parse(val)
                        new_kwargs[name] = _try_convert_bool(val)
                    elif mandatory:
                        raise HttpNotAcceptableException(error_description='Invalid data passed: {0} is missing'.format(name),
                                                         error='invalid_data')
        return kwargs, validator_kwargs

    def _try_convert_bool(value):
        # type: (any) -> Union[bool, Type[value]]
        """
        Convert strings to boolean
        No idea why we'd ever do this but I'd prefer to keep everything running at the moment
        :param value: Value to be parsed
        :type value: any
        :return: Bool if parsable else the value
        :rtype: Union[bool, value]
        """
        if value == 'true' or value == 'True':
            return True
        if value == 'false' or value == 'False':
            return False
        return value

    def _try_parse(value):
        # type: (any) -> Union[bool, Type[value]]
        """
        Tries to parse a value to a pythonic value
        :param value: Value to be parsed
        :type value: any
        :return: Dict if parsable else the value
        :rtype: Union[dict, value]
        """
        if isinstance(value, basestring):
            try:
                return json.loads(value)
            except ValueError:
                pass
        return value

    def load_dataobject_instance(passed_kwargs):
        # type: (Dict[str, any]) -> Union[DataObject, None]
        """
        Load the dataobject instance (if need be)
        :param passed_kwargs: Key word arguments passed to the original function
        :type passed_kwargs: Dict[str, any]
        :return: The loaded instance (if any)
        :rtype: Union[DataObject, None]
        :exception HttpNotFoundException if the requested object could not be found
        """
        instance = None
        if 'pk' in passed_kwargs and object_type is not None:
            try:
                instance = object_type(passed_kwargs['pk'])
            except ObjectNotFoundException:
                raise HttpNotFoundException(error_description='The requested object could not be found',
                                            error='object_not_found')
        return instance

    def load_wrapper(f):
        """
        Wrapper function
        """

        function_info = inspect.getargspec(f)
        if function_info.defaults is None:
            mandatory_vars = function_info.args[1:]
            optional_vars = []
        else:
            mandatory_vars = function_info.args[1:-len(function_info.defaults)]
            optional_vars = function_info.args[len(mandatory_vars) + 1:]
        metadata = f.ovs_metadata if hasattr(f, 'ovs_metadata') else {}
        metadata['load'] = {'mandatory': mandatory_vars,
                            'optional': optional_vars,
                            'object_type': object_type}
        f.ovs_metadata = metadata

        @wraps(f)
        def load_inner(*args, **kwargs):
            """
            Wrapped function
            """
            request = _find_request(args)
            start = time.time()
            version, raw_version = validate_get_version(request)
            instance = load_dataobject_instance(kwargs)
            new_kwargs, validation_new_kwargs = build_new_kwargs(f, request, instance, version, raw_version, kwargs)
            # Build new kwargs
            # Execute validator
            if validator is not None:
                validator(args[0], **validation_new_kwargs)
            duration = time.time() - start
            # Call the function
            result = f(args[0], **new_kwargs)
            if isinstance(result, OVSResponse):
                result.timings['parsing'] = [duration, 'Request parsing']
            return result

        return load_inner
    return load_wrapper


def return_list(object_type, default_sort=None):
    """
    List decorator
    """

    def wrap(f):
        """
        Wrapper function
        """

        metadata = f.ovs_metadata if hasattr(f, 'ovs_metadata') else {}
        metadata['returns'] = {'parameters': {'sorting': default_sort,
                                              'paging': None,
                                              'contents': None},
                               'returns': ['list', '200'],
                               'object_type': object_type}
        f.ovs_metadata = metadata

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            request = _find_request(args)
            timings = {}

            # 1. Pre-loading request data
            start = time.time()
            sort = request.QUERY_PARAMS.get('sort')
            if sort is None and default_sort is not None:
                sort = default_sort
            sort = None if sort is None else [s for s in reversed(sort.split(','))]
            page = request.QUERY_PARAMS.get('page')
            page = int(page) if page is not None and page.isdigit() else None
            page_size = request.QUERY_PARAMS.get('page_size')
            page_size = int(page_size) if page_size is not None and page_size.isdigit() else None
            page_size = page_size if page_size in [10, 25, 50, 100] else 10
            contents = request.QUERY_PARAMS.get('contents')
            contents = None if contents is None else contents.split(',')
            timings['preload'] = [time.time() - start, 'Data preloading']

            # 2. Construct hints for decorated function (so it can provide full objects if required)
            start = time.time()
            if 'hints' not in kwargs:
                kwargs['hints'] = {}
            kwargs['hints']['full'] = sort is not None or contents is not None
            timings['hinting'] = [time.time() - start, 'Request hinting']

            # 3. Fetch data
            start = time.time()
            data_list = f(*args, **kwargs)
            guid_list = isinstance(data_list, list) and len(data_list) > 0 and isinstance(data_list[0], basestring)
            timings['fetch'] = [time.time() - start, 'Fetching data']

            # 4. Sorting
            if sort is not None:
                start = time.time()
                if guid_list is True:
                    data_list = [object_type(guid) for guid in data_list]
                    guid_list = False  # The list is converted to objects
                for sort_item in sort:
                    desc = sort_item[0] == '-'
                    field = sort_item[1 if desc else 0:]
                    data_list.sort(key=lambda e: DalToolbox.extract_key(e, field), reverse=desc)
                timings['sort'] = [time.time() - start, 'Sorting data']

            # 5. Paging
            start = time.time()
            total_items = len(data_list)
            page_metadata = {'total_items': total_items,
                             'current_page': 1,
                             'max_page': 1,
                             'page_size': page_size,
                             'start_number': min(1, total_items),
                             'end_number': total_items}
            if page is not None:
                max_page = int(math.ceil(total_items / (page_size * 1.0)))
                if page > max_page:
                    page = max_page
                if page == 0:
                    start_number = -1
                    end_number = 0
                else:
                    start_number = (page - 1) * page_size  # Index - e.g. 0 for page 1, 10 for page 2
                    end_number = start_number + page_size  # Index - e.g. 10 for page 1, 20 for page 2
                data_list = data_list[start_number: end_number]
                page_metadata.update({'current_page': max(1, page),
                                      'max_page': max(1, max_page),
                                      'start_number': start_number + 1,
                                      'end_number': min(total_items, end_number)})
            else:
                page_metadata['page_size'] = total_items
            timings['paging'] = [time.time() - start, 'Selecting current page']

            # 6. Serializing
            start = time.time()
            if contents is not None:
                if guid_list is True:
                    data_list = [object_type(guid) for guid in data_list]
                data = FullSerializer(object_type, contents=contents, instance=data_list, many=True).data
            else:
                if guid_list is False:
                    data_list = [item.guid for item in data_list]
                data = data_list
            timings['serializing'] = [time.time() - start, 'Serializing']

            if contents is not None and len(data_list) > 0:
                object_timings = {}
                for obj in data_list:
                    dynamic_timings = obj.get_timings()
                    for timing in dynamic_timings:
                        key = 'dynamic_{0}'.format(timing)
                        if key not in object_timings:
                            object_timings[key] = []
                        object_timings[key].append([dynamic_timings[timing], 'Load \'{0}\''.format(timing)])
                for key in object_timings:
                    times = [entry[0] for entry in object_timings[key]]
                    timings[key] = [sum(times), object_timings[key][0][1]]
                    timings['{0}_avg'.format(key)] = [sum(times) / len(times), '{0} (avg)'.format(object_timings[key][0][1])]
                    timings['{0}_min'.format(key)] = [min(times), '{0} (min)'.format(object_timings[key][0][1])]
                    timings['{0}_max'.format(key)] = [max(times), '{0} (max)'.format(object_timings[key][0][1])]

            result = {'data': data,
                      '_paging': page_metadata,
                      '_contents': contents,
                      '_sorting': [s for s in reversed(sort)] if sort else sort}

            # 7. Building response
            return OVSResponse(result,
                               status=status.HTTP_200_OK,
                               timings=timings)

        return new_function
    return wrap


def return_object(object_type, mode=None):
    """
    Object decorator
    """

    def wrap(f):
        """
        Wrapper function
        """

        return_status = status.HTTP_200_OK
        if mode == 'accepted':
            return_status = status.HTTP_202_ACCEPTED
        elif mode == 'created':
            return_status = status.HTTP_201_CREATED

        metadata = f.ovs_metadata if hasattr(f, 'ovs_metadata') else {}
        metadata['returns'] = {'parameters': {'contents': None},
                               'returns': ['object', str(return_status)],
                               'object_type': object_type}
        f.ovs_metadata = metadata

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            request = _find_request(args)
            timings = {}

            contents = request.QUERY_PARAMS.get('contents')
            contents = None if contents is None else contents.split(',')

            start = time.time()
            obj = f(*args, **kwargs)
            timings['fetch'] = [time.time() - start, 'Fetching data']

            obj.reset_timings()

            start = time.time()
            data = FullSerializer(object_type, contents=contents, instance=obj).data
            timings['serializing'] = [time.time() - start, 'Serializing']

            dynamic_timings = obj.get_timings()
            for timing in dynamic_timings:
                timings['dynamic_{0}'.format(timing)] = [dynamic_timings[timing], 'Load \'{0}\''.format(timing)]

            return OVSResponse(data, status=return_status, timings=timings)

        return new_function
    return wrap


def return_task():
    """
    Object decorator
    """

    def wrap(f):
        """
        Wrapper function
        """

        metadata = f.ovs_metadata if hasattr(f, 'ovs_metadata') else {}
        metadata['returns'] = {'parameters': {},
                               'returns': ['task', '200']}
        f.ovs_metadata = metadata

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            start = time.time()
            task = f(*args, **kwargs)
            return OVSResponse(task.id,
                               status=status.HTTP_200_OK,
                               timings={'launch': [time.time() - start, 'Launch task']})

        return new_function
    return wrap


def return_simple(mode=None):
    """
    Decorator to return plain data
    """

    def wrap(f):
        """
        Wrapper function
        """

        return_status = status.HTTP_200_OK
        if mode == 'accepted':
            return_status = status.HTTP_202_ACCEPTED
        elif mode == 'created':
            return_status = status.HTTP_201_CREATED

        metadata = f.ovs_metadata if hasattr(f, 'ovs_metadata') else {}
        metadata['returns'] = {'parameters': {},
                               'returns': [None, None]}
        f.ovs_metadata = metadata

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            start = time.time()
            result = f(*args, **kwargs)
            if result is None:
                return OVSResponse(status=status.HTTP_204_NO_CONTENT,
                                   timings={'load': [time.time() - start, 'Load data']})
            return OVSResponse(result,
                               status=return_status,
                               timings={'load': [time.time() - start, 'Load data']})

        return new_function

    return wrap


def limit(amount, per, timeout):
    """
    Rate-limits the decorated call
    """
    logger = LogHandler.get('api')

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

            now = time.time()
            key = 'ovs_api_limit_{0}.{1}_{2}'.format(
                f.__module__, f.__name__,
                request.META['HTTP_X_REAL_IP']
            )
            client = VolatileFactory.get_client()
            with volatile_mutex(key):
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
            return f(*args, **kwargs)

        return new_function
    return wrap


def log(log_slow=True):
    """
    Task logger
    :param log_slow: Indicates whether a slow call should be logged
    """
    logger = LogHandler.get('api')

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
            logging_start = time.time()

            method_args = list(args)[:]
            method_args = method_args[method_args.index(request) + 1:]

            # Log the call
            metadata = {'meta': dict((str(key), str(value)) for key, value in request.META.iteritems()),
                        'request': dict((str(key), str(value)) for key, value in request.REQUEST.iteritems()),
                        'cookies': dict((str(key), str(value)) for key, value in request.COOKIES.iteritems())}
            # Stripping password traces
            for mtype in metadata:
                for key in metadata[mtype]:
                    if 'password' in key:
                        metadata[mtype][key] = '**********************'
            _logger = LogHandler.get('log', name='api')
            _logger.info('[{0}.{1}] - {2} - {3} - {4} - {5}'.format(
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
            if isinstance(return_value, OVSResponse):
                return_value.timings['logging'] = [logging_duration, 'Logging']
            return return_value

        return new_function

    return wrap

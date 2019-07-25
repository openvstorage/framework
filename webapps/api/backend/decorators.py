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
import logging
from django.conf import settings
from django.core.handlers.wsgi import WSGIRequest
from functools import wraps
from rest_framework import status
from rest_framework.request import Request
from api.backend.toolbox import ApiToolbox
from api.helpers import OVSResponse
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObject
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.helpers import DalToolbox
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException,\
    HttpTooManyRequestsException, HttpUnauthorizedException, HttpUpgradeNeededException
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.volatilefactory import VolatileFactory

if os.environ.get('RUNNING_UNITTESTS') == 'True':
    from api.backend.serializers.mockups import FullSerializer
else:
    from api.backend.serializers.serializers import FullSerializer

# noinspection PyUnreachableCode
if False:
    from typing import Union, Tuple, Type, Any, Dict


logger = logging.getLogger(__name__)


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
                raise HttpUnauthorizedException(error='not_authenticated',
                                                error_description='Not authenticated')
            user = UserList.get_user_by_username(request.user.username)
            if user is None:
                raise HttpUnauthorizedException(error='not_authenticated',
                                                error_description='Not authenticated')
            if not ApiToolbox.is_token_in_roles(request.token, roles):
                raise HttpForbiddenException(error='invalid_roles',
                                             error_description='This call requires roles: {0}'.format(', '.join(roles)))
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
        :type request: Union[WSGIRequest, Request]
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
            raise HttpUpgradeNeededException(
                error_description='API version requirements: {0} <= <version> <= {1}. Got {2}'.format(versions[0],
                                                                                                      versions[1],
                                                                                                      version),
                error='invalid_version')
        return version, raw_version

    def build_new_kwargs(original_function, request, instance, version, raw_version, passed_kwargs):
        # type: (callable, Union[WSGIRequest, Request], DataObject, int, str, **any) -> Tuple[dict, dict]
        """
        Convert all positional arguments to keyword arguments
        :param original_function: The orignally decorated function
        :type original_function: callable
        :param request: API request object
        :type request: Union[WSGIRequest, Request]
        :param instance: The data object instance to inject
        :type instance: DataObject
        :param version: Parsed API version
        :type version: int
        :param raw_version: Unparsed API version
        :type raw_version: str
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
        # type: (any) -> Union[bool, Type[Any]]
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
        # type: (any) -> Union[bool, Type[Any]]
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
        # Add metadata that can be used later on
        metadata = f.ovs_metadata if hasattr(f, 'ovs_metadata') else {}
        metadata['returns'] = {'parameters': {'sorting': default_sort,
                                              'paging': None,
                                              'contents': None,
                                              'query': None},
                               'returns': ['list', '200'],
                               'object_type': object_type}
        f.ovs_metadata = metadata

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            This function will process the api request an apply:
             - Paging (only return a subset of all results)
             - Sorting (sort on properties)
             - Filtering (filter on properties)
            Request arguments for paging:
            - page: The page number for which the items should be displayed (string/int)
            - page_size: The size of the pages (string/int)
            Request arguments for sorting:
            - sort: Comma separated list of the properties to sort on. Prefix with '-' to use descending order (eg name,-description) (string)
            Request arguments for filtering: identical to DataList query params
            - query: The query to perform. See DataList execute_query method for more info
            """
            request = _find_request(args)
            timings = {}

            # 1. Pre-loading request data
            start = time.time()
            sort = request.QUERY_PARAMS.get('sort')
            query = request.QUERY_PARAMS.get('query')
            if query:
                try:
                    query = json.loads(query)
                    DataList.validate_query(query)
                except ValueError as ex:
                    raise ValueError('Query is not valid: \'{0}\''.format(str(ex)))
            if sort is None and default_sort is not None:
                sort = default_sort
            sort = None if sort is None else [s for s in reversed(sort.split(','))]
            page = request.QUERY_PARAMS.get('page')
            page = int(page) if page is not None and (isinstance(page, int) or page.isdigit()) else None
            page_size = request.QUERY_PARAMS.get('page_size')
            page_size = int(page_size) if page_size is not None and (isinstance(page_size, int) or page_size.isdigit()) else None
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
            function_result = f(*args, **kwargs)
            if isinstance(function_result, DataList):
                # The result of the function is the data subset to perform further iterations on
                # Slicing is thus not an option as it will copy the queried data and not changing the item subset
                # So instead of query on the 3 returned StorageRouter items,
                # it will select all StorageRouter items again to query on.
                # Set guids would clear the DataList and is not an option either
                # # Deep copying the structure isn't required as the data will get serialized at the end of this function
                # # No mutation is possible
                data_list = function_result
            else:
                # Has to be a normal list!
                if not isinstance(function_result, list):
                    raise ValueError('API decorated list function does not yield a list!')
                # Determine if we have guids or objects
                if len(function_result) > 0:
                    if isinstance(function_result[0], basestring):
                        # GUIDS
                        data_list = DataList(object_type, guids=function_result)
                    elif isinstance(function_result[0], DataObject):
                        # Hybrids, reuse the data already given
                        data_list = DataList(object_type)
                        hybrids_mapped_by_guid = dict((hybrid.guid, hybrid) for hybrid in function_result)
                        data_list._executed = True
                        data_list._guids = hybrids_mapped_by_guid.keys()
                        data_list._objects = hybrids_mapped_by_guid
                        data_list._data = dict([(hybrid.guid, {'guid': hybrid.guid, 'data': hybrid._data})
                                                for hybrid in hybrids_mapped_by_guid.values()])
                    else:
                        raise ValueError('API decorated list function does not yield the correct item type!')
                else:
                    data_list = DataList(object_type, guids=[])

            timings['fetch'] = [time.time() - start, 'Fetching data']

            # 4. Filtering data
            if query:
                start = time.time()
                # Use the guids from the result list as a base to query inside the functions results and apply the query
                data_list.set_query(query)
                # Trigger the query
                _ = data_list.guids
                timings['querying'] = [time.time() - start, 'Querying data']

            # 5. Sorting
            if sort:
                start = time.time()
                for sort_item in sort:
                    desc = sort_item[0] == '-'
                    field = sort_item[1 if desc else 0:]
                    data_list.sort(key=lambda e: DalToolbox.extract_key(e, field), reverse=desc)
                timings['sort'] = [time.time() - start, 'Sorting data']

            # 6. Paging
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

            # 7. Serializing
            start = time.time()
            if contents:
                data = FullSerializer(object_type, contents=contents, instance=data_list, many=True).data
            else:
                # No serializing requested. Return the guids
                data = data_list.guids
            timings['serializing'] = [time.time() - start, 'Serializing']

            # Add timings about dynamics
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

            # 8. Building response
            return OVSResponse(result,
                               status=status.HTTP_200_OK,
                               timings=timings)

        return new_function
    return wrap


def return_object(object_type, mode=None):
    """
    Object decorator to return a serialized Hybrid
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
            if not isinstance(obj, DataObject):
                raise TypeError('Returned object is not a hybrid')
            if not isinstance(obj, object_type):
                raise TypeError('Returned Hybrid is not of type {0}'.format(str(object_type)))
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


class RateLimitContainer(object):
    """
    Rate limit container object
    """
    volatile_client = VolatileFactory.get_client()

    def __init__(self, key, calls, timeout):
        # type: (str, List[float], float) -> None
        """
        Initialize a rate limit container object
        :param key: Key to save the rate limit on
        :type key: str
        :param calls: A list of timestamps that represent a function call
        :type calls: List[float]
        :param timeout: End time of a function rate limit cooldown (if any)
        :type timeout: float
        """
        self.key = key
        self.calls = calls
        self.timeout = timeout

    def get_calls(self, younger_than_timestamp):
        # type: (float) -> List[float]
        """
        Get the calls that are older than the given timestamp.
        This filters out calls that are no longer relevant
        :param younger_than_timestamp: Timestamp to check
        :type younger_than_timestamp: float
        :return:
        """
        return [call for call in self.calls if call > younger_than_timestamp]

    def timeout_exceeds(self, timestamp):
        # type: (float) -> bool
        """
        Check if the current timeout timestamp exceeds the given timestamp
        :param timestamp: Timestamp to check
        :type timestamp: float
        :return:
        """
        return self.timeout and self.timeout > timestamp

    def clear_timeout(self):
        # type: () -> None
        """
        Clears the current timeout
        :return: None
        :rtype: NoneType
        """
        self.timeout = None

    def save(self):
        # type: () -> None
        """
        Save the current rate limit configuration
        :return: None
        :rtype: NoneType
        """
        self.volatile_client.set(self.key, dict((k, v) for k, v in vars(self).iteritems() if k != 'key'))


class RateLimiter(object):
    """
    Exposes some of the rate limiting logic so that the unittests do not have to duplicate implementations
    """
    logger = logging.getLogger(__name__)
    volatile_client = VolatileFactory.get_client()

    def __init__(self, request, func, amount, per, timeout):
        # type: (WSGIRequest, callable, int, int, int) -> None
        """
        Rate-limits the decorated call
        :param request: Request object that was passed by Django
        :type request: WSGIRequest
        :param func: Decorated function
        :type func: callable
        :param amount: Amount of calls that can be handled at the same time (in seconds)
        :type amount: int
        :param per: Timeframe to ratelimit on (in seconds)
        :type per: int
        :param timeout: Cooldown period (in seconds)
        :type timeout: int
        """
        self.request = request
        self.func = func
        self.amount = amount
        self.per = per
        self.timeout = timeout

    @classmethod
    def get_rate_limit_info(cls, request, func, key=None):
        # type: (WSGIRequest, callable, Optional[str]) -> RateLimitContainer
        """
        Retrieve rate limiting info
        :param request: Request object that was passed by Django
        :type request: WSGIRequest
        :param func: Decorated function
        :type func: callable
        :param key: Optionally supply the key to fetch
        :type key: Optional[str]
        :return: The rate limting info.
        :rtype: RateLimitContainer
        """

        rate_limit_key = key or cls.build_ratelimit_key(request, func)
        return RateLimitContainer(key=rate_limit_key, **cls.volatile_client.get(rate_limit_key, {'calls': [], 'timeout': None}))

    def enforce_rate_limit(self):
        # type: () -> None
        """
        Enforce the rate limit
        :raises HttpTooManyRequestsException:
        - When the cooldown period is in configured
        - When the number of calls exceeded the threshold
        """
        now = time.time()
        rate_limit_key = self.build_ratelimit_key(self.request, self.func)
        with volatile_mutex(rate_limit_key):
            rate_info = self.get_rate_limit_info(self.request, self.func, key=rate_limit_key)
            if rate_info.timeout_exceeds(now):
                self.logger.warning('Call {0} is being throttled with a wait of {1}'.format(rate_limit_key, rate_info.timeout - now))
                raise HttpTooManyRequestsException(error='rate_limit_timeout',
                                                   error_description='Rate limit timeout ({0}s remaining)'.format(round(rate_info.timeout- now, 2)))
            rate_limit_timeframe_start = now - self.per
            # Get the relevant calling timestamps and add the current call to the calls
            calls_within_timeframe = rate_info.get_calls(rate_limit_timeframe_start) + [now]
            # Re-save the calls
            rate_info.calls = calls_within_timeframe
            try:
                if len(calls_within_timeframe) > self.amount:
                    # Rate limiting exceeded. Initiate the cooldown
                    rate_info.timeout = now + self.timeout
                    self.logger.warning('Call {0} is being throttled with a wait of {1}'.format(rate_limit_key, self.timeout))
                    raise HttpTooManyRequestsException(error='rate_limit_reached',
                                                       error_description='Rate limit reached ({0} in last {1}s)'.format(len(calls_within_timeframe),
                                                                                                                        self.per))
            finally:
                rate_info.save()

    @staticmethod
    def build_ratelimit_key(request, func):
        # type: (WSGIRequest, callable) -> str
        """
        Get the built rate limit key
        :param request: Request object that was passed by Django
        :type request: WSGIRequest
        :param func: Decorated function
        :type func: callable
        :return: The generated key
        :rtype: str
        """
        return 'ovs_api_limit_{0}.{1}_{2}'.format(func.__module__,
                                                  func.__name__,
                                                  request.META['HTTP_X_REAL_IP'])


def limit(amount, per, timeout):
    # type: (int, int, int) -> callable
    """
    Rate-limits the decorated call
    :param amount: Amount of calls that can be handled at the same time (in seconds)
    :type amount: int
    :param per: Timeframe to ratelimit on (in seconds)
    :type per: int
    :param timeout: Cooldown period (in seconds)
    :type timeout: int
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
            rate_limiter = RateLimiter(request, f, amount, per, timeout)
            rate_limiter.enforce_rate_limit()  # Will raise when the rate limit is hit
            return f(*args, **kwargs)

        return new_function
    return wrap


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
            if isinstance(return_value, OVSResponse):
                return_value.timings['logging'] = [logging_duration, 'Logging']
            return return_value

        return new_function

    return wrap

#####################
# Django Decorators #
#####################


def extended_action(methods=None, detail=None, url_path=None, url_name=None, **kwargs):
    """
    Mark a ViewSet method as a routable action.

    Set the `detail` boolean to determine if this action should apply to
    instance/detail requests or collection/list requests.

    See: https://github.com/encode/django-rest-framework/blob/master/docs/api-guide/viewsets.md#marking-extra-actions-for-routing
    Decorator to mark a 'post' action. Decorator from version 3.8.2 to use for the internal router
    """
    methods = ['get'] if (methods is None) else methods
    methods = [method.lower() for method in methods]

    assert detail is not None, "@action() missing required argument: 'detail'"

    def decorator(func):
        func.bind_to_methods = methods
        func.detail = detail
        func.url_path = url_path if url_path else func.__name__
        func.url_name = url_name if url_name else func.__name__.replace('_', '-')
        func.kwargs = kwargs
        return func
    return decorator

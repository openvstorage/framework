import re
import json
import time
import inspect
from flask import Request
from functools import wraps
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException, HttpNotFoundException,\
    HttpTooManyRequestsException, HttpUnauthorizedException, HttpUpgradeNeededException
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.lists.storagerouterlist import StorageRouterList
from api_flask.response import ResponseOVS


def _find_request(args):
    """
    Finds the "request" object in args
    """
    for item in args:
        if isinstance(item, Request):
            return item


def load(object_type=None, validator=None):
    """
    Parameter discovery decorator
    """

    def wrap(f):
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

        def _try_parse(value):
            """
            Tries to parse a value to a pythonic value
            """
            if value == 'true' or value == 'True':
                return True
            if value == 'false' or value == 'False':
                return False
            if isinstance(value, basestring):
                try:
                    return json.loads(value)
                except ValueError:
                    pass
            return value

        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            request = _find_request(args)
            start = time.time()
            new_kwargs = {}
            validation_new_kwargs = {}
            # Find out the arguments of the decorated function
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
            # Load some information
            instance = None
            if 'pk' in kwargs and object_type is not None:
                try:
                    instance = object_type(kwargs['pk'])
                except ObjectNotFoundException:
                    raise HttpNotFoundException(error='object_not_found',
                                                error_description='The requested object could not be found')
            # Build new kwargs
            for _mandatory_vars, _optional_vars, _new_kwargs in [(f.ovs_metadata['load']['mandatory'][:], f.ovs_metadata['load']['optional'][:], new_kwargs),
                                                                 (validation_mandatory_vars, validation_optional_vars, validation_new_kwargs)]:
                if 'request' in _mandatory_vars:
                    _new_kwargs['request'] = request
                    _mandatory_vars.remove('request')
                if instance is not None:
                    typename = object_type.__name__.lower()
                    if typename in _mandatory_vars:
                        _new_kwargs[typename] = instance
                        _mandatory_vars.remove(typename)
                if 'local_storagerouter' in _mandatory_vars:
                    from ovs.extensions.generic.system import System
                    machine_id = System.get_my_machine_id()
                    storagerouter = StorageRouterList.get_by_machine_id(machine_id)
                    _new_kwargs['local_storagerouter'] = storagerouter
                    _mandatory_vars.remove('local_storagerouter')
                # The rest of the mandatory parameters
                post_data = request.DATA if hasattr(request, 'DATA') else request.POST
                get_data = request.QUERY_PARAMS if hasattr(request, 'QUERY_PARAMS') else request.GET
                for name in _mandatory_vars:
                    if name in kwargs:
                        _new_kwargs[name] = kwargs[name]
                    else:
                        if name not in post_data:
                            if name not in get_data:
                                raise HttpNotAcceptableException(error='invalid_data',
                                                                 error_description='Invalid data passed: {0} is missing'.format(name))
                            _new_kwargs[name] = _try_parse(get_data[name])
                        else:
                            _new_kwargs[name] = _try_parse(post_data[name])
                # Try to fill optional parameters
                for name in _optional_vars:
                    if name in kwargs:
                        _new_kwargs[name] = kwargs[name]
                    else:
                        if name in post_data:
                            _new_kwargs[name] = _try_parse(post_data[name])
                        elif name in get_data:
                            _new_kwargs[name] = _try_parse(get_data[name])
            # Execute validator
            if validator is not None:
                validator(args[0], **validation_new_kwargs)
            duration = time.time() - start
            # Call the function
            result = f(args[0], **new_kwargs)
            if isinstance(result, ResponseOVS): #todo klopt?
                result.timings['parsing'] = [duration, 'Request parsing']
            return result

        return new_function
    return wrap

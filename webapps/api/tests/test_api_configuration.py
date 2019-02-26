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
APIConfiguration module
"""
import inspect
import unittest
from ovs_extensions.constants.modules import API_VIEWS
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.lib.plugin import PluginController


class APIConfiguration(unittest.TestCase):
    """
    This test suite will validate whether all APIs are properly decorated
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        # Some modules rely on this key, which is loaded during imp.load_source in _get_functions()
        Configuration.set(key=Configuration.EDITION_KEY, value=PackageFactory.EDITION_ENTERPRISE)

    def test_return(self):
        """
        Validates whether all API calls have a proper @return_* decorator
        """
        functions, return_exceptions = APIConfiguration._get_functions()
        errors = []
        for fun in functions:
            fun_id = self.retrieve_view_and_func_name('{0}.{1}'.format(fun.__module__, fun.__name__))
            if not hasattr(fun, 'ovs_metadata'):
                errors.append('{0} - Missing metadata'.format(fun_id))
                continue
            metadata = fun.ovs_metadata
            if 'returns' not in metadata:
                errors.append('{0} - Missing @return_* decorator'.format(fun_id))
                continue
            return_metadata = metadata['returns']
            if fun_id not in return_exceptions:
                if fun.__name__ == 'list':
                    if return_metadata['returns'][0] != 'list':
                        errors.append('{0} - List return decorator expected on a list method'.format(fun_id))
                elif fun.__name__ == 'retrieve':
                    if return_metadata['returns'][0] != 'object':
                        errors.append('{0} - Object return decorator expected on a retrieve method'.format(fun_id))
                elif fun.__name__ == 'create':
                    if return_metadata['returns'][0] != 'object':
                        errors.append('{0} - Object return decorator expected on a create method'.format(fun_id))
                    if return_metadata['returns'][1] != '201':
                        errors.append('{0} - Expected status 201 on a create method'.format(fun_id))
                elif fun.__name__ == 'partial_update':
                    if return_metadata['returns'][0] != 'object':
                        errors.append('{0} - Object return decorator expected on a partial_update method'.format(fun_id))
                    if return_metadata['returns'][1] != '202':
                        errors.append('{0} - Expected status 202 on a partial_update method'.format(fun_id))
                elif fun.__name__ == 'destroy':
                    if return_metadata['returns'][0] is not None:
                        errors.append('{0} - Would not expect a return type on a destroy method'.format(fun_id))
            if return_metadata['returns'][0] is None and return_metadata['returns'][1] is None:
                if fun.__doc__ is None:
                    errors.append('{0} - Missing docstring'.format(fun_id))
                    continue
                if ':return:' not in fun.__doc__ or ':rtype:' not in fun.__doc__:
                    errors.append('{0} - Missing return docstring'.format(fun_id))
        self.assertEqual(len(errors), 0, '{0} errors are found:\n- {1}'.format(len(errors), '\n- '.join(errors)))

    def test_load(self):
        """
        Validates whether an @load decorator is set
        """
        functions, _ = APIConfiguration._get_functions()
        errors = []
        for fun in functions:
            fun_id = '{0}.{1}'.format(fun.__module__, fun.__name__)
            if not hasattr(fun, 'ovs_metadata'):
                errors.append('{0} - Missing metadata'.format(fun_id))
                continue
            metadata = fun.ovs_metadata
            if 'load' not in metadata:
                errors.append('{0} - Missing @load decorator'.format(fun_id))
                continue
            load_metadata = metadata['load']
            parameters = load_metadata['mandatory'] + load_metadata['optional']
            missing_params = []
            if fun.__doc__ is None:
                errors.append('{0} - Missing docstring'.format(fun_id))
                continue
            for parameter in parameters:
                if ':param {0}:'.format(parameter) not in fun.__doc__:
                    missing_params.append(parameter)
                    continue
                if ':type {0}:'.format(parameter) not in fun.__doc__:
                    missing_params.append(parameter)
            if len(missing_params) > 0:
                errors.append('{0} - Missing docstring for parameters {1}'.format(fun_id, ', '.join(missing_params)))
        self.assertEqual(len(errors), 0, '{0} errors are found:\n- {1}'.format(len(errors), '\n- '.join(errors)))

    @staticmethod
    def _get_functions():
        funs = []
        return_exceptions = []
        for cls in PluginController.get_webapps():
            if hasattr(cls, 'skip_spec') and cls.skip_spec is True:
                continue
            if hasattr(cls, 'return_exceptions'):
                return_exceptions += cls.return_exceptions
            base_calls = ['list', 'retrieve', 'create', 'destroy', 'partial_update']
            funs += [func for func_name, func in inspect.getmembers(cls, predicate=inspect.ismethod)
                     if func_name in base_calls or hasattr(func, 'bind_to_methods')]
        return funs, return_exceptions

    @staticmethod
    def retrieve_view_and_func_name(full_module_path):
        # type: (str) -> str
        """
        Only retrieve the view and funcname instead of the fullpath
        :param full_module_path: The complete module path eg api.views.backend.a_view.a_func
        :type full_module_path: str
        :return: The view_func path eg a_view.a_func
        :rtype: str
        """
        return full_module_path.replace(API_VIEWS + '.', '')

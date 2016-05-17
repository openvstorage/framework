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
Interface test module
"""
import os
import inspect
import unittest


class Interfaces(unittest.TestCase):
    """
    This Interfaces test will verify whether the different hypervisor/mgmtcenter classes do share an identical
    interface. This is required to be able to keep the calling code hypervisor agnostic
    """

    def test_hypervisors(self):
        """
        Validates hypervisor equality
        """
        self._test_module('hypervisors')

    def test_mgmtcenters(self):
        """
        Validates the mgmtcenter equality
        """
        self._test_module('mgmtcenters')

    def _test_module(self, module_name):
        """
        Tests a complete module
        """
        hypervisors = []
        overview = {}
        classes = Interfaces._get_classes(module_name)
        for current_class in classes:
            hypervisors.append(current_class.__name__)
            for member in inspect.getmembers(current_class, inspect.ismethod):
                function = member[1]
                fname = function.__name__
                if fname.startswith('_') and fname not in ['__init__']:
                    # single underscore (private names) are ignored
                    # __init__ method MUST be identical
                    continue
                if fname not in overview:
                    overview[fname] = [False for _ in range(len(hypervisors) + 2)]
                function_info = inspect.getargspec(function)
                function_parameters = function_info.args[1:]
                overview[fname][hypervisors.index(current_class.__name__)] = function_parameters
        for function in overview:
            overview[function][2] = overview[function][0] == overview[function][1]
        Interfaces._print_table(overview, hypervisors + ['Equal parameters'])
        all_ok = True
        for function in overview:
            for item in overview[function]:
                if item is False:
                    all_ok = False
        self.assertTrue(all_ok, 'Not all functions in the {0} are equal'.format(module_name))

    @staticmethod
    def _print_table(overview, headers):
        y_entries = overview.keys()
        y_width = max(len(entry) for entry in y_entries)

        print '\n'
        print ' ' * y_width + ' | ' + ' | '.join(headers)
        print '-' * y_width + '-+-' + '-+-'.join('-' * len(header) for header in headers)
        for entry in overview:
            data = []
            for header in headers:
                data.append(('x' if overview[entry][headers.index(header)] is not False else ' ') + ' ' * (len(header) - 1))
            print entry + ' ' * (y_width - len(entry)) + ' | ' + ' | '.join(data)
        print

    @staticmethod
    def _get_classes(module_name):
        """
        Returns the classes in the given module
        """
        classes = []
        path = '/'.join([os.path.dirname(__file__), '../' + module_name])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = inspect.imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        the_class = member[1]
                        classes.append(the_class)
        return classes

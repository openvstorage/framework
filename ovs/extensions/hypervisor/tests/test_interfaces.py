#!/usr/bin/env python2
#  Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Interface test module
"""
import os
import imp
import inspect
from unittest import TestCase


class Interfaces(TestCase):
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
        path = os.path.join(os.path.dirname(__file__), '../' + module_name)
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        the_class = member[1]
                        classes.append(the_class)
        return classes

if __name__ == '__main__':
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Interfaces)
    unittest.TextTestRunner(verbosity=2).run(suite)

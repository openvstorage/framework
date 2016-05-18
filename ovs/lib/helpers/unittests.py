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
Class used to execute unit tests
Limitations:
    Only test-classes inheriting from unittest.TestCase can be executed
    Only test-modules which are in a directory called 'tests' can be executed
"""

import os
import sys
import time
import inspect
import unittest


class UnitTest(object):
    """
    Class to execute all unit tests or a subset of tests
    """
    _test_info = {}
    _OVS_PATH = '/opt/OpenvStorage'

    def __init__(self):
        """
        Initialize a UnitTest instance
        """
        raise Exception('Static class, cannot be instantiated')

    @staticmethod
    def _sec_to_readable(seconds):
        """
        Parse the seconds to hours, minutes, seconds
        :param seconds: Amount of seconds
        :type seconds: float
        :return: Human readable string
        :rtype: str
        """
        seconds = int(seconds)
        if seconds < 1:
            return '< 1 second'
        hours = seconds / 3600
        rest = seconds % 3600
        minutes = rest / 60
        seconds = rest % 60
        if hours > 0:
            return '{0} hour{1}, {2} minute{3} and {4} second{5}'.format(hours, '' if hours == 1 else 's', minutes, '' if minutes == 1 else 's', seconds, '' if seconds == 1 else 's')
        elif minutes > 0:
            return '{0} minute{1} and {2} second{3}'.format(minutes, '' if minutes == 1 else 's', seconds, '' if seconds == 1 else 's')
        else:
            return '{0} second{1}'.format(seconds, '' if seconds == 1 else 's')

    @staticmethod
    def _gather_test_info(directories=None):
        """
        Retrieve all test classes recursively in the specified directories
        :param directories: Directories to recursively check
        :type directories: list

        :return: None
        """
        unittest.running_tests = True
        if directories is None:
            directories = [UnitTest._OVS_PATH]
        if isinstance(directories, str):
            directories = [directories]
        if not isinstance(directories, list):
            raise ValueError('Directories should be a list or string')

        UnitTest._test_info = {}
        for directory in directories:
            directory = directory.rstrip('/')
            for root, dirs, files in os.walk(directory):
                if root.startswith('{0}/ci'.format(UnitTest._OVS_PATH)):  # Skip autotests
                    continue

                if root.endswith('tests'):
                    for filename in files:
                        if not filename.endswith('.py'):
                            continue
                        name = filename.replace('.py', '')
                        filepath = os.path.join(root, filename)
                        try:
                            module = inspect.imp.load_source(name, filepath)
                        except Exception as ex:
                            print 'Test file {0} could not be loaded. Error: {1}'.format(filepath, ex)
                            continue
                        filepath = filepath.replace('.py', '')
                        for member in inspect.getmembers(module):
                            if inspect.isclass(member[1]) and \
                               member[1].__module__ == name and \
                               'TestCase' in [base.__name__ for base in member[1].__bases__]:
                                    class_name = member[0]
                                    class_cl = member[1]
                                    full_class_path = '{0}.{1}'.format(filepath, class_name)

                                    if filepath not in UnitTest._test_info:
                                        UnitTest._test_info[filepath] = {'tests': unittest.TestLoader().loadTestsFromModule(module),
                                                                         'use_case': 'test-module'}

                                    if full_class_path not in UnitTest._test_info:
                                        UnitTest._test_info[full_class_path] = {'tests': unittest.TestLoader().loadTestsFromTestCase(class_cl),
                                                                                'use_case': 'test-class'}

                                    for test_case in unittest.TestLoader().getTestCaseNames(class_cl):
                                        full_test_path = '{0}.{1}:{2}'.format(filepath, class_name, test_case)
                                        UnitTest._test_info[full_test_path] = {'tests': unittest.TestLoader().loadTestsFromName(test_case, class_cl),
                                                                               'use_case': 'test-case'}

    @staticmethod
    def list_cases_for_file(file_path):
        """
        List all test cases for a test class
        :param file_path: Class to list the tests for
        :type file_path: TestCaseClass

        :return: All test cases
        :rtype: list
        """
        if not file_path.endswith('.py'):
            file_path += '.py'
        UnitTest._gather_test_info(directories=os.path.dirname(file_path))
        file_path = file_path.replace('.py', '')
        return sorted([test_name for test_name in UnitTest._test_info if test_name.split('.')[0] == file_path and ':' in test_name])

    @staticmethod
    def list_tests(directories=None, print_tests=False):
        """
        List all the tests found on the system or in the directories specified
        :param directories: Directories to check for tests
        :type directories: list

        :param print_tests: Print the tests (Used by /usr/bin/ovs)
        :type print_tests: bool

        :return: All tests found
        :rtype: list
        """
        UnitTest._gather_test_info(directories=directories)
        tests = sorted(key for key in UnitTest._test_info if ':' not in key and '.' not in key)
        if print_tests is True:
            for test in tests:
                print test
        return tests

    @staticmethod
    def run_tests(tests=None):
        """
        Execute the tests specified or all if no tests provided
        :param tests: Tests to execute
                      /opt/OpenvStorage/ovs/dal/tests/test_basic
                      /opt/OpenvStorage/ovs/dal/tests/test_basic.Basic
                      /opt/OpenvStorage/ovs/dal/tests/test_basic.Basic:test_recursive
        :type tests: list

        :return: None
        """
        if tests is None:  # Put all test files and their classes in custom dict
            tests = UnitTest.list_tests()
        else:  # Validate the specified tests
            UnitTest._gather_test_info()
            errors = []
            if isinstance(tests, str):
                tests = [tests]
            if not isinstance(tests, list):
                raise ValueError('Tests should be a list')

            for test in tests:
                if test not in UnitTest._test_info:
                    errors.append('Test {0} is not a valid test file, class or case'.format(test))
            if len(errors) > 0:
                raise ValueError('Following errors found:\n - {0}'.format('\n - '.join(errors)))

        # Execute the tests
        test_results = ['############', '# OVERVIEW #', '############', '']
        start_all = time.time()
        total_tests = 0.0
        total_error = 0
        total_success = 0
        total_failure = 0
        for test in tests:
            start_test = time.time()
            text_string = '# Processing {0} {1} #'.format(UnitTest._test_info[test]['use_case'], test)
            print '\n\n\n{0}\n{1}\n{0}\n'.format(len(text_string) * '#', text_string, len(text_string) * '#')
            tests_to_run = UnitTest._test_info[test]['tests']
            test_amount = tests_to_run.countTestCases()
            result = unittest.TextTestRunner(verbosity=2).run(tests_to_run)
            test_results.append('  - Module: {0}  ({1} test{2})'.format(test.split('.')[0], test_amount, '' if test_amount == 1 else 's'))
            test_results.append('    - DURATION: {0}'.format(UnitTest._sec_to_readable(time.time() - start_test)))
            test_results.append('    - SUCCESS: {0}'.format(test_amount - len(result.errors) - len(result.failures)))

            total_tests += test_amount
            total_success += test_amount - len(result.errors) - len(result.failures)
            if len(result.failures) > 0:
                total_failure += len(result.failures)
                test_results.append('    - FAILURE: {0}'.format(len(result.failures)))
                for failure in result.failures:
                    test_results.append('      - Class: {0}, Test: {1}, Message: {2}'.format(failure[0].id().split('.')[-2], failure[0].id().split('.')[-1], failure[1].splitlines()[-1]))
            if len(result.errors) > 0:
                total_error += len(result.errors)
                test_results.append('    - ERRORS: {0}'.format(len(result.errors)))
                for error in result.errors:
                    test_results.append('      - Class: {0}, Test: {1}, Message: {2}'.format(error[0].id().split('.')[-2], error[0].id().split('.')[-1], error[1].splitlines()[-1]))
            test_results.append('')
        if len(tests) > 1:
            test_results.insert(4, '')
            if total_error > 0:
                test_results.insert(4, '    - ERROR: {0} / {1} ({2:.2f} %)'.format(total_error, int(total_tests), total_error / total_tests * 100))
            if total_failure > 0:
                test_results.insert(4, '    - FAILURE: {0} / {1} ({2:.2f} %)'.format(total_failure, int(total_tests), total_failure / total_tests * 100))
            test_results.insert(4, '    - SUCCESS: {0} / {1} ({2:.2f} %)'.format(total_success, int(total_tests), total_success / total_tests * 100))
            test_results.insert(4, '  - Total amount of tests: {0}'.format(int(total_tests)))
            test_results.insert(4, '  - Total duration: {0}'.format(UnitTest._sec_to_readable(time.time() - start_all)))
        print '\n\n\n{0}'.format('\n'.join(test_results))
        unittest.running_tests = False
        sys.exit(0 if total_tests == total_success else 1)

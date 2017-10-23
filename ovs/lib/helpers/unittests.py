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
    _invalid_test_modules = []
    _ERROR = '\033[91mERROR\033[0m'
    _SUCCESS = '\033[92mSUCCESS\033[0m'
    _FAILURE = '\033[93mFAILURE\033[0m'
    _OVS_PATH = '/opt/OpenvStorage'

    def __init__(self):
        """
        Initialize a UnitTest instance
        """
        raise Exception('Static class, cannot be instantiated')

    @staticmethod
    def _sec_to_readable(seconds, precision=0):
        """
        Parse the seconds to hours, minutes, seconds
        :param seconds: Amount of seconds
        :type seconds: float
        :param precision: Amount of digits after comma
        :type precision: int
        :return: Human readable string
        :rtype: str
        """
        if seconds < 1:
            return '< 1 second'
        hours = int(seconds / 3600)
        rest = seconds % 3600
        minutes = int(rest / 60)
        seconds = '{{0:.{0}f}}'.format(precision).format(rest % 60)
        if hours > 0:
            return '{0} hour{1}, {2} minute{3} and {4} second{5}'.format(hours, '' if hours == 1 else 's', minutes, '' if minutes == 1 else 's', seconds, '' if seconds == 1 else 's')
        elif minutes > 0:
            return '{0} minute{1} and {2} second{3}'.format(minutes, '' if minutes == 1 else 's', seconds, '' if seconds == 1 else 's')
        else:
            return '{0} second{1}'.format(seconds, '' if seconds == 1 else 's')

    @staticmethod
    def _gather_test_info(directories=None, silent_invalid_modules=False):
        """
        Retrieve all test classes recursively in the specified directories
        :param directories: Directories to recursively check
        :type directories: list
        :param silent_invalid_modules: Ignore output for invalid modules
        :type silent_invalid_modules: bool
        :return: None
        :rtype: NoneType
        """
        os.environ['RUNNING_UNITTESTS'] = 'True'
        if directories is None:
            directories = [UnitTest._OVS_PATH]
        if isinstance(directories, str):
            directories = [directories]
        if not isinstance(directories, list):
            raise ValueError('Directories should be a list or string')

        UnitTest._test_info = {}
        UnitTest._failed_test_classes = {}
        for directory in directories:
            directory = directory.rstrip('/')
            for root, dirs, files in os.walk(directory):
                if root.startswith('{0}/ci'.format(UnitTest._OVS_PATH)):  # Skip autotests
                    continue

                if root.endswith('tests'):
                    for filename in files:
                        if not filename.endswith('.py') or filename == '__init__.py':
                            continue
                        name = filename.replace('.py', '')
                        filepath = os.path.join(root, filename)
                        try:
                            mod = inspect.imp.load_source(name, filepath)
                        except Exception as ex:
                            if silent_invalid_modules is False:
                                print 'Test file {0} could not be loaded. Error: {1}'.format(filepath, ex)
                            UnitTest._invalid_test_modules.append(filepath)
                            continue
                        filepath = filepath.replace('.py', '')
                        for member in inspect.getmembers(mod, predicate=inspect.isclass):
                            if member[1].__module__ == name and 'TestCase' in [base.__name__ for base in member[1].__bases__]:
                                    class_name = member[0]
                                    class_cl = member[1]
                                    full_class_path = '{0}.{1}'.format(filepath, class_name)

                                    if filepath not in UnitTest._test_info:
                                        UnitTest._test_info[filepath] = {'tests': unittest.TestLoader().loadTestsFromModule(mod),
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
    def list_tests(directories=None, print_tests=False, silent_invalid_modules=False):
        """
        List all the tests found on the system or in the directories specified
        :param directories: Directories to check for tests
        :type directories: list
        :param print_tests: Print the tests (Used by /usr/bin/ovs)
        :type print_tests: bool
        :param silent_invalid_modules: Ignore output for invalid modules
        :type silent_invalid_modules: bool
        :return: All tests found
        :rtype: list
        """
        UnitTest._gather_test_info(directories=directories, silent_invalid_modules=silent_invalid_modules)
        tests = sorted(key for key in UnitTest._test_info if ':' not in key and '.' not in key)
        if print_tests is True:
            for test in tests:
                print test
        return tests

    @staticmethod
    def run_tests(tests=None, add_averages=False):
        """
        Execute the tests specified or all if no tests provided
        :param tests: Tests to execute
                      /opt/OpenvStorage/ovs/dal/tests/test_basic
                      /opt/OpenvStorage/ovs/dal/tests/test_basic.Basic
                      /opt/OpenvStorage/ovs/dal/tests/test_basic.Basic:test_recursive
        :type tests: list
        :param add_averages: Add average timings for each test module
        :type add_averages: bool
        :return: None
        :rtype: NoneType
        """
        failed_modules = []
        if tests is None:  # Put all test files and their classes in custom dict
            tests_to_execute = UnitTest.list_tests()
            failed_modules = UnitTest._invalid_test_modules
        else:  # Validate the specified tests
            UnitTest._gather_test_info(silent_invalid_modules=True)
            errors = []
            if isinstance(tests, str):
                tests = tests.split(',')
            if not isinstance(tests, list):
                raise ValueError('Tests should be a list')

            sorted_tests = sorted(UnitTest._test_info.keys())
            tests_to_execute = []
            for test in tests:
                found_tests = False
                if test in sorted_tests:
                    found_tests = True
                    tests_to_execute.append(test)
                else:
                    for sorted_test in sorted_tests:
                        if '.' in sorted_test:
                            continue
                        if sorted_test.startswith(test) and os.path.isdir(test):
                            found_tests = True
                            tests_to_execute.append(sorted_test)
                if found_tests is False:
                    errors.append('Test {0} is not a valid test file, class or case'.format(test))
            if len(errors) > 0:
                raise ValueError('Following errors found:\n - {0}'.format('\n - '.join(errors)))

        # Removing tests which might be a subset of another 'test_to_execute'
        tests_to_execute = sorted(list(set(tests_to_execute)))
        for test in list(tests_to_execute):
            if '.' in test and test.split('.')[0] in tests_to_execute:
                tests_to_execute.remove(test)
            if ':' in test and test.split(':')[0] in tests_to_execute:
                tests_to_execute.remove(test)

        # Execute the tests
        test_results = ['############', '# OVERVIEW #', '############', '']
        averages = {}
        start_all = time.time()
        total_tests = 0.0
        total_error = 0
        total_success = 0
        total_failure = 0
        for test in tests_to_execute:
            start_test = time.time()
            text_string = '# Processing {0} {1} #'.format(UnitTest._test_info[test]['use_case'], test)
            print '\n\n\n{0}\n{1}\n{0}\n'.format(len(text_string) * '#', text_string)
            tests_to_run = UnitTest._test_info[test]['tests']
            test_amount = tests_to_run.countTestCases()
            result = unittest.TextTestRunner(verbosity=2).run(tests_to_run)
            duration = time.time() - start_test
            specification = 'TestCase' if ':' in test else 'TestClass' if '.' in test else 'TestModule'
            test_line = '  - {0}: {1}  ({2} test{3})'.format(specification, test, test_amount, '' if test_amount == 1 else 's')
            averages[test_line] = duration / test_amount if test_amount > 0 else 0
            test_results.append(test_line)
            test_results.append('    - DURATION: {0}'.format(UnitTest._sec_to_readable(duration)))
            test_results.append('    - {0}: {1}'.format(UnitTest._SUCCESS, test_amount - len(result.errors) - len(result.failures)))

            total_tests += test_amount
            total_success += test_amount - len(result.errors) - len(result.failures)
            if len(result.failures) > 0:
                total_failure += len(result.failures)
                test_results.append('    - {0}: {1}'.format(UnitTest._FAILURE, len(result.failures)))
                for failure in result.failures:
                    test_results.append('      - Class: {0}, Test: {1}, Message: {2}'.format(failure[0].id().split('.')[-2], failure[0].id().split('.')[-1], failure[1].splitlines()[-1]))
            if len(result.errors) > 0:
                total_error += len(result.errors)
                test_results.append('    - {0}: {1}'.format(UnitTest._ERROR, len(result.errors)))
                for error in result.errors:
                    test_results.append('      - Class: {0}, Test: {1}, Message: {2}'.format(error[0].id().split('.')[-2], error[0].id().split('.')[-1], error[1].splitlines()[-1]))
            test_results.append('')

        if len(tests_to_execute) > 1 or len(failed_modules) > 1:
            test_results.append('')
            test_results.append('###########')
            test_results.append('# SUMMARY #')
            test_results.append('###########')
            test_results.append('')
            if len(tests_to_execute) > 1:
                test_results.append('  - Total amount of tests: {0}'.format(int(total_tests)))
                test_results.append('  - Total duration: {0}'.format(UnitTest._sec_to_readable(time.time() - start_all)))
                test_results.append('    - {0}: {1} / {2} ({3:.2f} %)'.format(UnitTest._SUCCESS, total_success, int(total_tests), total_success / total_tests * 100))
                if total_failure > 0:
                    test_results.append('    - {0}: {1} / {2} ({3:.2f} %)'.format(UnitTest._FAILURE, total_failure, int(total_tests), total_failure / total_tests * 100))
                if total_error > 0:
                    test_results.append('    - {0}: {1} / {2} ({3:.2f} %)'.format(UnitTest._ERROR, total_error, int(total_tests), total_error / total_tests * 100))
            if len(failed_modules) > 1:
                test_results.append('  - {0}: {1} invalid test modules'.format(UnitTest._ERROR, len(failed_modules)))
            test_results.append('')

        if add_averages is True:
            longest_duration = max(averages.values())
            for index, line in enumerate(test_results[:]):
                if line in averages:
                    average_duration = averages[line]
                    if average_duration > 1:
                        new_line = '{0} - {1} per test)'.format(line[:-1], UnitTest._sec_to_readable(average_duration, 2))
                        if average_duration == longest_duration and len(averages) > 1:
                            new_line = '{0}   \033[91mSLOWEST\033[0m'.format(new_line)
                        test_results.remove(line)
                        test_results.insert(index, new_line)

        print '\n\n\n{0}'.format('\n'.join(test_results))
        os.environ['RUNNING_UNITTESTS'] = 'False'
        success = total_tests == total_success and len(failed_modules) == 0
        sys.exit(0 if success else 1)

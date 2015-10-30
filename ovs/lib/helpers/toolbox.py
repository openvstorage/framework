# Copyright 2015 iNuron NV
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
Module containing certain helper classes providing various logic
"""
import os
import re
import imp
import sys
import random
import string
import inspect
import subprocess
import time
from ovs.dal.helpers import Toolbox as HelperToolbox


class Toolbox(object):
    """
    Generic class for various methods
    """

    regex_ip = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    regex_vpool = re.compile('^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$')
    regex_mountpoint = re.compile('^(/[a-zA-Z0-9\-_\.]+)+/?$')
    compiled_regex_type = type(re.compile('someregex'))

    @staticmethod
    def fetch_hooks(hook_type, hook):
        """
        Load hooks
        """
        functions = []
        path = '{0}/../'.format(os.path.dirname(__file__))
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py') and filename != '__init__.py':
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        for submember in inspect.getmembers(member[1]):
                            if hasattr(submember[1], 'hooks') \
                                    and isinstance(submember[1].hooks, dict) \
                                    and hook_type in submember[1].hooks \
                                    and isinstance(submember[1].hooks[hook_type], list) \
                                    and hook in submember[1].hooks[hook_type]:
                                functions.append(submember[1])
        return functions

    @staticmethod
    def verify_required_params(required_params, actual_params):
        error_messages = []
        for required_key, key_info in required_params.iteritems():
            expected_type = key_info[0]
            expected_value = key_info[1]
            optional = len(key_info) == 3 and key_info[2] is False

            if optional is True and (required_key not in actual_params or actual_params[required_key] in ('', None)):
                continue

            if required_key not in actual_params:
                error_messages.append('Missing required param "{0}"'.format(required_key))
                continue

            actual_value = actual_params[required_key]
            if HelperToolbox.check_type(actual_value, expected_type)[0] is False:
                error_messages.append('Required param "{0}" is of type "{1}" but we expected type "{2}"'.format(required_key, type(actual_value), expected_type))
                continue

            if expected_value is None:
                continue

            if expected_type == list:
                if type(expected_value) == Toolbox.compiled_regex_type:  # List of strings which need to match regex
                    for item in actual_value:
                        if not re.match(expected_value, item):
                            error_messages.append('Required param "{0}" has an item "{1}" which does not match regex "{2}"'.format(required_key, item, expected_value.pattern))
            elif expected_type == dict:
                Toolbox.verify_required_params(expected_value, actual_params[required_key])
            elif expected_type == int:
                if isinstance(expected_value, list) and actual_value not in expected_value:
                    error_messages.append('Required param "{0}" with value "{1}" should be 1 of the following: {2}'.format(required_key, actual_value, expected_value))
                if isinstance(expected_value, dict):
                    minimum = expected_value.get('min', sys.maxint * -1)
                    maximum = expected_value.get('max', sys.maxint)
                    if not minimum <= actual_value <= maximum:
                        error_messages.append('Required param "{0}" with value "{1}" should be in range: {2} - {3}'.format(required_key, actual_value, minimum, maximum))
            else:
                if HelperToolbox.check_type(expected_value, list)[0] is True and actual_value not in expected_value:
                    error_messages.append('Required param "{0}" with value "{1}" should be 1 of the following: {2}'.format(required_key, actual_value, expected_value))
                elif HelperToolbox.check_type(expected_value, Toolbox.compiled_regex_type)[0] is True and not re.match(expected_value, actual_value):
                    error_messages.append('Required param "{0}" with value "{1}" does not match regex "{2}"'.format(required_key, actual_value, expected_value.pattern))
        if error_messages:
            raise RuntimeError('\n' + '\n'.join(error_messages))

    @staticmethod
    def get_hash(length=16):
        """
        Generates a random hash
        """
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

    @staticmethod
    def retry_client_run(client, command, max_count=5, time_sleep=5, logger=None):
        """
        Retry a client run command, catch CalledProcessError
        """
        cpe = None
        retry = 0
        while retry < max_count:
            try:
                return client.run(command)
            except subprocess.CalledProcessError as cpe:
                if logger:
                    logger.error(cpe)
                time.sleep(time_sleep)
                retry += 1
        if cpe:
            raise cpe

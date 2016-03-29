# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
from ovs.extensions.services.service import ServiceManager


class Toolbox(object):
    """
    Generic class for various methods
    """

    regex_ip = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    regex_guid = re.compile('^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$')
    regex_vpool = re.compile('^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$')
    regex_preset = re.compile('^[0-9a-zA-Z][a-zA-Z0-9]{1,18}[a-zA-Z0-9]$')
    regex_mountpoint = re.compile('^(/[a-zA-Z0-9\-_\.]+)+/?$')
    compiled_regex_type = type(re.compile('some_regex'))

    @staticmethod
    def fetch_hooks(hook_type, hook):
        """
        Load hooks
        :param hook_type: Type of hook, can be update, setup, license
        :param hook: Sub-component of hook type, Eg: pre-install, post-install, ...
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
    def verify_required_params(required_params, actual_params, exact_match=False):
        """
        Verify whether the actual parameters match the required parameters
        :param required_params: Required parameters which actual parameters have to meet
        :param actual_params: Actual parameters to check for validity
        :param exact_match: Keys of both dictionaries must be identical
        :return: None
        """
        error_messages = []
        if not isinstance(required_params, dict) or not isinstance(actual_params, dict):
            raise RuntimeError('Required and actual parameters must be of type dictionary')

        if exact_match is True:
            for key in set(actual_params.keys()).difference(required_params.keys()):
                error_messages.append('Missing key "{0}" in required_params'.format(key))

        for required_key, key_info in required_params.iteritems():
            expected_type = key_info[0]
            expected_value = key_info[1]
            optional = len(key_info) == 3 and key_info[2] is False

            if optional is True and (required_key not in actual_params or actual_params[required_key] in ('', None)):
                continue

            if required_key not in actual_params:
                error_messages.append('Missing required param "{0}" in actual parameters'.format(required_key))
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
        :param length: Length of hash to generate
        :return: Randomly generated hash of length characters
        """
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

    @staticmethod
    def retry_client_run(client, command, max_count=5, time_sleep=5, logger=None):
        """
        Retry a client run command, catch CalledProcessError
        :param client: SSHClient
        :param command: Command to retry
        :param max_count: Maximum retries
        :param time_sleep: Seconds of sleep in between tries
        :param logger: LogHandler Object
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

    @staticmethod
    def change_service_state(client, name, state, logger=None):
        """
        Starts/stops/restarts a service
        :param client: SSHClient on which to connect and change service state
        :param name: Name of the service
        :param state: State to put the service in
        :param logger: LogHandler Object
        """
        action = None
        # Enable service before changing the state
        status = ServiceManager.is_enabled(name, client=client)
        if status is False:
            if logger is not None:
                logger.debug('  {0:<15} - Enabling service {1}'.format(client.ip, name))
            ServiceManager.enable_service(name, client=client)

        status = ServiceManager.get_service_status(name, client=client)
        if status is False and state in ['start', 'restart']:
            if logger is not None:
                logger.debug('  {0:<15} - Starting service {1}'.format(client.ip, name))
            ServiceManager.start_service(name, client=client)
            action = 'started'
        elif status is True and state == 'stop':
            if logger is not None:
                logger.debug('  {0:<15} - Stopping service {1}'.format(client.ip, name))
            ServiceManager.stop_service(name, client=client)
            action = 'stopped'
        elif status is True and state == 'restart':
            if logger is not None:
                logger.debug('  {0:<15} - Restarting service {1}'.format(client.ip, name))
            ServiceManager.restart_service(name, client=client)
            action = 'restarted'

        if action is None:
            print '  [{0}] {1} already {2}'.format(client.ip, name, 'running' if status is True else 'halted')
        else:
            logger.debug('  {0:<15} - Service {1} {2}'.format(client.ip, name, action))
            print '  [{0}] {1} {2}'.format(client.ip, name, action)

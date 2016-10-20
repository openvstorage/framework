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
Module containing certain helper classes providing various logic
"""
import os
import re
import imp
import sys
import time
import random
import string
import inspect
import subprocess
from celery.schedules import crontab
from ovs.dal.helpers import Toolbox as HelperToolbox
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.services.service import ServiceManager
from ovs.log.log_handler import LogHandler


class Toolbox(object):
    """
    Generic class for various methods
    """

    regex_ip = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    regex_guid = re.compile('^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$')
    regex_vpool = re.compile('^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$')
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
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py') and filename != '__init__.py':
                name = filename.replace('.py', '')
                module = imp.load_source(name, '/'.join([path, filename]))
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
        :type required_params: dict

        :param actual_params: Actual parameters to check for validity
        :type actual_params: dict

        :param exact_match: Keys of both dictionaries must be identical
        :type exact_match: bool

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

            mandatory_or_optional = 'Optional' if optional is True else 'Mandatory'
            actual_value = actual_params[required_key]
            if HelperToolbox.check_type(actual_value, expected_type)[0] is False:
                error_messages.append('{0} param "{1}" is of type "{2}" but we expected type "{3}"'.format(mandatory_or_optional, required_key, type(actual_value), expected_type))
                continue

            if expected_value is None:
                continue

            if expected_type == list:
                if type(expected_value) == Toolbox.compiled_regex_type:  # List of strings which need to match regex
                    for item in actual_value:
                        if not re.match(expected_value, item):
                            error_messages.append('{0} param "{1}" has an item "{2}" which does not match regex "{3}"'.format(mandatory_or_optional, required_key, item, expected_value.pattern))
            elif expected_type == dict:
                Toolbox.verify_required_params(expected_value, actual_params[required_key])
            elif expected_type == int:
                if isinstance(expected_value, list) and actual_value not in expected_value:
                    error_messages.append('{0} param "{1}" with value "{2}" should be 1 of the following: {3}'.format(mandatory_or_optional, required_key, actual_value, expected_value))
                if isinstance(expected_value, dict):
                    minimum = expected_value.get('min', sys.maxint * -1)
                    maximum = expected_value.get('max', sys.maxint)
                    if not minimum <= actual_value <= maximum:
                        error_messages.append('{0} param "{1}" with value "{2}" should be in range: {3} - {4}'.format(mandatory_or_optional, required_key, actual_value, minimum, maximum))
            else:
                if HelperToolbox.check_type(expected_value, list)[0] is True and actual_value not in expected_value:
                    error_messages.append('{0} param "{1}" with value "{2}" should be 1 of the following: {3}'.format(mandatory_or_optional, required_key, actual_value, expected_value))
                elif HelperToolbox.check_type(expected_value, Toolbox.compiled_regex_type)[0] is True and not re.match(expected_value, actual_value):
                    error_messages.append('{0} param "{1}" with value "{2}" does not match regex "{3}"'.format(mandatory_or_optional, required_key, actual_value, expected_value.pattern))
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

        status, _ = ServiceManager.get_service_status(name, client=client)
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

    @staticmethod
    def wait_for_service(client, name, status, logger):
        """
        Wait for service to enter status
        :param client: SSHClient to run commands
        :param name: name of service
        :param status: True - running/False - not running
        :param logger: Logging object
        """
        tries = 10
        while tries > 0:
            service_status, _ = ServiceManager.get_service_status(name, client)
            if service_status == status:
                break
            logger.debug('... waiting for service {0}'.format(name))
            tries -= 1
            time.sleep(10 - tries)
        service_status, output = ServiceManager.get_service_status(name, client)
        if service_status != status:
            raise RuntimeError('Service {0} does not have expected status: {1}'.format(name, output))


class Schedule(object):
    """
    This decorator adds a schedule to a function. All arguments are these from celery's "crontab" class
    """
    _logger = LogHandler.get('lib', name='scheduler')

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def generate_schedule(self, name):
        try:
            schedules = Configuration.get('/ovs/framework/scheduling/celery', default={})
        except Exception as ex:
            Schedule._logger.error('Error loading celery scheduling configuration for {0}: {1}'.format(name, ex))
            schedules = {}
        if name in schedules:
            schedule = schedules[name]
            if schedule is None:
                return None, 'disabled by configuration'
            source = 'scheduled from configuration'
        else:
            schedule = self.kwargs
            source = 'scheduled from code'
        try:
            return crontab(**schedule), '{0}: {1}'.format(source, ', '.join(['{0}="{1}"'.format(key, value) for key, value in schedule.iteritems()]))
        except Exception as ex:
            Schedule._logger.error('Could not generate crontab for {0} with data {1} {2}: {3}'.format(name, schedule, source, ex))
            return None, 'error in configuration'


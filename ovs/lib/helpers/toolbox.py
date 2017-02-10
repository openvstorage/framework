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
from ovs.dal.helpers import DalToolbox
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.interactive import Interactive
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.services.service import ServiceManager
from ovs.log.log_handler import LogHandler


class LibToolbox(object):
    """
    Generic class for various methods
    """
    _function_pointers = {}

    regex_ip = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    regex_guid = re.compile('^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$')
    regex_vpool = re.compile('^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$')
    regex_preset = re.compile('^[0-9a-zA-Z][a-zA-Z0-9-_]{1,18}[a-zA-Z0-9]$')
    compiled_regex_type = type(re.compile('some_regex'))

    @staticmethod
    def fetch_hooks(component, sub_component):
        """
        Load hooks
        :param component: Type of hook, can be update, setup, ...
        :type component: str
        :param sub_component: Sub-component of hook type, Eg: pre-install, post-install, ...
        :type sub_component: str
        :return: The functions found decorated with the specified hooks
        :rtype: list
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return LibToolbox._function_pointers.get('{0}-{1}'.format(component, sub_component), [])

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
                                    and component in submember[1].hooks \
                                    and isinstance(submember[1].hooks[component], list) \
                                    and sub_component in submember[1].hooks[component]:
                                functions.append(submember[1])
        return functions

    @staticmethod
    def run_hooks(component, sub_component, logger=None, **kwargs):
        """
        Execute hooks
        :param component: Name of the component, eg: update, setup
        :type component: str
        :param sub_component: Name of the sub-component, eg: pre-install, post-install
        :type sub_component: str
        :param logger: Logger object to use for logging
        :type logger: ovs.log.log_handler.LogHandler
        :param kwargs: Additional named arguments
        :type kwargs: dict
        :return: Amount of functions executed
        """
        functions = LibToolbox.fetch_hooks(component=component, sub_component=sub_component)
        functions_found = len(functions) > 0
        if logger is not None and functions_found is True:
            LibToolbox.log(logger=logger, messages='Running "{0} - {1}" hooks'.format(component, sub_component), title=True)
        for function in functions:
            if logger is not None:
                LibToolbox.log(logger=logger, messages='Executing {0}.{1}'.format(function.__module__, function.__name__))
            function(**kwargs)
        return functions_found

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
            if DalToolbox.check_type(actual_value, expected_type)[0] is False:
                error_messages.append('{0} param "{1}" is of type "{2}" but we expected type "{3}"'.format(mandatory_or_optional, required_key, type(actual_value), expected_type))
                continue

            if expected_value is None:
                continue

            if expected_type == list:
                if type(expected_value) == LibToolbox.compiled_regex_type:  # List of strings which need to match regex
                    for item in actual_value:
                        if not re.match(expected_value, item):
                            error_messages.append('{0} param "{1}" has an item "{2}" which does not match regex "{3}"'.format(mandatory_or_optional, required_key, item, expected_value.pattern))
            elif expected_type == dict:
                LibToolbox.verify_required_params(expected_value, actual_params[required_key])
            elif expected_type == int or expected_type == float:
                if isinstance(expected_value, list) and actual_value not in expected_value:
                    error_messages.append('{0} param "{1}" with value "{2}" should be 1 of the following: {3}'.format(mandatory_or_optional, required_key, actual_value, expected_value))
                if isinstance(expected_value, dict):
                    minimum = expected_value.get('min', sys.maxint * -1)
                    maximum = expected_value.get('max', sys.maxint)
                    if not minimum <= actual_value <= maximum:
                        error_messages.append('{0} param "{1}" with value "{2}" should be in range: {3} - {4}'.format(mandatory_or_optional, required_key, actual_value, minimum, maximum))
            else:
                if DalToolbox.check_type(expected_value, list)[0] is True and actual_value not in expected_value:
                    error_messages.append('{0} param "{1}" with value "{2}" should be 1 of the following: {3}'.format(mandatory_or_optional, required_key, actual_value, expected_value))
                elif DalToolbox.check_type(expected_value, LibToolbox.compiled_regex_type)[0] is True and not re.match(expected_value, actual_value):
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
        status, _ = ServiceManager.get_service_status(name, client=client)
        if status is False and state in ['start', 'restart']:
            if logger is not None:
                logger.debug('{0}: Starting service {1}'.format(client.ip, name))
            ServiceManager.start_service(name, client=client)
            action = 'Started'
        elif status is True and state == 'stop':
            if logger is not None:
                logger.debug('{0}: Stopping service {1}'.format(client.ip, name))
            ServiceManager.stop_service(name, client=client)
            action = 'Stopped'
        elif status is True and state == 'restart':
            if logger is not None:
                logger.debug('{0}: Restarting service {1}'.format(client.ip, name))
            ServiceManager.restart_service(name, client=client)
            action = 'Restarted'

        if action is None:
            print '  [{0}] {1} already {2}'.format(client.ip, name, 'running' if status is True else 'halted')
        else:
            if logger is not None:
                logger.debug('{0}: {1} service {2}'.format(client.ip, action, name))
            print '  [{0}] {1} {2}'.format(client.ip, name, action.lower())

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

    @staticmethod
    def log(logger, messages, title=False, boxed=False, loglevel='info', silent=False):
        """
        Print a message on stdout and log to file
        :param logger: Logger object to use for the logging
        :type logger: ovs.log.log_handler.LogHandler
        :param messages: Messages to print and log
        :type messages: str or list
        :param title: If True some extra chars will be pre- and appended
        :type title: bool
        :param boxed: Use the Interactive boxed message print option
        :type boxed: bool
        :param loglevel: level to log on
        :type loglevel: str
        :param silent: If set to True, the messages will only be logged to file
        :type silent: bool
        :return: None
        """
        if type(messages) in (str, basestring, unicode):
            messages = [messages]
        if silent is False:
            if boxed is True:
                print Interactive.boxed_message(lines=messages)
            else:
                for message in messages:
                    if title is True:
                        message = '\n+++ {0} +++\n'.format(message)
                    if loglevel in ['error', 'exception']:
                        message = 'ERROR: {0}'.format(message)
                    print message

        for message in messages:
            getattr(logger, loglevel)(message)

    @staticmethod
    def is_service_internally_managed(service):
        """
        Validate whether the service is internally or externally managed
        :param service: Service to verify
        :type service: str
        :return: True if internally managed, False otherwise
        :rtype: bool
        """
        if service not in ['memcached', 'rabbitmq']:
            raise ValueError('Can only check memcached or rabbitmq')

        service_name_map = {'memcached': 'memcache',
                            'rabbitmq': 'messagequeue'}[service]
        config_key = '/ovs/framework/{0}'.format(service_name_map)
        if not Configuration.exists(key=config_key):
            return True

        if not Configuration.exists(key='{0}|metadata'.format(config_key)):
            raise ValueError('Not all required keys ({0}) for {1} are present in the configuration management'.format(config_key, service))
        metadata = Configuration.get('{0}|metadata'.format(config_key))
        if 'internal' not in metadata:
            raise ValueError('Internal flag not present in metadata for {0}.\nPlease provide a key: {1} and value "metadata": {{"internal": True/False}}'.format(service, config_key))

        internal = metadata['internal']
        if internal is False:
            if not Configuration.exists(key='{0}|endpoints'.format(config_key)):
                raise ValueError('Externally managed {0} cluster must have "endpoints" information\nPlease provide a key: {1} and value "endpoints": [<ip:port>]'.format(service, config_key))
            endpoints = Configuration.get(key='{0}|endpoints'.format(config_key))
            if not isinstance(endpoints, list) or len(endpoints) == 0:
                raise ValueError('The endpoints for {0} cannot be empty and must be a list'.format(service))
        return internal

    @staticmethod
    def ask_validate_password(ip, logger, username='root', previous=None):
        """
        Asks a user to enter the password for a given user on a given ip and validates it
        If previous is provided, we first attempt to login using the previous password, if successful, we don't ask for a password
        :param ip: IP of the node on which we want to validate / ask the password
        :type ip: str
        :param logger: Logger object to use for the logging
        :type logger: ovs.log.log_handler.LogHandler
        :param username: Username to login with
        :type username: str
        :param previous: Previously used password for another node in the cluster
        :type previous: str
        :return: None
        """
        try:
            SSHClient(ip, username)
            return None
        except:
            pass

        if previous is not None:
            try:
                SSHClient(ip, username=username, password=previous)
                return previous
            except:
                pass

        node_string = 'this node' if ip == '127.0.0.1' else ip
        while True:
            try:
                password = Interactive.ask_password('Enter the {0} password for {1}'.format(username, node_string))
                if password in ['', None]:
                    continue
                SSHClient(ip, username=username, password=password)
                return password
            except KeyboardInterrupt:
                raise
            except UnableToConnectException:
                raise
            except:
                LibToolbox.log(logger=logger, messages='Password invalid or could not connect to this node')


class Schedule(object):
    """
    This decorator adds a schedule to a function. All arguments are these from celery's "crontab" class
    """
    _logger = LogHandler.get('lib', name='scheduler')

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def generate_schedule(self, name):
        """
        Generate a schedule
        :param name: Name to generate
        :return: Crontab and additional information about scheduling
        """
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

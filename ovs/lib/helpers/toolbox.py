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
import time
import random
import string
import inspect
import logging
import subprocess
from celery.schedules import crontab
from ovs_extensions.constants import is_unittest_mode
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.interactive import Interactive
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.lib.plugin import PluginController


class Toolbox(object):
    """
    Generic class for various methods
    """
    _function_pointers = {}

    regex_ip = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    regex_guid = re.compile('^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$')
    regex_vpool = re.compile('^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$')
    regex_backend = re.compile('^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$')
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
        if is_unittest_mode():
            return Toolbox._function_pointers.get('{0}-{1}'.format(component, sub_component), [])
        functions = []
        for member in PluginController.get_lib():
            for submember_name, submember in inspect.getmembers(member):
                if hasattr(submember, 'hooks') \
                        and isinstance(submember.hooks, dict) \
                        and component in submember.hooks \
                        and isinstance(submember.hooks[component], list) \
                        and sub_component in submember.hooks[component]:
                    functions.append(submember)
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
        :type logger: ovs.extensions.generic.logger.Logger
        :param kwargs: Additional named arguments
        :return: Amount of functions executed
        """
        functions = Toolbox.fetch_hooks(component=component, sub_component=sub_component)
        functions_found = len(functions) > 0
        if logger is not None and functions_found is True:
            Toolbox.log(logger=logger, messages='Running "{0} - {1}" hooks'.format(component, sub_component), title=True)
        for fct in functions:
            if logger is not None:
                Toolbox.log(logger=logger, messages='Executing {0}.{1}'.format(fct.__module__, fct.__name__))
            fct(**kwargs)
        return functions_found

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
        :param logger: Logger Object
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
    def log(logger, messages, title=False, boxed=False, loglevel='info', silent=False):
        """
        Print a message on stdout and log to file
        :param logger: Logger object to use for the logging
        :type logger: ovs.extensions.generic.logger.Logger
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
        :type logger: ovs.extensions.generic.logger.Logger
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
                Toolbox.log(logger=logger, messages='Password invalid or could not connect to this node')


class Schedule(object):
    """
    This decorator adds a schedule to a function. All arguments are these from celery's "crontab" class
    """
    _logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def generate_schedule(self, name):
        """
        Generate a schedule for a Celery task
        :param name: Name of the Celery task to generate a schedule for
        :type name: str
        :return: Crontab and additional information about scheduling
        :rtype: tuple
        """
        Schedule._logger.debug('Generating schedule for {0}'.format(name))
        schedule_key = '/ovs/framework/scheduling/celery'
        try:
            schedules = Configuration.get(key=schedule_key, default={})
        except Exception:
            Schedule._logger.exception('Error loading celery scheduling configuration for {0}'.format(name))
            schedules = {}

        if schedules in ['', None]:  # Can occur when key has once been set and afterwards been emptied
            schedules = {}
        if not isinstance(schedules, dict):
            raise ValueError('Value for key "{0}" should be a dictionary'.format(schedule_key))

        if name in schedules:
            schedule = schedules[name]
            if schedule is None:
                return None, 'disabled by configuration'
            source = 'scheduled from configuration'
        else:
            schedule = self.kwargs
            source = 'scheduled from code'

        schedule_msg = ', '.join(['{0}="{1}"'.format(key, value) for key, value in schedule.iteritems()])
        Schedule._logger.debug('Generated schedule for {0}: {1}'.format(name, schedule_msg))
        try:
            return crontab(**schedule), '{0}: {1}'.format(source, schedule_msg)
        except TypeError:
            Schedule._logger.error('Invalid crontab schedule specified for task name {0}. Schedule: {1}'.format(name, schedule_msg))
            raise

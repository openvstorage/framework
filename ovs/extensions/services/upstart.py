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
Upstart module
"""

import re
import time
from subprocess import CalledProcessError
from ovs.extensions.generic.toolbox import Toolbox
from ovs.log.log_handler import LogHandler


class Upstart(object):
    """
    Contains all logic related to Upstart services
    """
    _logger = LogHandler.get('extensions', name='servicemanager')

    @staticmethod
    def _service_exists(name, client, path):
        if path is None:
            path = '/etc/init/'
        file_to_check = '{0}{1}.conf'.format(path, name)
        return client.file_exists(file_to_check)

    @staticmethod
    def _get_name(name, client, path=None):
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        """
        if Upstart._service_exists(name, client, path):
            return name
        if client.file_exists('/etc/init.d/{0}'.format(name)):
            return name
        name = 'ovs-{0}'.format(name)
        if Upstart._service_exists(name, client, path):
            return name
        Upstart._logger.info('Service {0} could not be found.'.format(name))
        raise ValueError('Service {0} could not be found.'.format(name))

    @staticmethod
    def add_service(name, client, params=None, target_name=None, additional_dependencies=None):
        """
        Add a service
        :param name: Name of the service to add
        :type name: str
        :param client: Client on which to add the service
        :type client: SSHClient
        :param params: Additional information about the service
        :type params: dict
        :param target_name: Overrule default name of the service with this name
        :type target_name: str
        :param additional_dependencies: Additional dependencies for this service
        :type additional_dependencies: list
        :return: None
        """
        if params is None:
            params = {}

        name = Upstart._get_name(name, client, '/opt/OpenvStorage/config/templates/upstart/')
        template_conf = '/opt/OpenvStorage/config/templates/upstart/{0}.conf'

        if not client.file_exists(template_conf.format(name)):
            # Given template doesn't exist so we are probably using system
            # init scripts
            return

        template_file = client.file_read(template_conf.format(name))

        for key, value in params.iteritems():
            template_file = template_file.replace('<{0}>'.format(key), value)
        if '<SERVICE_NAME>' in template_file:
            service_name = name if target_name is None else target_name
            template_file = template_file.replace('<SERVICE_NAME>', Toolbox.remove_prefix(service_name, 'ovs-'))
        template_file = template_file.replace('<_SERVICE_SUFFIX_>', '')

        dependencies = ''
        if additional_dependencies:
            for service in additional_dependencies:
                dependencies += '{0} '.format(service)
        template_file = template_file.replace('<ADDITIONAL_DEPENDENCIES>', dependencies)

        if target_name is None:
            client.file_write('/etc/init/{0}.conf'.format(name), template_file)
        else:
            client.file_write('/etc/init/{0}.conf'.format(target_name), template_file)

    @staticmethod
    def get_service_status(name, client):
        """
        Retrieve the status of a service
        :param name: Name of the service to retrieve the status of
        :type name: str
        :param client: Client on which to retrieve the status
        :type client: SSHClient
        :return: The status of the service and the output of the command
        :rtype: tuple
        """
        try:
            name = Upstart._get_name(name, client)
            output = client.run('service {0} status || true'.format(name))
            # Special cases (especially old SysV ones)
            if 'rabbitmq' in name:
                status = re.search('\{pid,\d+?\}', output) is not None
                return status, output
            # Normal cases - or if the above code didn't yield an outcome
            if 'start/running' in output or 'is running' in output:
                return True, output
            if 'stop' in output or 'not running' in output:
                return False, output
            return False, output
        except CalledProcessError as ex:
            Upstart._logger.exception('Get {0}.service status failed: {1}'.format(name, ex))
            raise Exception('Retrieving status for service "{0}" failed'.format(name))

    @staticmethod
    def remove_service(name, client):
        """
        Remove a service
        :param name: Name of the service to remove
        :type name: str
        :param client: Client on which to remove the service
        :type client: SSHClient
        :return: None
        """
        name = Upstart._get_name(name, client)
        client.file_delete('/etc/init/{0}.conf'.format(name))

    @staticmethod
    def start_service(name, client):
        """
        Start a service
        :param name: Name of the service to start
        :type name: str
        :param client: Client on which to start the service
        :type client: SSHClient
        :return: The output of the start command
        :rtype: str
        """
        status, output = Upstart.get_service_status(name, client)
        if status is True:
            return output
        try:
            name = Upstart._get_name(name, client)
            client.run('service {0} start'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            Upstart._logger.exception('Start {0} failed, {1}'.format(name, output))
            raise RuntimeError('Start {0} failed. {1}'.format(name, output))
        tries = 10
        while tries > 0:
            status, output = Upstart.get_service_status(name, client)
            if status is True:
                return output
            tries -= 1
            time.sleep(10 - tries)
        status, output = Upstart.get_service_status(name, client)
        if status is True:
            return output
        Upstart._logger.error('Start {0} failed. {1}'.format(name, output))
        raise RuntimeError('Start {0} failed. {1}'.format(name, output))

    @staticmethod
    def stop_service(name, client):
        """
        Stop a service
        :param name: Name of the service to stop
        :type name: str
        :param client: Client on which to stop the service
        :type client: SSHClient
        :return: The output of the stop command
        :rtype: str
        """
        status, output = Upstart.get_service_status(name, client)
        if status is False:
            return output
        try:
            name = Upstart._get_name(name, client)
            client.run('service {0} stop'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            Upstart._logger.exception('Stop {0} failed, {1}'.format(name, output))
            raise RuntimeError('Stop {0} failed, {1}'.format(name, output))
        tries = 10
        while tries > 0:
            status, output = Upstart.get_service_status(name, client)
            if status is False:
                return output
            tries -= 1
            time.sleep(10 - tries)
        status, output = Upstart.get_service_status(name, client)
        if status is False:
            return output
        Upstart._logger.error('Stop {0} failed. {1}'.format(name, output))
        raise RuntimeError('Stop {0} failed. {1}'.format(name, output))

    @staticmethod
    def restart_service(name, client):
        """
        Restart a service
        :param name: Name of the service to restart
        :type name: str
        :param client: Client on which to restart the service
        :type client: SSHClient
        :return: The output of the restart command
        :rtype: str
        """
        Upstart.stop_service(name, client)
        return Upstart.start_service(name, client)

    @staticmethod
    def has_service(name, client):
        """
        Verify existence of a service
        :param name: Name of the service to verify
        :type name: str
        :param client: Client on which to check for the service
        :type client: SSHClient
        :return: Whether the service exists
        :rtype: bool
        """
        try:
            Upstart._get_name(name, client)
            return True
        except ValueError:
            return False

    @staticmethod
    def get_service_pid(name, client):
        """
        Retrieve the PID of a service
        :param name: Name of the service to retrieve the PID for
        :type name: str
        :param client: Client on which to retrieve the PID for the service
        :type client: SSHClient
        :return: The PID of the service or 0 if no PID found
        :rtype: int
        """
        name = Upstart._get_name(name, client)
        if Upstart.get_service_status(name, client)[0] is True:
            output = client.run('service {0} status'.format(name))
            if output:
                # Special cases (especially old SysV ones)
                if 'rabbitmq' in name:
                    match = re.search('\{pid,(?P<pid>\d+?)\}', output)
                else:
                    # Normal cases - or if the above code didn't yield an outcome
                    match = re.search('start/running, process (?P<pid>\d+)', output)
                if match is not None:
                    match_groups = match.groupdict()
                    if 'pid' in match_groups:
                        return match_groups['pid']
        return -1

    @staticmethod
    def send_signal(name, signal, client):
        """
        Send a signal to a service
        :param name: Name of the service to send a signal
        :type name: str
        :param signal: Signal to pass on to the service
        :type signal: int
        :param client: Client on which to send a signal to the service
        :type client: SSHClient
        :return: None
        """
        name = Upstart._get_name(name, client)
        pid = Upstart.get_service_pid(name, client)
        if pid == -1:
            raise RuntimeError('Could not determine PID to send signal to')
        client.run('kill -s {0} {1}'.format(signal, pid))

    @staticmethod
    def list_services(client):
        """
        List all created services on a system
        :param client: Client on which to list all the services
        :type client: SSHClient
        :return: List of all services which have been created on some point
        :rtype: generator
        """
        for filename in client.dir_list('/etc/init'):
            if filename.endswith('.conf'):
                yield filename.replace('.conf', '')

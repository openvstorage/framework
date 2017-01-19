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
from subprocess import CalledProcessError, check_output
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.generic.toolbox import Toolbox
from ovs.log.log_handler import LogHandler


class Upstart(object):
    """
    Contains all logic related to Upstart services
    """
    _logger = LogHandler.get('extensions', name='service-manager')
    SERVICE_CONFIG_KEY = '/ovs/framework/hosts/{0}/services/{1}'

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
    def add_service(name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False):
        """
        Add a service
        :param name: Template name of the service to add
        :type name: str
        :param client: Client on which to add the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param params: Additional information about the service
        :type params: dict or None
        :param target_name: Overrule default name of the service with this name
        :type target_name: str or None
        :param startup_dependency: Additional startup dependency
        :type startup_dependency: str or None
        :param delay_registration: Register the service parameters in the config management right away or not
        :type delay_registration: bool
        :return: Parameters used by the service
        :rtype: dict
        """
        if params is None:
            params = {}

        service_name = Upstart._get_name(name, client, '/opt/OpenvStorage/config/templates/upstart/')
        template_file = '/opt/OpenvStorage/config/templates/upstart/{0}.conf'.format(service_name)

        if not client.file_exists(template_file):
            # Given template doesn't exist so we are probably using system init scripts
            return

        if target_name is not None:
            service_name = target_name

        params.update({'SERVICE_NAME': Toolbox.remove_prefix(service_name, 'ovs-'),
                       'STARTUP_DEPENDENCY': '' if startup_dependency is None else 'started {0}'.format(startup_dependency)})
        template_content = client.file_read(template_file)
        for key, value in params.iteritems():
            template_content = template_content.replace('<{0}>'.format(key), value)
        client.file_write('/etc/init/{0}.conf'.format(service_name), template_content)

        if delay_registration is False:
            Upstart.register_service(service_metadata=params, node_name=System.get_my_machine_id(client))
        return params

    @staticmethod
    def regenerate_service(name, client, target_name):
        """
        Regenerates the service files of a service.
        :param name: Template name of the service to regenerate
        :type name: str
        :param client: Client on which to regenerate the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param target_name: The current service name eg ovs-volumedriver_flash01.service
        :type target_name: str
        :return: None
        :rtype: NoneType
        """
        configuration_key = Upstart.SERVICE_CONFIG_KEY.format(System.get_my_machine_id(client), Toolbox.remove_prefix(target_name, 'ovs-'))
        # If the entry is stored in arakoon, it means the service file was previously made
        if not Configuration.exists(configuration_key):
            raise RuntimeError('Service {0} was not previously added and cannot be regenerated.'.format(target_name))
        # Rewrite the service file
        service_params = Configuration.get(configuration_key)
        startup_dependency = service_params['STARTUP_DEPENDENCY']
        if startup_dependency == '':
            startup_dependency = None
        else:
            startup_dependency = '.'.join(
                startup_dependency.split('.')[:-1])  # Remove .service from startup dependency
        output = Upstart.add_service(name=name,
                                     client=client,
                                     params=service_params,
                                     target_name=target_name,
                                     startup_dependency=startup_dependency,
                                     delay_registration=True)
        if output is None:
            raise RuntimeError('Regenerating files for service {0} has failed'.format(target_name))

    @staticmethod
    def get_service_status(name, client):
        """
        Retrieve the status of a service
        :param name: Name of the service to retrieve the status of
        :type name: str
        :param client: Client on which to retrieve the status
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: The status of the service and the output of the command
        :rtype: tuple
        """
        try:
            name = Upstart._get_name(name, client)
            output = client.run(['service', name, 'status'], allow_nonzero=True)
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
    def remove_service(name, client, delay_unregistration=False):
        """
        Remove a service
        :param name: Name of the service to remove
        :type name: str
        :param client: Client on which to remove the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param delay_unregistration: Un-register the service parameters in the config management right away or not
        :type delay_unregistration: bool
        :return: None
        """
        name = Upstart._get_name(name, client)
        run_file_name = '/opt/OpenvStorage/run/{0}.version'.format(Toolbox.remove_prefix(name, 'ovs-'))
        if client.file_exists(run_file_name):
            client.file_delete(run_file_name)
        client.file_delete('/etc/init/{0}.conf'.format(name))

        if delay_unregistration is False:
            Upstart.unregister_service(service_name=name, node_name=System.get_my_machine_id(client))

    @staticmethod
    def start_service(name, client):
        """
        Start a service
        :param name: Name of the service to start
        :type name: str
        :param client: Client on which to start the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: The output of the start command
        :rtype: str
        """
        status, output = Upstart.get_service_status(name, client)
        if status is True:
            return output
        try:
            name = Upstart._get_name(name, client)
            client.run(['service', name, 'start'])
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
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: The output of the stop command
        :rtype: str
        """
        status, output = Upstart.get_service_status(name, client)
        if status is False:
            return output
        try:
            name = Upstart._get_name(name, client)
            client.run(['service', name, 'stop'])
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
        :type client: ovs.extensions.generic.sshclient.SSHClient
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
        :type client: ovs.extensions.generic.sshclient.SSHClient
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
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: The PID of the service or 0 if no PID found
        :rtype: int
        """
        name = Upstart._get_name(name, client)
        if Upstart.get_service_status(name, client)[0] is True:
            output = client.run(['service', name, 'status'])
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
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: None
        """
        name = Upstart._get_name(name, client)
        pid = Upstart.get_service_pid(name, client)
        if pid == -1:
            raise RuntimeError('Could not determine PID to send signal to')
        client.run(['kill', '-s', signal, pid])

    @staticmethod
    def list_services(client):
        """
        List all created services on a system
        :param client: Client on which to list all the services
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: List of all services which have been created at some point
        :rtype: generator
        """
        for filename in client.dir_list('/etc/init'):
            if filename.endswith('.conf'):
                yield filename.replace('.conf', '')

    @staticmethod
    def monitor_services():
        """
        Monitor the local OVS services
        :return: None
        """

        def _advanced_sort(name1, name2):
            counter1 = name1.split('_')[-1]
            counter2 = name2.split('_')[-1]
            if counter1.isdigit() and counter2.isdigit():
                name1 = '_'.join(name1.split('_')[:-1])
                name2 = '_'.join(name2.split('_')[:-1])
                if name1 == name2:
                    return -1 if int(counter1) < int(counter2) else 1
            return -1 if name1 < name2 else 1

        try:
            previous_output = None
            while True:
                # Gather service states
                running_services = {}
                non_running_services = {}
                longest_service_name = 0
                for service_info in check_output('initctl list', shell=True).splitlines():
                    if not service_info.startswith('ovs-'):
                        continue
                    service_info = service_info.split(',')[0].strip()
                    service_name = service_info.split()[0].strip()
                    service_state = service_info.split()[1].strip()
                    if service_state == "start/running":
                        running_services[service_name] = service_state
                    else:
                        non_running_services[service_name] = service_state

                    if len(service_name) > longest_service_name:
                        longest_service_name = len(service_name)

                # Put service states in list
                output = ['OVS running processes',
                          '=====================\n']
                for service_name in sorted(running_services, cmp=_advanced_sort):
                    output.append('{0} {1} {2}'.format(service_name, ' ' * (longest_service_name - len(service_name)), running_services[service_name]))

                output.extend(['\n\nOVS non-running processes',
                               '=========================\n'])
                for service_name in sorted(non_running_services, cmp=_advanced_sort):
                    output.append('{0} {1} {2}'.format(service_name, ' ' * (longest_service_name - len(service_name)), non_running_services[service_name]))

                # Print service states (only if changes)
                if previous_output != output:
                    print '\x1b[2J\x1b[H'
                    for line in output:
                        print line
                    previous_output = list(output)
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def register_service(node_name, service_metadata):
        """
        Register the metadata of the service to the configuration management
        :param node_name: Name of the node on which the service is running
        :type node_name: str
        :param service_metadata: Metadata of the service
        :type service_metadata: dict
        :return: None
        """
        service_name = service_metadata['SERVICE_NAME']
        Configuration.set(key=Upstart.SERVICE_CONFIG_KEY.format(node_name, Toolbox.remove_prefix(service_name, 'ovs-')),
                          value=service_metadata)

    @staticmethod
    def unregister_service(node_name, service_name):
        """
        Un-register the metadata of a service from the configuration management
        :param service_name: Name of the service to clean from the configuration management
        :type service_name: str
        :param node_name: Name of the node on which to un-register the service
        :type node_name: str
        :return: None
        """
        Configuration.delete(key=Upstart.SERVICE_CONFIG_KEY.format(node_name, Toolbox.remove_prefix(service_name, 'ovs-')))

    @staticmethod
    def is_rabbitmq_running(client):
        """
        Check if rabbitmq is correctly running
        :param client: Client on which to check the rabbitmq process
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: The PID of the process and a bool indicating everything runs as expected
        :rtype: tuple
        """
        rabbitmq_running = False
        rabbitmq_pid_ctl = -1
        rabbitmq_pid_sm = -1
        output = client.run(['rabbitmqctl', 'status'], allow_nonzero=True)
        if output:
            match = re.search('\{pid,(?P<pid>\d+?)\}', output)
            if match is not None:
                match_groups = match.groupdict()
                if 'pid' in match_groups:
                    rabbitmq_running = True
                    rabbitmq_pid_ctl = match_groups['pid']

        if Upstart.has_service('rabbitmq-server', client) and Upstart.get_service_status('rabbitmq-server', client)[0] is True:
            rabbitmq_running = True
            rabbitmq_pid_sm = Upstart.get_service_pid('rabbitmq-server', client)

        same_process = rabbitmq_pid_ctl == rabbitmq_pid_sm
        Upstart._logger.debug('Rabbitmq is reported {0}running, pids: {1} and {2}'.format('' if rabbitmq_running else 'not ',
                                                                                          rabbitmq_pid_ctl,
                                                                                          rabbitmq_pid_sm))
        return rabbitmq_running, same_process

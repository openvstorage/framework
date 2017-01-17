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
Systemd module
"""
import re
import time
from subprocess import CalledProcessError, check_output
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.generic.toolbox import Toolbox
from ovs.log.log_handler import LogHandler


class Systemd(object):
    """
    Contains all logic related to Systemd services
    """
    _logger = LogHandler.get('extensions', name='service-manager')
    SERVICE_CONFIG_KEY = '/ovs/framework/hosts/{0}/services/{1}'

    @staticmethod
    def _service_exists(name, client, path):
        if path is None:
            path = '/lib/systemd/system/'
        file_to_check = '{0}{1}.service'.format(path, name)
        return client.file_exists(file_to_check)

    @staticmethod
    def _get_name(name, client, path=None):
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        """
        if Systemd._service_exists(name, client, path):
            return name
        if Systemd._service_exists(name, client, '/lib/systemd/system/'):
            return name
        name = 'ovs-{0}'.format(name)
        if Systemd._service_exists(name, client, path):
            return name
        Systemd._logger.info('Service {0} could not be found.'.format(name))
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

        service_name = Systemd._get_name(name, client, '/opt/OpenvStorage/config/templates/systemd/')
        template_file = '/opt/OpenvStorage/config/templates/systemd/{0}.service'.format(service_name)

        if not client.file_exists(template_file):
            # Given template doesn't exist so we are probably using system init scripts
            return

        if target_name is not None:
            service_name = target_name

        params.update({'SERVICE_NAME': Toolbox.remove_prefix(service_name, 'ovs-'),
                       'STARTUP_DEPENDENCY': '' if startup_dependency is None else '{0}.service'.format(startup_dependency)})
        template_content = client.file_read(template_file)
        for key, value in params.iteritems():
            template_content = template_content.replace('<{0}>'.format(key), value)
        client.file_write('/lib/systemd/system/{0}.service'.format(service_name), template_content)

        try:
            client.run(['systemctl', 'daemon-reload'])
            client.run(['systemctl', 'enable', '{0}.service'.format(service_name)])
        except CalledProcessError as cpe:
            Systemd._logger.exception('Add {0}.service failed, {1}'.format(service_name, cpe.output))
            raise Exception('Add {0}.service failed, {1}'.format(service_name, cpe.output))

        if delay_registration is False:
            Systemd.register_service(service_metadata=params, node_name=System.get_my_machine_id(client))
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
        configuration_key = Systemd.SERVICE_CONFIG_KEY.format(System.get_my_machine_id(client), Toolbox.remove_prefix(target_name, 'ovs-'))
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
        output = Systemd.add_service(name=name,
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
        name = Systemd._get_name(name, client)
        output = client.run(['systemctl', 'is-active', name], allow_nonzero=True)
        if output == 'active':
            return True, output
        elif output == 'inactive':
            return False, output
        return False, output

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
        name = Systemd._get_name(name, client)
        run_file_name = '/opt/OpenvStorage/run/{0}.version'.format(Toolbox.remove_prefix(name, 'ovs-'))
        if client.file_exists(run_file_name):
            client.file_delete(run_file_name)
        try:
            client.run(['systemctl', 'disable', '{0}.service'.format(name)])
        except CalledProcessError:
            pass  # Service already disabled
        client.file_delete('/lib/systemd/system/{0}.service'.format(name))
        client.run(['systemctl', 'daemon-reload'])

        if delay_unregistration is False:
            Systemd.unregister_service(service_name=name, node_name=System.get_my_machine_id(client))

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
        status, output = Systemd.get_service_status(name, client)
        if status is True:
            return output
        try:
            # When service files have been adjusted, a reload is required for these changes to take effect
            client.run(['systemctl', 'daemon-reload'])
        except CalledProcessError:
            pass
        try:
            name = Systemd._get_name(name, client)
            output = client.run(['systemctl', 'start', '{0}.service'.format(name)])
        except CalledProcessError as cpe:
            output = cpe.output
            Systemd._logger.exception('Start {0} failed, {1}'.format(name, output))
        return output

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
        status, output = Systemd.get_service_status(name, client)
        if status is False:
            return output
        try:
            name = Systemd._get_name(name, client)
            output = client.run(['systemctl', 'stop', '{0}.service'.format(name)])
        except CalledProcessError as cpe:
            output = cpe.output
            Systemd._logger.exception('Stop {0} failed, {1}'.format(name, output))
        return output

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
        try:
            # When service files have been adjusted, a reload is required for these changes to take effect
            client.run(['systemctl', 'daemon-reload'])
        except CalledProcessError:
            pass
        try:
            name = Systemd._get_name(name, client)
            output = client.run(['systemctl', 'restart', '{0}.service'.format(name)])
        except CalledProcessError as cpe:
            output = cpe.output
            Systemd._logger.exception('Restart {0} failed, {1}'.format(name, output))
        return output

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
            Systemd._get_name(name, client)
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
        pid = 0
        name = Systemd._get_name(name, client)
        if Systemd.get_service_status(name, client)[0] is True:
            output = client.run(['systemctl', 'show', name, '--property=MainPID']).split('=')
            if len(output) == 2:
                pid = output[1]
                if not pid.isdigit():
                    pid = 0
        return int(pid)

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
        name = Systemd._get_name(name, client)
        pid = Systemd.get_service_pid(name, client)
        if pid == 0:
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
        for service_info in client.run(['systemctl', 'list-unit-files', '--type=service', '--no-legend', '--no-pager']).splitlines():
            yield '.'.join(service_info.split(' ')[0].split('.')[:-1])

    @staticmethod
    def monitor_services():
        """
        Monitor the local OVS services
        :return: None
        """
        try:
            previous_output = None
            while True:
                # Gather service states
                running_services = {}
                non_running_services = {}
                longest_service_name = 0
                for service_name in check_output('systemctl list-unit-files --type=service --no-legend --no-pager | grep "ovs-" | tr -s " " | cut -d " " -f 1', shell=True).splitlines():
                    try:
                        service_state = check_output('systemctl is-active {0}'.format(service_name), shell=True).strip()
                    except CalledProcessError as cpe:
                        service_state = cpe.output.strip()

                    service_name = service_name.replace('.service', '')
                    if service_state == 'active':
                        running_services[service_name] = service_state
                    else:
                        non_running_services[service_name] = service_state

                    if len(service_name) > longest_service_name:
                        longest_service_name = len(service_name)

                # Put service states in list
                output = ['OVS running processes',
                          '=====================\n']
                for service_name in sorted(running_services):
                    output.append('{0} {1} {2}'.format(service_name, ' ' * (longest_service_name - len(service_name)), running_services[service_name]))

                output.extend(['\n\nOVS non-running processes',
                               '=========================\n'])
                for service_name in sorted(non_running_services):
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
        Configuration.set(key=Systemd.SERVICE_CONFIG_KEY.format(node_name, Toolbox.remove_prefix(service_name, 'ovs-')),
                          value=service_metadata)

    @staticmethod
    def unregister_service(node_name, service_name):
        """
        Un-register the metadata of a service from the configuration management
        :param node_name: Name of the node on which to un-register the service
        :type node_name: str
        :param service_name: Name of the service to clean from the configuration management
        :type service_name: str
        :return: None
        """
        Configuration.delete(key=Systemd.SERVICE_CONFIG_KEY.format(node_name, Toolbox.remove_prefix(service_name, 'ovs-')))

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

        if Systemd.has_service('rabbitmq-server', client) and Systemd.get_service_status('rabbitmq-server', client)[0] is True:
            rabbitmq_running = True
            rabbitmq_pid_sm = Systemd.get_service_pid('rabbitmq-server', client)

        same_process = rabbitmq_pid_ctl == rabbitmq_pid_sm
        Systemd._logger.debug('Rabbitmq is reported {0}running, pids: {1} and {2}'.format('' if rabbitmq_running else 'not ',
                                                                                          rabbitmq_pid_ctl,
                                                                                          rabbitmq_pid_sm))
        return rabbitmq_running, same_process

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
Fleetctl module
"""
import time
from subprocess import check_output

try:
    import fleet.v1 as fleet
    import googleapiclient
except ImportError as ie:
    raise ImportError('Fleet python client is not installed. Please check documentation on how to install it. {0}'.format(ie))

try:
    FLEET_CLIENT = fleet.Client('http+unix://%2Fvar%2Frun%2Ffleet.sock')
except googleapiclient.errors.HttpError as he:
    raise ValueError(he)

from ovs.log.log_handler import LogHandler
from ovs.extensions.generic.toolbox import Toolbox
from ovs.extensions.services.systemd import Systemd


class FleetCtl(object):
    """
    Contains all logic related to managing services through fleet
    Allows services not managed by fleet to be managed through this extension - delegates to systemd
    """
    _logger = LogHandler.get('extensions', name='fleetctl')

    @staticmethod
    def add_service(name, client, params=None, target_name=None, additional_dependencies=None):
        """
        This will generate a .service (temporary) file to feed to fleet to start a service
         service will become "name@<client.ip>.service"
        """
        if params is None:
            params = {}
        if additional_dependencies is None:
            additional_dependencies = []

        client_ip = FleetCtl._get_client_ip(client)

        if FleetCtl.has_service(name, client):
            FleetCtl._logger.info('Not re-adding service {0} to machine {1}'.format(name, client_ip))
            return

        # noinspection PyProtectedMember
        name = Systemd._get_name(name, client, '/opt/OpenvStorage/config/templates/systemd/')
        template_service = '/opt/OpenvStorage/config/templates/systemd/{0}.service'

        if not client.file_exists(template_service.format(name)):
            # Given template doesn't exist so we are probably using system
            # init scripts
            return

        template_file = client.file_read(template_service.format(name))

        for key, value in params.iteritems():
            template_file = template_file.replace('<{0}>'.format(key), value)
        if '<SERVICE_NAME>' in template_file:
            service_name = name if target_name is None else target_name
            template_file = template_file.replace('<SERVICE_NAME>', Toolbox.remove_prefix(service_name, 'ovs-'))
        template_file = template_file.replace('<_SERVICE_SUFFIX_>', '@{0}'.format(client_ip))

        dependencies = ''
        for service in additional_dependencies:
            dependencies += '{0}@{1}.service '.format(service, client_ip)
        template_file = template_file.replace('<ADDITIONAL_DEPENDENCIES>', dependencies)

        template_file += "\n[X-Fleet]\nMachineID={0}".format(FleetCtl._get_id_from_ip(client_ip))
        if target_name is not None:
            name = target_name
        fleet_name = "{0}@{1}.service".format(name, client_ip)

        FleetCtl._logger.debug('Creating fleet unit {0} {1}'.format(fleet_name, template_file))
        unit = FleetCtl._create_unit(fleet_name, template_file)
        time.sleep(1)
        FLEET_CLIENT.set_unit_desired_state(unit, 'loaded')
        time.sleep(1)
        unit = FleetCtl._get_unit(fleet_name)
        FleetCtl._logger.info('Created unit {0}'.format(unit.as_dict()))

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
        if FleetCtl._has_service(name, client):
            fleet_name = FleetCtl._get_unit_name(name, client)
            unit = FleetCtl._get_unit(fleet_name)
            FleetCtl._logger.debug('Fleet unit {0} status {1}'.format(fleet_name, unit.as_dict()['currentState']))
            return unit.as_dict()['currentState'] == 'launched'
        return Systemd.get_service_status(name, client)

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
        if FleetCtl._has_service(name, client):
            fleet_name = FleetCtl._get_unit_name(name, client)
            unit = FleetCtl._get_unit(fleet_name)
            FleetCtl.stop_service(name, client)
            result = FLEET_CLIENT.destroy_unit(unit)
            start = time.time()
            FleetCtl._logger.debug('Fleet destroy unit {0} {1}'.format(fleet_name, result))
            while time.time() - start < 60:
                time.sleep(1)
                if FleetCtl._has_service(name, client) is False:
                    return
            FleetCtl._logger.warning('Failed to remove unit {0} after 60 seconds'.format(fleet_name))
        else:
            return Systemd.remove_service(name, client)

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
        if FleetCtl._has_service(name, client):
            fleet_name = FleetCtl._get_unit_name(name, client)
            start = time.time()
            while time.time() - start < 60:
                unit = FLEET_CLIENT.get_unit(fleet_name)
                if unit.as_dict()['currentState'] != 'launched':
                    FLEET_CLIENT.set_unit_desired_state(unit, 'launched')
                    time.sleep(1)
                else:
                    break
            unit = FleetCtl._get_unit(fleet_name)
            if unit.as_dict()['currentState'] != 'launched':
                FleetCtl._logger.warning('Failed to start unit {0}'.format(unit.as_dict()))
            FleetCtl._logger.debug('Fleet start unit {0} > {1}'.format(fleet_name, unit.as_dict()['currentState']))
            return unit.as_dict()['currentState']
        return Systemd.start_service(name, client)

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
        if FleetCtl._has_service(name, client):
            fleet_name = FleetCtl._get_unit_name(name, client)
            start = time.time()
            while time.time() - start < 60:
                unit = FleetCtl._get_unit(fleet_name)
                if unit.as_dict()['currentState'] != 'loaded':
                    FLEET_CLIENT.set_unit_desired_state(unit, 'loaded')
                    time.sleep(1)
                else:
                    break
            unit = FleetCtl._get_unit(fleet_name)
            if unit['currentState'] != 'loaded':
                FleetCtl._logger.warning('Failed to stop unit {0}'.format(unit.as_dict()))
            FleetCtl._logger.debug('Fleet stop unit {0} {1}'.format(fleet_name, unit.as_dict()['currentState']))
            return unit.as_dict()['currentState']
        return Systemd.stop_service(name, client)

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
        if not FleetCtl.has_service(name, client):
            return Systemd.restart_service(name, client)
        FleetCtl.stop_service(name, client)
        FleetCtl.start_service(name, client)
        return FleetCtl.get_service_status(name, client)

    @staticmethod
    def _has_service(name, client):
        fleet_name = FleetCtl._get_unit_name(name, client)
        try:
            FleetCtl._get_unit(fleet_name)
            return True
        except (ValueError, RuntimeError):
            return False

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
        fleet_has_service = FleetCtl._has_service(name, client)
        if not fleet_has_service:
            return Systemd.has_service(name, client)
        return fleet_has_service

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
        return Systemd.get_service_pid(name, client)

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
        return Systemd.send_signal(name, signal, client)

    @staticmethod
    def _list_fleet_machines():
        return FLEET_CLIENT.list_machines()

    @staticmethod
    def _get_client_ip(client):
        if client.ip == '127.0.0.1':
            ips = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).split('\n')
            fleet_machines = dict((m['primaryIP'], m['id']) for m in FLEET_CLIENT.list_machines())
            match = list(set(fleet_machines.keys()).intersection(set(ips)))
            if len(match) == 1:
                return match[0]
            raise ValueError('Could not determine a match between this node and running fleet nodes')
        return client.ip

    @staticmethod
    def _get_id_from_ip(ip):
        for machine in FleetCtl._list_fleet_machines():
            if machine['primaryIP'] == ip:
                return machine['id']
        raise ValueError('Fleet machine with ip {0} not found. Make sure "fleet" service is running.'.format(ip))

    @staticmethod
    def _get_unit(fleet_name):
        try:
            return FLEET_CLIENT.get_unit(fleet_name)
        except fleet.APIError as ae:
            FleetCtl._logger.debug('Unit {0} not found. {1}'.format(fleet_name, ae))
            if ae.code == 404:
                # make error more descriptive
                raise ValueError('Unit with name {0} not found'.format(fleet_name))
            raise RuntimeError('Fleet API error {0}'.format(ae))

    # noinspection PyProtectedMember
    @staticmethod
    def _get_unit_name(name, client):
        try:
            name = Systemd._get_name(name, client, '/opt/OpenvStorage/config/templates/systemd/')
        except ValueError:
            try:
                name = Systemd._get_name(name, client)
            except ValueError:
                name = 'ovs-{0}'.format(name)
        client_ip = FleetCtl._get_client_ip(client)
        fleet_name = "{0}@{1}.service".format(name, client_ip)
        return fleet_name

    @staticmethod
    def _create_unit(fleet_name, template_file):
        from ovs.extensions.generic.configuration import Configuration
        start = time.time()
        while time.time() - start < 60:
            try:
                unit = FLEET_CLIENT.create_unit(fleet_name, fleet.Unit(from_string=template_file))
                return unit
            except fleet.APIError as ae:
                if ae.code == 500:
                    FleetCtl._logger.warning('API Error in fleet, most likely caused by etcd, retrying. {0}'.format(ae))
                    key = '/_coreos.com/fleet/job/{0}/object'.format(fleet_name)
                    if Configuration.exists(key):
                        Configuration.delete(key)
                    time.sleep(1)
                else:
                    raise
        raise RuntimeError('Failed to create ')

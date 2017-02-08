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
Systemd Mock module
"""


class Systemd(object):
    """
    Contains all logic related to Systemd Mock services
    """
    services = {}

    @staticmethod
    def clean():
        """
        Clean up mocked Class
        """
        Systemd.services = {}

    @staticmethod
    def add_service(name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False):
        """
        Adds a mocked service
        """
        _ = params, startup_dependency, delay_registration
        key = 'None' if client is None else client.ip
        name = name if target_name is None else target_name
        Systemd.services[key] = {name: 'HALTED'}

    @staticmethod
    def get_service_status(name, client):
        """
        Retrieve the mocked service status
        """
        key = 'None' if client is None else client.ip
        output = 'active' if name in Systemd.services[key] else 'inactive'
        status = Systemd.services[key].get(name, 'HALTED') == 'RUNNING'
        return status, output

    @staticmethod
    def remove_service(name, client):
        """
        Remove a mocked service
        """
        key = 'None' if client is None else client.ip
        if name in Systemd.services[key]:
            Systemd.services[key].pop(name)

    @staticmethod
    def start_service(name, client):
        """
        Start a mocked service
        """
        key = 'None' if client is None else client.ip
        if name not in Systemd.services[key]:
            raise RuntimeError('Service {0} does not exist'.format(name))
        Systemd.services[key][name] = 'RUNNING'
        status, output = Systemd.get_service_status(name, client)
        if status is True:
            return output
        raise RuntimeError('Start {0} failed. {1}'.format(name, output))

    @staticmethod
    def stop_service(name, client):
        """
        Stop a mocked service
        """
        key = 'None' if client is None else client.ip
        if name not in Systemd.services[key]:
            raise RuntimeError('Service {0} does not exist'.format(name))
        Systemd.services[key][name] = 'HALTED'
        status, output = Systemd.get_service_status(name, client)
        if status is False:
            return output
        raise RuntimeError('Stop {0} failed. {1}'.format(name, output))

    @staticmethod
    def restart_service(name, client):
        """
        Restart a mocked service
        """
        Systemd.stop_service(name, client)
        return Systemd.start_service(name, client)

    @staticmethod
    def has_service(name, client):
        """
        Verify whether a mocked service exists
        """
        key = 'None' if client is None else client.ip
        return name in Systemd.services.get(key, {})

    @staticmethod
    def register_service(node_name, service_metadata):
        """
        Register a service in the configuration management
        """
        _ = node_name, service_metadata

    @staticmethod
    def unregister_service(node_name, service_name):
        """
        Un-register a service from the configuration management
        """
        _ = node_name, service_name

    @staticmethod
    def _service_exists(name, client, path):
        """
        Verify whether a mocked service exists
        """
        _ = path
        key = 'None' if client is None else client.ip
        return name in Systemd.services[key]

    @staticmethod
    def _get_name(name, client, path=None):
        """
        Return the name of the mocked service
        """
        _ = client, path
        return name

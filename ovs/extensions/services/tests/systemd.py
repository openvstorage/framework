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


class MockSystemd(object):
    """
    Contains all logic related to Upstart Mock services
    """
    services = {}

    @staticmethod
    def clean():
        """
        Clean up mocked Class
        """
        MockSystemd.services = {}

    @staticmethod
    def add_service(name, client, params=None, target_name=None, additional_dependencies=None):
        """
        Adds a mocked service
        """
        _ = client, params, target_name, additional_dependencies
        MockSystemd.services[name] = 'HALTED'

    @staticmethod
    def get_service_status(name, client):
        """
        Retrieve the mocked service status
        """
        _ = client
        output = 'active' if name in MockSystemd.services else 'inactive'
        status = MockSystemd.services.get(name, 'HALTED') == 'RUNNING'
        return status, output

    @staticmethod
    def remove_service(name, client):
        """
        Remove a mocked service
        """
        _ = client
        if name in MockSystemd.services:
            MockSystemd.services.pop(name)

    @staticmethod
    def disable_service(name, client):
        """
        Disable a mocked service
        """
        _ = name, client

    @staticmethod
    def enable_service(name, client):
        """
        Enabled a mocked service
        """
        _ = name, client

    @staticmethod
    def start_service(name, client):
        """
        Start a mocked service
        """
        if name not in MockSystemd.services:
            raise RuntimeError('Service {0} does not exist'.format(name))
        MockSystemd.services[name] = 'RUNNING'
        status, output = MockSystemd.get_service_status(name, client)
        if status is True:
            return output
        raise RuntimeError('Start {0} failed. {1}'.format(name, output))

    @staticmethod
    def stop_service(name, client):
        """
        Stop a mocked service
        """
        if name not in MockSystemd.services:
            raise RuntimeError('Service {0} does not exist'.format(name))
        MockSystemd.services[name] = 'HALTED'
        status, output = MockSystemd.get_service_status(name, client)
        if status is True:
            return output
        raise RuntimeError('Stop {0} failed. {1}'.format(name, output))

    @staticmethod
    def restart_service(name, client):
        """
        Restart a mocked service
        """
        MockSystemd.stop_service(name, client)
        return MockSystemd.start_service(name, client)

    @staticmethod
    def has_service(name, client):
        """
        Verify whether a mocked service exists
        """
        _ = client
        return name in MockSystemd.services

    @staticmethod
    def is_enabled(name, client):
        """
        Verify whether a mocked service is enabled
        """
        _ = name, client
        return True

    @staticmethod
    def _service_exists(name, client, path):
        """
        Verify whether a mocked service exists
        """
        _ = client, path
        return name in MockSystemd.services

    @staticmethod
    def _get_name(name, client, path=None):
        """
        Return the name of the mocked service
        """
        _ = client, path
        return name

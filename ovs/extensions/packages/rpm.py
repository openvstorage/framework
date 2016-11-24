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
Rpm Package module
"""
import time
from subprocess import check_output, CalledProcessError
from ovs.log.log_handler import LogHandler


class RpmPackage(object):
    """
    Contains all logic related to Rpm packages (used in e.g. Centos)
    """
    _logger = LogHandler.get('lib', name='package-manager-rpm')

    @staticmethod
    def get_installed_versions(client=None, package_names=None):
        """
        Retrieve currently installed versions of all packages
        :param client: Client on which to check the installed versions
        :type client: SSHClient
        :param package_names: Name of the packages to check
        :type package_names: list
        :return: Package installed versions
        :rtype: dict
        """
        versions = {}
        if package_names is None:
            package_names = RpmPackage.OVS_PACKAGE_NAMES
        for package_name in package_names:
            command = "yum info {0} | grep Version | cut -d ':' -f 2 || true".format(package_name)
            if client is None:
                version_info = check_output(command, shell=True).strip()
            else:
                version_info = client.run(command, allow_insecure=True).strip()
            if version_info:
                versions[package_name] = version_info
        return versions

    @staticmethod
    def get_candidate_versions(client, package_names):
        """
        Retrieve the versions candidate for installation of all packages
        :param client: Root client on which to check the candidate versions
        :type client: SSHClient
        :param package_names: Name of the packages to check
        :type package_names: list
        :return: Package candidate versions
        :rtype: dict
        """
        RpmPackage.update(client=client)
        versions = {}
        for package_name in package_names:
            installed = None
            candidate = None
            versions[package_name] = ''
            for line in client.run(['yum', 'list', package_name]).splitlines():
                if line.startswith(package_name):
                    version = line.split()
                    if len(version) > 1:
                        if installed is None:
                            candidate = version[1]
                        else:
                            candidate = version[1]
            versions[package_name] = candidate
        return versions

    @staticmethod
    def install(package_name, client):
        """
        Install the specified package
        :param package_name: Name of the package to install
        :type package_name: str
        :param client: Root client on which to execute the installation of the package
        :type client: SSHClient
        :return: None
        """
        if client.username != 'root':
            raise RuntimeError('Only the "root" user can install packages')

        counter = 0
        max_counter = 3
        while counter < max_counter:
            try:
                client.run(['yum', 'update', '-y', package_name])
                break
            except CalledProcessError as cpe:
                # Retry 3 times if fail
                if counter == max_counter:
                    RpmPackage._logger.error('Install {0} failed. Error: {1}'.format(package_name, cpe.output))
                    raise cpe
            except Exception as ex:
                raise ex
            counter += 1
            time.sleep(1)

    @staticmethod
    def update(client):
        """
        Run the 'aptdcon --refresh' command on the specified node to update the package information
        :param client: Root client on which to update the package information
        :type client: SSHClient
        :return: None
        """
        if client.username != 'root':
            raise RuntimeError('Only the "root" user can update packages')
        try:
            client.run(['yum', 'check-update'])
        except CalledProcessError as cpe:
            # Returns exit value of 100 if there are packages available for an update
            if cpe.returncode != 100:
                RpmPackage._logger.error('Update failed. Error: {0}'.format(cpe.output))
                raise cpe

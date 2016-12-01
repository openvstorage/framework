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
Debian Package module
"""

from subprocess import check_output, CalledProcessError
from ovs.extensions.generic.toolbox import Toolbox
from ovs.log.log_handler import LogHandler


class DebianPackage(object):
    """
    Contains all logic related to Debian packages (used in e.g. Debian, Ubuntu)
    """

    APT_CONFIG_STRING = '-o Dir::Etc::sourcelist="sources.list.d/ovsaptrepo.list"'
    _logger = LogHandler.get('lib', name='package-manager-debian')

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
            package_names = DebianPackage.OVS_PACKAGE_NAMES
        for package_name in package_names:
            command = "dpkg -s '{0}' | grep Version | awk '{{print $2}}'".format(package_name.replace(r"'", r"'\''"))
            if client is None:
                versions[package_name] = check_output(command, shell=True).strip()
            else:
                versions[package_name] = client.run(command, allow_insecure=True).strip()
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
        DebianPackage.update(client=client)
        versions = {}
        for package_name in package_names:
            versions[package_name] = ''
            for line in client.run(['apt-cache', 'policy', package_name, DebianPackage.APT_CONFIG_STRING]).splitlines():
                line = line.strip()
                if line.startswith('Candidate:'):
                    candidate = Toolbox.remove_prefix(line, 'Candidate:').strip()
                    if candidate == '(none)':
                        candidate = ''
                    versions[package_name] = candidate
                    break
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

        installed = DebianPackage.get_installed_versions(client=client, package_names=[package_name])[package_name]
        candidate = DebianPackage.get_candidate_versions(client=client, package_names=[package_name])[package_name]

        if installed == candidate:
            return

        command = "aptdcon --hide-terminal --allow-unauthenticated --install '{0}'".format(package_name.replace(r"'", r"'\''"))
        DebianPackage._logger.debug('{0}: Installing package {1}'.format(client.ip, package_name))
        try:
            output = client.run('yes | {0}'.format(command), allow_insecure=True)
            if 'ERROR' in output:
                raise Exception('Installing package {0} failed. Command used: "{1}". Output returned: {2}'.format(package_name, command, output))
            DebianPackage._logger.debug('{0}: Installed package {1}'.format(client.ip, package_name))
        except CalledProcessError as cpe:
            DebianPackage._logger.warning('{0}: Install failed, trying to reconfigure the packages: {1}'.format(client.ip, cpe.output))
            client.run(['aptdcon', '--fix-install', '--hide-terminal', '--allow-unauthenticated'])
            DebianPackage._logger.debug('{0}: Trying to install the package again'.format(client.ip))
            output = client.run('yes | {0}'.format(command), allow_insecure=True)
            if 'ERROR' in output:
                raise Exception('Installing package {0} failed. Command used: "{1}". Output returned: {2}'.format(package_name, command, output))

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
            client.run(['which', 'aptdcon'])
        except CalledProcessError:
            raise Exception('APT Daemon package is not correctly installed')
        try:
            client.run(['aptdcon', '--refresh', '--sources-file=ovsaptrepo.list'])
        except CalledProcessError as cpe:
            DebianPackage._logger.warning('{0}: Update package cache failed, trying to reconfigure the packages: {1}'.format(client.ip, cpe.output))
            client.run(['aptdcon', '--fix-install', '--hide-terminal', '--allow-unauthenticated'])
            DebianPackage._logger.debug('{0}: Trying to update the package cache again'.format(client.ip))
            client.run(['aptdcon', '--refresh', '--sources-file=ovsaptrepo.list'])

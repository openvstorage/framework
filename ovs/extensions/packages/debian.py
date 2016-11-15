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

import time
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
            command = "dpkg -s {0} | grep Version | awk '{{print $2}}'".format(package_name)
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
    def install(package_name, client, force=False):
        """
        Install the specified package
        :param package_name: Name of the package to install
        :type package_name: str
        :param client: Root client on which to execute the installation of the package
        :type client: SSHClient
        :param force: Flag indicating to use the '--force-yes' flag
        :type force: bool
        :return: None
        """
        if client.username != 'root':
            raise RuntimeError('Only the "root" user can install packages')

        force_text = '--force-yes' if force is True else ''
        counter = 0
        max_counter = 5
        last_exception = None
        success = False
        while counter < max_counter:
            counter += 1
            try_again = ', trying again' if counter < max_counter else ''
            try:
                client.run(['apt-get', 'install', '-y', force_text, package_name])
                installed, candidate = DebianPackage._get_installed_candidate_version(package_name, client=client)
                if installed == candidate:
                    success = True
                    break
                else:
                    last_exception = RuntimeError('"apt-get install" succeeded, but upgrade not visible in "apt-cache policy"')
                    DebianPackage._logger.error('Failure: Upgrade not visible{0}'.format(try_again))
            except CalledProcessError as cpe:
                DebianPackage._logger.error('Install failed{0}: {1}'.format(try_again, cpe.output))
                if cpe.output and 'You may want to run apt-get update' in cpe.output[0] and counter != max_counter:
                    DebianPackage.update(client)
                last_exception = cpe
            except Exception as ex:
                DebianPackage._logger.error('Install failed{0}: {1}'.format(try_again, ex))
                last_exception = ex
            time.sleep(1)
        if success is False:
            raise last_exception

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
            client.run(['aptdcon', '--refresh', '--sources-file=ovsaptrepo.list'])
        except CalledProcessError:
            DebianPackage._logger.error('Failed to update the packages on StorageRouter with IP {0}'.format(client.ip))

    @staticmethod
    def verify_update_required(packages, services, client):
        """
        :param packages:
        :param services:
        :param client:
        :return:
        """
        services_checked = []
        update_info = {'version': '',
                       'packages': [],
                       'services': []}
        for package_name in packages:
            installed, candidate = DebianPackage._get_installed_candidate_version(package_name, client=client)

            if installed is not None and candidate != installed:
                update_info['packages'].append(package_name)
                update_info['services'] = services
                update_info['version'] = candidate
            else:
                for service in services:
                    if service in services_checked:
                        continue
                    services_checked.append(service)
                    if client.file_exists('/opt/OpenvStorage/run/{0}.version'.format(service)):
                        running_version = client.file_read('/opt/OpenvStorage/run/{0}.version'.format(service)).strip()
                        if running_version != candidate:
                            update_info['services'].append(service)
                            update_info['version'] = candidate
        return update_info

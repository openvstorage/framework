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
from ovs.log.logHandler import LogHandler
from subprocess import check_output
from subprocess import CalledProcessError


class DebianPackage(object):
    """
    Contains all logic related to Debian packages (used in e.g. Debian, Ubuntu)
    """

    OVS_PACKAGE_NAMES = ['openvstorage', 'openvstorage-core', 'openvstorage-webapps', 'openvstorage-sdm',
                         'openvstorage-backend', 'openvstorage-backend-core', 'openvstorage-backend-webapps', 'openvstorage-cinder-plugin',
                         'volumedriver-server', 'volumedriver-base', 'alba', 'arakoon']
    APT_CONFIG_STRING = '-o Dir::Etc::sourcelist="sources.list.d/ovsaptrepo.list" -o Dir::Etc::sourceparts="-" -o APT::Get::List-Cleanup="0"'
    _logger = LogHandler.get('lib', name='packager')

    @staticmethod
    def _get_version(package_name):
        return check_output("dpkg -s {0} | grep Version | cut -d ' ' -f 2".format(package_name), shell=True).strip()

    @staticmethod
    def _get_installed_candidate_version(package_name, client):
        installed = None
        candidate = None
        for line in client.run('apt-cache policy {0} {1}'.format(package_name, DebianPackage.APT_CONFIG_STRING)).splitlines():
            line = line.strip()
            if line.startswith('Installed:'):
                installed = line.lstrip('Installed:').strip()
            elif line.startswith('Candidate:'):
                candidate = line.lstrip('Candidate:').strip()
            if installed is not None and candidate is not None:
                break
        return installed if installed != '(none)' else None, candidate if candidate != '(none)' else None

    @staticmethod
    def get_versions():
        versions = {}
        for package_name in DebianPackage.OVS_PACKAGE_NAMES:
            version_info = DebianPackage._get_version(package_name)
            if version_info:
                versions[package_name] = version_info
        return versions

    @staticmethod
    def install(package_name, client, force=False):
        force_text = '--force-yes' if force is True else ''
        counter = 0
        max_counter = 5
        last_exception = None
        success = False
        while counter < max_counter:
            counter += 1
            try_again = ', trying again' if counter < max_counter else ''
            try:
                client.run('apt-get install -y {0} {1}'.format(force_text, package_name))
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
        counter = 0
        max_counter = 5
        while True and counter < max_counter:
            counter += 1
            try:
                client.run('apt-get update {0}'.format(DebianPackage.APT_CONFIG_STRING))
                break
            except CalledProcessError as cpe:
                if cpe.output and 'Could not get lock' in cpe.output[0] and counter != max_counter:
                    DebianPackage._logger.info('Attempt {0} to get lock failed, trying again'.format(counter))
                if counter == max_counter:  # Update can sometimes fail because apt lock cannot be retrieved
                    DebianPackage._logger.error('Update failed. Error: {0}'.format(cpe.output))
                    raise cpe
            time.sleep(1)

    @staticmethod
    def verify_update_required(packages, services, client):
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

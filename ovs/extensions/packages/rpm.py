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
from ovs.log.log_handler import LogHandler
from subprocess import check_output
from subprocess import CalledProcessError


class RpmPackage(object):
    """
    Contains all logic related to Rpm packages (used in e.g. Centos)
    """
    _logger = LogHandler.get('lib', name='packager')
    OVS_PACKAGE_NAMES = ['openvstorage', 'openvstorage-core', 'openvstorage-webapps', 'openvstorage-sdm',
                         'openvstorage-backend', 'openvstorage-backend-core', 'openvstorage-backend-webapps', 'openvstorage-cinder-plugin',
                         'volumedriver-server', 'volumedriver-base', 'volumedriver-no-dedup-server', 'volumedriver-no-dedup-base',
                         'alba', 'arakoon']

    @staticmethod
    def _get_version(package_name, client):
        command = "yum info {0} | grep Version | cut -d ':' -f 2 || true".format(package_name)
        if client is None:
            return check_output(command, shell=True).strip()
        return client.run(command, allow_insecure=True).strip()

    @staticmethod
    def get_versions(client):
        versions = {}
        for package_name in RpmPackage.OVS_PACKAGE_NAMES:
            version_info = RpmPackage._get_version(package_name, client)
            if version_info:
                versions[package_name] = version_info
        return versions

    @staticmethod
    def install(package_name, client, force=False):
        _ = force
        counter = 0
        max_counter = 3
        while counter < max_counter:
            counter += 1
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
            time.sleep(1)

    @staticmethod
    def update(client):
        try:
            client.run(['yum', 'check-update'])
        except CalledProcessError as cpe:
            # Returns exit value of 100 if there are packages available for an update
            if cpe.returncode != 100:
                RpmPackage._logger.error('Update failed. Error: {0}'.format(cpe.output))
                raise cpe

    @staticmethod
    def verify_update_required(packages, services, client):
        services_checked = []
        update_info = {'version': '',
                       'packages': [],
                       'services': []}
        for package_name in packages:
            installed = None
            candidate = None
            for line in client.run(['yum', 'list', package_name]).splitlines():
                if line.startswith(package_name):
                    version = line.split()
                    if len(version) > 1:
                        if not installed:
                            installed = version[1]
                        else:
                            candidate = version[1]

                if installed is not None and candidate is not None:
                    break

            if candidate is not None and candidate != installed:
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
                        if candidate is not None and running_version not in candidate:
                            update_info['services'].append(service)
                            update_info['version'] = candidate
        return update_info

# Copyright 2015 CloudFounders NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Rpm Package module
"""
import time
from ovs.log.logHandler import LogHandler
from subprocess import check_output
from subprocess import CalledProcessError

logger = LogHandler.get('lib', name='packager')

class RpmPackage(object):
    """
    Contains all logic related to Rpm packages (used in e.g. Centos)
    """

    OVS_PACKAGE_NAMES = ['openvstorage', 'openvstorage-backend', 'volumedriver-server', 'volumedriver-base', 'alba', 'openvstorage-sdm']

    @staticmethod
    def _get_version(package_name):
        return check_output("yum info {0} | grep Version | cut -d ':' -f 2 || true".format(package_name), shell=True).strip()

    @staticmethod
    def get_versions():
        versions = {}
        for package_name in RpmPackage.OVS_PACKAGE_NAMES:
            version_info = RpmPackage._get_version(package_name)
            if version_info:
                versions[package_name] = version_info
        return versions

    @staticmethod
    def install(package_name, client, force=False):
        counter = 0
        max_counter = 3
        while counter < max_counter:
            counter += 1
            try:
                client.run('yum update -y {0}'.format(package_name))
                break
            except CalledProcessError as cpe:
                # Retry 3 times if fail
                if counter == max_counter:
                    logger.error('Install {0} failed. Error: {1}'.format(package_name, cpe.output))
                    raise cpe
            except Exception as ex:
                raise ex
            time.sleep(1)

    @staticmethod
    def update(client):
        try:
            client.run('yum check-update')
        except CalledProcessError as cpe:
            # Returns exit value of 100 if there are packages available for an update
            if cpe.returncode != 100:
                logger.error('Update failed. Error: {0}'.format(cpe.output))
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
            for line in client.run("yum list {0}".format(package_name)).splitlines():
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

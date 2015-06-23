# Copyright 2015 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Debian Package module
"""

from subprocess import check_output


class DebianPackage(object):
    """
    Contains all logic related to Debian packages (used in e.g. Debian, Ubuntu)
    """

    OVS_PACKAGE_NAMES = ['openvstorage', 'openvstorage-backend', 'volumedriver-server', 'volumedriver-base', 'alba', 'openvstorage-sdm']

    @staticmethod
    def _get_version(package_name):
        return check_output("dpkg -s {0} | grep Version | cut -d ' ' -f 2".format(package_name), shell=True).strip()

    @staticmethod
    def get_versions():
        versions = {}
        for package_name in DebianPackage.OVS_PACKAGE_NAMES:
            version_info = DebianPackage._get_version(package_name)
            if version_info:
                versions[package_name] = version_info
        return versions

    @staticmethod
    def get_installed_and_candidate_version(package_name):
        installed = None
        candidate = None
        for line in check_output('apt-cache policy {0}'.format(package_name), shell=True).splitlines():
            line = line.strip()
            if line.startswith('Installed:'):
                installed = line.lstrip('Installed:').strip()
            elif line.startswith('Candidate:'):
                candidate = line.lstrip('Candidate:').strip()

            if installed is not None and candidate is not None:
                break
        return installed, candidate

    @staticmethod
    def install(package_name, client, force=False):
        force_text = '--force-yes' if force is True else ''
        return client.run('apt-get install -y {0} {1}'.format(force_text, package_name))

    @staticmethod
    def update(client, configuration_string=''):
        return client.run('apt-get update {0}'.format(configuration_string))

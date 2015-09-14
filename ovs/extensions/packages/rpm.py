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
Rpm Package module
"""

from subprocess import check_output


class RpmPackage(object):
    """
    Contains all logic related to Rpm packages (used in e.g. Centos)
    """

    OVS_PACKAGE_NAMES = ['volumedriver-server', 'volumedriver-base']

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
        raise NotImplementedError("Installing RPM packages not yet implemented")

    @staticmethod
    def update(client):
        raise NotImplementedError("Updating RPM packages not yet implemented")

    @staticmethod
    def verify_update_required(packages, services, client):
        raise NotImplementedError("Verifying RPM packages not yet implemented")

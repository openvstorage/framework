# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OVS migration module
"""


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'  # Used by migrator.py, so don't remove

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version, master_ips=None, extra_ips=None):
        """
        Migrates from any version to any version, running all migrations required
        If previous_version is for example 0 and this script is at
        verison 3 it will execute two steps:
          - 1 > 2
          - 2 > 3
        :param previous_version: The previous version from which to start the migration.
        :param master_ips: IP addresses of the MASTER nodes
        :param extra_ips: IP addresses of the EXTRA nodes
        """

        working_version = previous_version

        # Version 1 introduced:
        # - Flexible SSD layout
        if working_version < 1:
            from ovs.extensions.generic.configuration import Configuration
            if Configuration.exists('ovs.arakoon'):
                Configuration.delete('ovs.arakoon', remove_root=True)
            Configuration.set('ovs.core.ovsdb', '/opt/OpenvStorage/db')

            working_version = 1

        # Version 2 introduced:
        # - Registration
        if working_version < 2:
            import time
            from ovs.extensions.generic.configuration import Configuration
            if not Configuration.exists('ovs.core.registered'):
                Configuration.set('ovs.core.registered', False)
                Configuration.set('ovs.core.install_time', time.time())

            working_version = 2

        # Version 3 introduced:
        # - New arakoon clients
        if working_version < 3:
            from ovs.extensions.db.arakoon import ArakoonInstaller
            reload(ArakoonInstaller)
            from ovs.extensions.db.arakoon import ArakoonInstaller
            from ovs.extensions.generic.sshclient import SSHClient
            from ovs.extensions.generic.configuration import Configuration
            if master_ips is not None:
                for ip in master_ips:
                    client = SSHClient(ip)
                    if client.dir_exists(ArakoonInstaller.ArakoonInstaller.ARAKOON_CONFIG_DIR):
                        for cluster_name in client.dir_list(ArakoonInstaller.ArakoonInstaller.ARAKOON_CONFIG_DIR):
                            try:
                                ArakoonInstaller.ArakoonInstaller.deploy_cluster(cluster_name, ip)
                            except:
                                pass
            if Configuration.exists('ovs.core.storage.persistent'):
                Configuration.set('ovs.core.storage.persistent', 'pyrakoon')

            working_version = 3

        return working_version

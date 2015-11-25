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
    def migrate(previous_version):
        """
        Migrates from any version to any version, running all migrations required
        If previous_version is for example 0 and this script is at
        verison 3 it will execute two steps:
          - 1 > 2
          - 2 > 3
        @param previous_version: The previous version from which to start the migration.
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

        return working_version

# Copyright 2014 CloudFounders NV
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
OVS SNMP bootstrap module
"""
from ovs.extensions.snmp.server import SNMPServer
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.exceptions import KeyNotFoundException
from ovs.plugin.provider.configuration import Configuration

import signal

STORAGE_PREFIX = "ovs_snmp"

class OVSSNMPServer():
    """
    Bootstrap the SNMP Server, hook into ovs
    """
    def __init__(self):
        """
        Init
        """
        signal.signal(signal.SIGTERM, self.SIGTERM)

        from ovs.extensions.generic.system import Ovs
        my_vsa = Ovs.get_my_vsa()
        host = my_vsa.ip
        port = 161

        self.persistent = PersistentFactory.get_client()
        users = self.get_users()
        # Load from model
        assigned_oids = {}

        self.server = SNMPServer(host = host, port = port, users = users, assigned_oids = assigned_oids)

    def get_users(self):
        """
        Returns all saved users from the database
        """
        user_prefix = "{}_user_".format(STORAGE_PREFIX)
        users = self.persistent.prefix(user_prefix)
        return [self.persistent.get(user) for user in users]

    def add_user(self, username, password, privatekey):
        """
        Adds an snmp v3 user to the database
        """
        storage_key = "{}_user_{}".format(STORAGE_PREFIX, username)
        value = (username, password, privatekey, 'authPriv')
        self.persistent.set(storage_key, value)

    def configure(self, group, key, value):
        """
        Store/Update a key in persisten storage
        e.g "dal", "enabled", True
        """
        storage_key = "{}_config_{}_{}".format(STORAGE_PREFIX, group, key)
        self.persistent.set(storage_key, value)

    def _bootstrap_dal_models(self):
        """
        Load/hook dal models as snmp oids
        """
        enabled_key = "{}_config_dal_enabled".format(STORAGE_PREFIX)
        try:
            enabled = self.persistent.get(enabled_key)
        except KeyNotFoundException:
            enabled = False
        if enabled:
            from ovs.dal.lists.vdisklist import VDiskList
            from ovs.dal.lists.vmachinelist import VMachineList
            from ovs.dal.lists.pmachinelist import PMachineList
            from ovs.dal.lists.vpoollist import VPoolList
            from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
            from ovs.dal.lists.mgmtcenterlist import MgmtCenterList

            # TODO: extend with required properties
            # TODO: implement dal decorator to mark properties to be exposed
            for vm in VMachineList.get_vmachines():
                self.server.register_custom_oid_for_model(0, vm, [lambda x: x.statistics['data_transferred_ps'],
                                                                  lambda x: x.statistics['write_operations_ps']
                                                                  ])

            for vd in VDiskList.get_vdisks():
                self.server.register_custom_oid_for_model(1, vd, [lambda x: x.statistics['operations_ps'],
                                                                  ])

    def start(self):
        """
        Start
        """
        self._bootstrap_dal_models()
        self.server.start()

    def SIGTERM(self, signum, frame):
        """
        Clean stop on SIGTERM
        """
        print('Got sigterm...')
        self.server.stop()

if __name__ == '__main__':
    server = OVSSNMPServer()

    server.start()
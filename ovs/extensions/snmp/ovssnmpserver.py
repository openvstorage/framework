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
NAMING_SCHEME = "1.3.6.1.4.1.0.%s.%s.%s"

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
        self.assigned_oids = {}
        self.instance_oid = 0
        self.server = SNMPServer(host = host, port = port, users = users, naming_scheme = NAMING_SCHEME)

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
        Store/Update a key in persistent storage
        e.g "dal", "enabled", True
        """
        storage_key = "{}_config_{}_{}".format(STORAGE_PREFIX, group, key)
        self.persistent.set(storage_key, value)

    def _save_model_oid(self, guid, oid, attribute):
        """
        Store the mapping between oid and object guid
        """
        key = "{}_dal2oid_{}_{}".format(STORAGE_PREFIX, guid, attribute)
        self.persistent.set(key, oid)

    def _get_model_oid(self, guid, attribute):
        """
        Return the oid for a specific guid/attribute
        """
        key = "{}_dal2oid_{}_{}".format(STORAGE_PREFIX, guid, attribute)
        try:
            return self.persistent.get(key)
        except KeyNotFoundException:
            return None

    def _get_mappings(self, guid):
        """
        Return the oids and the attributes - dict
        """
        mapping = {}
        key = "{}_dal2oid_{}_".format(STORAGE_PREFIX, guid)
        keys = self.persistent.prefix(key)
        for key in keys:
            oid = self.persistent.get(key)
            attr_name = key.replace(STORAGE_PREFIX, '').replace('_dal2oid_', '')
            guid = attr_name.split('_')[0]
            attr_name = attr_name.replace('{}_'.format(guid), '')
            if not guid in mapping:
                mapping[guid] = {}
            mapping[guid][oid] = attr_name
        return mapping

    def _register_dal_model(self, class_id, model_object, attribute, key=None, atype=str):
        """
        Register a DAL model as OID
        class_id is the unique id of the type
        an unique id for the instance will be generated
        an unique id for the attribute will be generated
        together they will form oid that will be stored in the model
        """
        if not class_id in self.assigned_oids:
            self.assigned_oids[class_id] = {}
            self.instance_oid = 0

        if not self.instance_oid in self.assigned_oids[class_id]:
            self.assigned_oids[class_id][self.instance_oid] = {}
            self.attrb_oid = 0

        key_name = "{}_{}".format(attribute, key) if key else attribute
        existing = self._get_model_oid(model_object.guid, key_name)
        if existing:
            # there is already an oid assigned for this attribute
            oid = existing
            _oid = oid.split('.')
            self.instance_oid = int(_oid[-2])
            if not self.instance_oid in self.assigned_oids[class_id]:
                self.assigned_oids[class_id][self.instance_oid] = {}
            self.attrb_oid = int(_oid[-1])
        else:
            # there is no oid assigned for this
            pass

        while True:
            existing = self.assigned_oids[class_id][self.instance_oid].get(self.attrb_oid, None)
            if existing:
                if existing == (model_object, attribute):
                    #  Already modeled correctly
                    return
                else:
                    #  Something is present here but not the expected model_object
                    self.instance_oid += 1
                    self.attrb_oid = 0
            else:
                # Nothing exists, so we add here
                self.assigned_oids[class_id][self.instance_oid][self.attrb_oid] = (model_object, attribute)
                def get_function():
                    value = getattr(model_object, attribute)
                    if key and isinstance(value, dict):
                        return value[key]
                    elif key:
                        return getattr(value, key)
                    return value

                oid = self.server.register_custom_oid(class_id, self.instance_oid, self.attrb_oid, get_function, atype)
                self.attrb_oid += 1
                self._save_model_oid(model_object.guid, oid, "{}_{}".format(attribute, key) if key else attribute)
                return oid


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
                if vm.is_internal:
                    self._register_dal_model(10, vm, 'name')
                    self._register_dal_model(10, vm, 'hypervisor_status')
                    self._register_dal_model(10, vm, 'pmachine', 'host_status')
                else:
                    self._register_dal_model(0, vm, 'name')
                    for key in vm.statistics.keys():
                        self._register_dal_model(0, vm, 'statistics', key, atype = int)
                    self._register_dal_model(0, vm, 'stored_data', atype = int)
                self.instance_oid += 1

            for vd in VDiskList.get_vdisks():
                self._register_dal_model(1, vd, 'name')
                for key in vd.statistics.keys():
                    self._register_dal_model(1, vd, 'statistics', key, atype = int)
                self.instance_oid += 1

            for pm in PMachineList.get_pmachines():
                pass

            for vp in VPoolList.get_vpools():
                self._register_dal_model(3, vp, 'name')
                for key in vp.statistics.keys():
                    self._register_dal_model(3, vp, 'statistics', key, atype = int)
            for vsr in VolumeStorageRouterList.get_volumestoragerouters():
                self._register_dal_model(4, vsr, 'name')
                self._register_dal_model(4, vsr, 'stored_data', atype = int)

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
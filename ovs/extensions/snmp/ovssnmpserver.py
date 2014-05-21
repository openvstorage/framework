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
        self.host = my_vsa.ip
        self.port = 161

        self.persistent = PersistentFactory.get_client()
        self.users = self.get_users()
        # Load from model
        self.assigned_oids = {}
        self.instance_oid = 0

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

    def get_mappings(self, guid):
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

    def _register_dal_model(self, class_id, model_object, attribute, attrb_oid, key=None, atype=str):
        """
        Register a DAL model as OID
        class_id is the unique id of the type
        an unique id for the instance will be generated
        attrb_oid: an unique id for the attribute (hardcoded)
        together they will form oid that will be stored in the model
        """
        if not class_id in self.assigned_oids:
            self.assigned_oids[class_id] = {}
            self.instance_oid = 0

        if not self.instance_oid in self.assigned_oids[class_id]:
            self.assigned_oids[class_id][self.instance_oid] = {}


        for instance_id in self.assigned_oids[class_id]:
            for attr_id in self.assigned_oids[class_id][instance_id]:
                existing = self.assigned_oids[class_id][instance_id][attr_id]
                if existing[0].guid == model_object.guid and existing[1] == attribute + str(key):
                    #  Already modeled correctly
                    return

        # Nothing exists, so we add here
        self.assigned_oids[class_id][self.instance_oid][attrb_oid] = (model_object, attribute + str(key))
        def get_function():
            print('[DEBUG] Get function for %s %s %s' % (model_object.guid, attribute, str(key)))
            value = getattr(model_object, attribute)
            if key and isinstance(value, dict):
                return value[key]
            elif key:
                return getattr(value, key)
            return value

        oid = self.server.register_custom_oid(class_id, self.instance_oid, attrb_oid, get_function, atype)
        self._save_model_oid(model_object.guid, oid, "{}_{}".format(attribute, key) if key else attribute)
        return oid


    def _bootstrap_dal_models(self):
        """
        Load/hook dal models as snmp oids
        """
        enabled_key = "{}_config_dal_enabled".format(STORAGE_PREFIX)
        self.instance_oid = 0
        try:
            enabled = self.persistent.get(enabled_key)
        except KeyNotFoundException:
            enabled = True # Enabled by default, can be disabled by setting the key
        if enabled:
            from ovs.dal.lists.vdisklist import VDiskList
            from ovs.dal.lists.vmachinelist import VMachineList
            from ovs.dal.lists.pmachinelist import PMachineList
            from ovs.dal.lists.vpoollist import VPoolList
            from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList

            for vm in VMachineList.get_vmachines():
                if vm.is_internal:
                    self._register_dal_model(10, vm, 'name', "1")
                    self._register_dal_model(10, vm, 'hypervisor_status', "2")
                    self._register_dal_model(10, vm, 'pmachine', "3", key = 'host_status')
                    self.instance_oid += 1

            for vm in VMachineList.get_vmachines():
                if not vm.is_internal:
                    self._register_dal_model(0, vm, 'name', "1")
                    self._register_dal_model(0, vm, 'statistics', "2.0", key = "operations", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.1", key = "cluster_cache_misses_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.2", key = "data_read", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.3", key = "sco_cache_misses", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.4", key = "sco_cache_hits_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.5", key = "sco_cache_hits", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.6", key = "write_operations", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.7", key = "cluster_cache_misses", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.8", key = "read_operations_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.9", key = "sco_cache_misses_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.10", key = "backend_write_operations", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.11", key = "backend_data_read", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.12", key = "cache_hits", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.13", key = "backend_write_operations_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.14", key = "metadata_store_hits_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.15", key = "metadata_store_misses", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.16", key = "backend_data_written", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.17", key = "data_read_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.18", key = "read_operations", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.19", key = "cluster_cache_hits", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.20", key = "data_written_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.21", key = "cluster_cache_hits_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.22", key = "cache_hits_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.23", key = "timestamp", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.24", key = "metadata_store_misses_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.25", key = "backend_data_written_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.26", key = "backend_read_operations", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.27", key = "data_written", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.28", key = "metadata_store_hits", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.29", key = "backend_data_read_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.30", key = "operations_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.31", key = "backend_read_operations_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.32", key = "data_transferred_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.33", key = "write_operations_ps", atype = int)
                    self._register_dal_model(0, vm, 'statistics', "2.34", key = "data_transferred", atype = int)
                    self._register_dal_model(0, vm, 'stored_data', "3", atype = int)
                self.instance_oid += 1

            for vd in VDiskList.get_vdisks():
                self._register_dal_model(1, vd, 'name', "1")
                self._register_dal_model(1, vd, 'statistics', "2.0", key = "operations", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.1", key = "data_written_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.2", key = "data_read", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.3", key = "sco_cache_misses", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.4", key = "sco_cache_hits_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.5", key = "sco_cache_hits", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.6", key = "write_operations", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.7", key = "cluster_cache_misses", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.8", key = "read_operations_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.9", key = "sco_cache_misses_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.10", key = "backend_write_operations", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.11", key = "backend_data_read", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.12", key = "cache_hits", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.13", key = "backend_write_operations_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.14", key = "metadata_store_hits_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.15", key = "metadata_store_misses", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.16", key = "backend_data_written", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.17", key = "data_read_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.18", key = "read_operations", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.19", key = "cluster_cache_hits", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.20", key = "cluster_cache_misses_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.21", key = "cluster_cache_hits_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.22", key = "cache_hits_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.23", key = "timestamp", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.24", key = "metadata_store_misses_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.25", key = "backend_data_written_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.26", key = "backend_read_operations", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.27", key = "data_written", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.28", key = "metadata_store_hits", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.29", key = "backend_data_read_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.30", key = "operations_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.31", key = "backend_read_operations_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.32", key = "data_transferred_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.33", key = "write_operations_ps", atype = int)
                self._register_dal_model(1, vd, 'statistics', "2.34", key = "data_transferred", atype = int)
                self.instance_oid += 1

            for pm in PMachineList.get_pmachines():
                self._register_dal_model(2, pm, 'name', "1")
                self._register_dal_model(2, pm, 'host_status', "2")
                self.instance_oid += 1

            for vp in VPoolList.get_vpools():
                self._register_dal_model(3, vp, 'name', "1")
                self._register_dal_model(3, vp, 'statistics', "2.0", key = "operations", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.1", key = "cluster_cache_misses_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.2", key = "data_read", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.3", key = "sco_cache_misses", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.4", key = "sco_cache_hits_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.5", key = "sco_cache_hits", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.6", key = "write_operations", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.7", key = "cluster_cache_misses", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.8", key = "read_operations_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.9", key = "sco_cache_misses_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.10", key = "backend_write_operations", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.11", key = "backend_data_read", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.12", key = "cache_hits", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.13", key = "backend_write_operations_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.14", key = "metadata_store_hits_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.15", key = "metadata_store_misses", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.16", key = "backend_data_written", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.17", key = "data_read_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.18", key = "read_operations", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.19", key = "cluster_cache_hits", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.20", key = "data_written_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.21", key = "cluster_cache_hits_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.22", key = "cache_hits_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.23", key = "timestamp", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.24", key = "metadata_store_misses_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.25", key = "backend_data_written_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.26", key = "backend_read_operations", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.27", key = "data_written", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.28", key = "metadata_store_hits", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.29", key = "backend_data_read_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.30", key = "operations_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.31", key = "backend_read_operations_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.32", key = "data_transferred_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.33", key = "write_operations_ps", atype = int)
                self._register_dal_model(3, vp, 'statistics', "2.34", key = "data_transferred", atype = int)
                self.instance_oid += 1

            for vsr in VolumeStorageRouterList.get_volumestoragerouters():
                self._register_dal_model(4, vsr, 'name', "1")
                self._register_dal_model(4, vsr, 'stored_data', "2", atype = int)
                self.instance_oid += 1

    def _polling_functions(self):
        def _poll(timestamp_float):
            print('[POLLING] %s' % (str(timestamp_float)))
            self._bootstrap_dal_models()
            print('[DONE POLLING]')

        self.server.register_polling_function(_poll, 300) #5 minutes

    def start(self):
        """
        Start
        """
        self.server = SNMPServer(host = self.host,
                                 port = self.port,
                                 users = self.users,
                                 naming_scheme = NAMING_SCHEME)
        self._polling_functions()
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
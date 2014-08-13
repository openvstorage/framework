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
from ovs.dal.dataobjectlist import DataObjectList

import signal

STORAGE_PREFIX = "ovs_snmp"
NAMING_SCHEME = "1.3.6.1.4.1.29961.%s.%s.%s"


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
        my_storagerouter = Ovs.get_my_storagerouter()
        self.host = my_storagerouter.ip
        self.port = 161

        self.persistent = PersistentFactory.get_client()
        self.users = self.get_users()
        # Load from model
        self.assigned_oids = {}
        self.instance_oid = 0
        # Book-keeping
        self.model_oids = set()

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
            mapping[oid] = attr_name
        return mapping

    def _register_dal_model(self, class_id, model_object, attribute, attrb_oid, key=None, func=None, atype=str):
        """
        Register a DAL model as OID
        class_id is the unique id of the type
        an unique id for the instance will be generated
        attrb_oid: an unique id for the attribute (hardcoded)
        together they will form oid that will be stored in the model
        """
        self.model_oids.add(model_object.guid)

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
            if func:
                print('[DEBUG] Calling lambda function %s' % func)
                return func(model_object)

            try:
                value = getattr(model_object, attribute)
                if key and isinstance(value, dict):
                    value = value[key]
                elif key:
                    value = getattr(value, key)
                elif not key and (isinstance(value, list) or isinstance(value, DataObjectList)):
                    value = len(value)
            except Exception as ex:
                print('[EXCEPTION] %s' % (str(ex)))
                if atype == int:
                    value = -1
                elif atype == str:
                    value = 'N/A'
            try:
                return atype(value)
            except Exception as ex:
                print('[EXCEPTION 2] %s' % (str(ex)))
                return 0

        oid = self.server.register_custom_oid(class_id, self.instance_oid, attrb_oid, get_function, atype)
        self._save_model_oid(model_object.guid, oid, "{}_{}".format(attribute, key) if key else attribute)
        return oid


    def _bootstrap_dal_models(self):
        """
        Load/hook dal models as snmp oids
        """
        _guids = set()

        enabled_key = "{}_config_dal_enabled".format(STORAGE_PREFIX)
        self.instance_oid = 0
        try:
            enabled = self.persistent.get(enabled_key)
        except KeyNotFoundException:
            enabled = True # Enabled by default, can be disabled by setting the key
        if enabled:
            from ovs.dal.lists.vdisklist import VDiskList
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            from ovs.dal.lists.pmachinelist import PMachineList
            from ovs.dal.lists.vmachinelist import VMachineList
            from ovs.dal.lists.vpoollist import VPoolList
            from ovs.dal.lists.storagedriverlist import StorageDriverList

            for storagerouter in StorageRouterList.get_storagerouters():
                _guids.add(storagerouter.guid)

                self._register_dal_model(10, storagerouter, 'guid', "0")
                self._register_dal_model(10, storagerouter, 'name', "1")
                self._register_dal_model(10, storagerouter, 'pmachine', "3", key = 'host_status')
                self._register_dal_model(10, storagerouter, 'description', "4")
                self._register_dal_model(10, storagerouter, 'devicename', "5")
                self._register_dal_model(10, storagerouter, 'failover_mode', "6")
                self._register_dal_model(10, storagerouter, 'ip', "8")
                self._register_dal_model(10, storagerouter, 'machineid', "9")
                self._register_dal_model(10, storagerouter, 'status', "10")
                self._register_dal_model(10, storagerouter, '#vdisks', "11",
                                         func = lambda storagerouter: len([vdisk for vpool_vdisks in [storagedriver.vpool.vdisks for storagedriver in storagerouter.storagedrivers] for vdisk in vpool_vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id]),
                                         atype = int)
                self._register_dal_model(10, storagerouter, '#vmachines', "12",
                                         func = lambda storagerouter: len(set([vdisk.vmachine.guid for vpool_vdisks in [storagedriver.vpool.vdisks for storagedriver in storagerouter.storagedrivers] for vdisk in vpool_vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id])),
                                         atype = int)
                self._register_dal_model(10, storagerouter, '#stored_data', "13",
                                         func = lambda storagerouter: sum([vdisk.vmachine.stored_data for vpool_vdisks in [storagedriver.vpool.vdisks for storagedriver in storagerouter.storagedrivers] for vdisk in vpool_vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id]),
                                         atype = int)
                self.instance_oid += 1

            for vm in VMachineList.get_vmachines():
                _guids.add(vm.guid)

                if vm.is_vtemplate:
                    self._register_dal_model(11, vm, 'guid', "0")
                    self._register_dal_model(11, vm, 'name', "1")
                    def _children(vmt):
                        children = 0
                        disks = [vd.guid for vd in vmt.vdisks]
                        for vdisk in [vdisk.parent_vdisk_guid for item in [vm.vdisks for vm in VMachineList.get_vmachines() if not vm.is_vtemplate] for vdisk in item]:
                            for disk in disks:
                                if vdisk == disk:
                                    children += 1
                        return children
                    self._register_dal_model(11, vm, '#children', 2, func = _children, atype = int)
                    self.instance_oid += 1

            for vm in VMachineList.get_vmachines():
                _guids.add(vm.guid)

                if not vm.is_vtemplate:
                    self._register_dal_model(0, vm, 'guid', "0")
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
                    self._register_dal_model(0, vm, 'description', "4")
                    self._register_dal_model(0, vm, 'devicename', "5")
                    self._register_dal_model(0, vm, 'failover_mode', "6")
                    self._register_dal_model(0, vm, 'hypervisorid', "7")
                    self._register_dal_model(0, vm, 'ip', "8")
                    self._register_dal_model(0, vm, 'status', "10")
                    self._register_dal_model(0, vm, 'stored_data', "10", atype = int)
                    self._register_dal_model(0, vm, 'snapshots', "11", atype = int)
                    self._register_dal_model(0, vm, 'vdisks', "12", atype = int)
                    self._register_dal_model(0, vm, 'FOC', '13',
                                             func = lambda vm: 'DEGRADED' if all(item == 'DEGRADED' for item in [vd.info['failover_mode'] for vd in vm.vdisks]) else 'OK')
                self.instance_oid += 1

            for vd in VDiskList.get_vdisks():
                _guids.add(vd.guid)

                self._register_dal_model(1, vd, 'guid', "0")
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
                self._register_dal_model(1, vd, 'info', "3", key = 'stored', atype = int)
                self._register_dal_model(1, vd, 'info', "4", key = 'failover_mode', atype = int)
                self._register_dal_model(1, vd, 'snapshots', "5", atype = int)
                self.instance_oid += 1

            for pm in PMachineList.get_pmachines():
                _guids.add(pm.guid)

                self._register_dal_model(2, pm, 'guid', "0")
                self._register_dal_model(2, pm, 'name', "1")
                self._register_dal_model(2, pm, 'host_status', "2")
                self.instance_oid += 1

            for vp in VPoolList.get_vpools():
                _guids.add(vp.guid)

                self._register_dal_model(3, vm, 'guid', "0")
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
                self._register_dal_model(3, vp, 'status', "3")
                self._register_dal_model(3, vp, 'description', "4")
                self._register_dal_model(3, vp, 'vdisks', "5", atype = int)
                self._register_dal_model(3, vp, '#vmachines', "6",
                                         func = lambda vp: len(set([vd.vmachine.guid for vd in vp.vdisks])),
                                         atype = int)
                self.instance_oid += 1

            for storagedriver in StorageDriverList.get_storagedrivers():
                _guids.add(storagedriver.guid)

                self._register_dal_model(4, storagedriver, 'guid', "0")
                self._register_dal_model(4, storagedriver, 'name', "1")
                self._register_dal_model(4, storagedriver, 'stored_data', "2", atype = int)
                self.instance_oid += 1

            reload = False
            for object_guid in list(self.model_oids):
                if not object_guid in _guids:
                    self.model_oids.remove(object_guid)
                    reload = True
            if reload:
                self._reload_snmp()

    def _polling_functions(self):
        def _poll(timestamp_float):
            print('[POLLING] %s' % (str(timestamp_float)))
            self._bootstrap_dal_models()
            print('[DONE POLLING]')
        self.server.register_polling_function(_poll, 300) #5 minutes

    def _reload_snmp(self):
        """
        Restart snmp
        """
        print('[SNMP] Reload started')
        import os
        os.system('echo "service ovs-snmp restart" | at now')

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

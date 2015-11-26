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
MDSService test module
"""
import unittest
import sys
import json
from unittest import TestCase
from ovs.lib.tests.mockups import StorageDriverModule, StorageDriverClient
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore


class MDSServices(TestCase):
    """
    This test class will validate the various scenarios of the MDSService logic
    """

    client = None
    BackendType = None
    FailureDomain = None
    MDSService = None
    MDSServiceController = None
    MDSServiceVDisk = None
    PMachine = None
    Service = None
    ServiceType = None
    StorageDriver = None
    StorageRouter = None
    VDisk = None
    VolatileMutex = None
    VPool = None
    logLevel = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        # Load dummy stores
        VolatileFactory.store = DummyVolatileStore()
        PersistentFactory.store = DummyPersistentStore()
        DummyVolatileStore._keep_in_memory_only = True
        DummyPersistentStore._keep_in_memory_only = True

        # Replace mocked classes
        sys.modules['ovs.extensions.storageserver.storagedriver'] = StorageDriverModule
        # Retrieve persistent factory client
        # Import required modules/classes after mocking is done
        from ovs.dal.hybrids.backendtype import BackendType
        from ovs.dal.hybrids.failuredomain import FailureDomain
        from ovs.dal.hybrids.j_mdsservice import MDSService
        from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.hybrids.service import Service
        from ovs.dal.hybrids.servicetype import ServiceType
        from ovs.dal.hybrids.storagedriver import StorageDriver
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.hybrids.vdisk import VDisk
        from ovs.dal.hybrids.vpool import VPool
        from ovs.extensions.generic.volatilemutex import VolatileMutex
        from ovs.lib.mdsservice import MDSServiceController
        # Globalize mocked classes
        global client
        global BackendType
        global FailureDomain
        global MDSService
        global MDSServiceController
        global MDSServiceVDisk
        global PMachine
        global Service
        global ServiceType
        global StorageDriver
        global StorageRouter
        global VDisk
        global VolatileMutex
        global VPool
        client = PersistentFactory.get_client()
        _ = BackendType(), FailureDomain(), MDSService(), MDSServiceController, MDSServiceVDisk(), \
            PMachine(), Service(), ServiceType(), StorageDriver(), StorageRouter(), VDisk(), VolatileMutex('dummy'), VPool()

        # Configuration
        def _get(key):
            if client.exists(key):
                return client.get(key)

        Configuration.get = staticmethod(_get)

        # Cleaning storage
        VolatileFactory.store.clean()
        PersistentFactory.store.clean()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()

        client.set('ovs.logging.path', '/var/log/ovs')
        client.set('ovs.logging.level', 'DEBUG')
        client.set('ovs.logging.default_file', 'generic')
        client.set('ovs.logging.default_name', 'logger')

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        pass

    def _generate_nc_function(self, address, mds_service):
        """
        Generates the lambda that will return the address or ip
        """
        _ = self
        if address is True:
            return lambda s: mds_service.service.storagerouter.ip
        return lambda s: int(mds_service.service.ports[0])

    def _generate_bc_function(self, _configs):
        """
        Generates the lambda that will return the config list
        """
        _ = self
        return lambda s: _configs

    def _build_service_structure(self, structure):
        """
        Builds an MDS service structure
        """
        vpools = {}
        services = {}
        mds_services = {}
        storagerouters = {}
        storagedrivers = {}
        failure_domains = {}
        service_type = ServiceType()
        service_type.name = 'MetadataServer'
        service_type.save()
        backend_type = BackendType()
        backend_type.name = 'BackendType'
        backend_type.code = 'BT'
        backend_type.save()
        pmachine = PMachine()
        pmachine.name = 'PMachine'
        pmachine.username = 'root'
        pmachine.ip = '127.0.0.1'
        pmachine.hvtype = 'VMWARE'
        pmachine.save()
        for fd_id in structure['failure_domains']:
            failure_domain = FailureDomain()
            failure_domain.name = 'failure_domain_{0}'.format(fd_id)
            failure_domain.save()
            failure_domains[fd_id] = failure_domain
        for vpool_id in structure['vpools']:
            vpool = VPool()
            vpool.name = str(vpool_id)
            vpool.backend_type = backend_type
            vpool.save()
            vpools[vpool_id] = vpool
        for sr_id, fd_primary_id, fd_secondary_id in structure['storagerouters']:
            storagerouter = StorageRouter()
            storagerouter.name = str(sr_id)
            storagerouter.ip = '10.0.0.{0}'.format(sr_id)
            storagerouter.pmachine = pmachine
            storagerouter.rdma_capable = False
            storagerouter.primary_failure_domain = failure_domains[fd_primary_id]
            if fd_secondary_id is not None:
                storagerouter.secondary_failure_domain = failure_domains[fd_secondary_id]
            storagerouter.save()
            storagerouters[sr_id] = storagerouter
        for sd_id, vpool_id, sr_id in structure['storagedrivers']:
            storagedriver = StorageDriver()
            storagedriver.vpool = vpools[vpool_id]
            storagedriver.storagerouter = storagerouters[sr_id]
            storagedriver.name = str(sd_id)
            storagedriver.mountpoint = '/'
            storagedriver.cluster_ip = storagerouters[sr_id].ip
            storagedriver.storage_ip = '127.0.0.1'
            storagedriver.storagedriver_id = str(sd_id)
            storagedriver.ports = [1, 2, 3]
            storagedriver.save()
            storagedrivers[sd_id] = storagedriver
        for mds_id, sd_id in structure['mds_services']:
            sd = storagedrivers[sd_id]
            s_id = '{0}-{1}'.format(sd.storagerouter.name, mds_id)
            service = Service()
            service.name = s_id
            service.storagerouter = sd.storagerouter
            service.ports = [mds_id]
            service.type = service_type
            service.save()
            services[s_id] = service
            mds_service = MDSService()
            mds_service.service = service
            mds_service.number = 0
            mds_service.capacity = 10
            mds_service.vpool = sd.vpool
            mds_service.save()
            mds_services[mds_id] = mds_service
        return vpools, storagerouters, storagedrivers, services, mds_services, service_type, failure_domains

    def _create_vdisks_for_mds_service(self, amount, start_id, mds_service=None, vpool=None):
        """
        Generates vdisks and appends them to a given mds_service
        """
        vdisks = {}
        storagedriver_id = None
        if mds_service is not None:
            for sd in mds_service.vpool.storagedrivers:
                if sd.storagerouter_guid == mds_service.service.storagerouter_guid:
                    storagedriver_id = sd.storagedriver_id
        for i in xrange(start_id, start_id + amount):
            disk = VDisk()
            disk.name = str(i)
            disk.devicename = 'disk_{0}'.format(i)
            disk.volume_id = 'disk_{0}'.format(i)
            disk.vpool = mds_service.vpool if mds_service is not None else vpool
            disk.size = 0
            disk.save()
            disk.reload_client()
            if mds_service is not None:
                junction = MDSServiceVDisk()
                junction.vdisk = disk
                junction.mds_service = mds_service
                junction.is_master = True
                junction.save()
                config = type('MDSNodeConfig', (),
                              {'address': self._generate_nc_function(True, mds_service),
                               'port': self._generate_nc_function(False, mds_service)})()
                mds_backend_config = type('MDSMetaDataBackendConfig', (),
                                          {'node_configs': self._generate_bc_function([config])})()
                StorageDriverClient.metadata_backend_config['disk_{0}'.format(i)] = mds_backend_config
                StorageDriverClient.catch_up['disk_{0}'.format(i)] = 50
                StorageDriverClient.vrouter_id['disk_{0}'.format(i)] = storagedriver_id
            vdisks[i] = disk
        return vdisks

    def _check_reality(self, configs, loads, vdisks, mds_services, display=False):
        """
        Validates 'reality' with an expected config/load
        """
        reality_configs = [vdisk.info['metadata_backend_config'] for vdisk in vdisks.values()]
        if display is True:
            for c in reality_configs:
                print c
        self.assertListEqual(reality_configs, configs)
        reality_loads = []
        for _mds_service in mds_services.itervalues():
            masters, slaves = 0, 0
            for _junction in _mds_service.vdisks:
                if _junction.is_master:
                    masters += 1
                else:
                    slaves += 1
            capacity = _mds_service.capacity
            if capacity == -1:
                capacity = 'infinite'
            _load, _ = MDSServiceController.get_mds_load(_mds_service)
            if _load == float('inf'):
                _load = 'infinite'
            else:
                _load = round(_load, 2)
            reality_loads.append([_mds_service.service.storagerouter.ip, _mds_service.service.ports[0], masters, slaves, capacity, _load])
        if display is True:
            for l in reality_loads:
                print l
        self.assertListEqual(reality_loads, loads)

    def test_load_calculation(self):
        """
        Validates whether the load calculation works
        MDSServiceController.get_mds_load returns the current load and load in case 1 extra disk would be created for this MDS
            * Current load: Amount of vdisks for this MDS service / Amount of vdisks this MDS service can serve * 100
            * Next load: Amount of vdisks for this MDS service + 1 / Amount of vdisks this MDS service can serve * 100
        This test does:
            * Create 2 vdisks whose MDS service has capacity of 10 (So MDS service can serve 10 disks)
                * Load should be 20%, load_plus should be 30%
            * Create 3 vdisks on same MDS service with capacity of 10
                * Load should become 50%, load plus should become 60%
            * Create another 5 vdisks on same MDS service
                * Load should become 100%, load plus should become 110%
            * Creating additional vdisks should result in a new MDS service being spawned (tested in other test)
            * Set capacity to -1  (means infinite number of disks can be served)
                * Load should ALWAYS return 50%, load_plus should ALWAYS return 50%
            * Set capacity to 0  (also implies infinite, but caught in code, otherwise division by 0 error)
                * Load should ALWAYS return infinite, load_plus should ALWAYS return infinite
        """
        vpools, storagerouters, storagedrivers, services, mds_services, _, _ = self._build_service_structure(
            {'vpools': [1],
             'failure_domains': [1],
             'storagerouters': [(1, 1, None)],  # (<id>, <primary_failure_domain_id>, <secondary_failure_domain_id>)
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        mds_service = mds_services[1]
        vdisks = self._create_vdisks_for_mds_service(2, 1, mds_service=mds_service)
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 20, 'There should be a 20% load. {0}'.format(load))
        self.assertEqual(load_plus, 30, 'There should be a 30% plus load. {0}'.format(load_plus))
        vdisks.update(self._create_vdisks_for_mds_service(3, len(vdisks) + 1, mds_service=mds_service))
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 50, 'There should be a 50% load. {0}'.format(load))
        self.assertEqual(load_plus, 60, 'There should be a 60% plus load. {0}'.format(load_plus))
        vdisks.update(self._create_vdisks_for_mds_service(5, len(vdisks) + 1, mds_service=mds_service))
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 100, 'There should be a 100% load. {0}'.format(load))
        self.assertEqual(load_plus, 110, 'There should be a 110% plus load. {0}'.format(load_plus))
        mds_service.capacity = -1
        mds_service.save()
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 50, 'There should be a 50% load. {0}'.format(load))
        self.assertEqual(load_plus, 50, 'There should be a 50% plus load. {0}'.format(load_plus))
        mds_service.capacity = 0
        mds_service.save()
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, float('inf'), 'There should be infinite load. {0}'.format(load))
        self.assertEqual(load_plus, float('inf'), 'There should be infinite plus load. {0}'.format(load_plus))

    def test_storagedriver_config_set(self):
        """
        Validates whether storagedriver configuration is generated as expected
        MDSServiceController.get_mds_storagedriver_config_set returns the optimal configuration for MDS service deployment for a vpool
            * If possible it will return 3 MDS services (always on different nodes)
            * The 1st node return should be the local node
            * The other nodes are determined as following:
                * Always different storage routers
                * Nodes also serving the corresponding vPool
                * Nodes in same primary failure domain as 1st node with lowest load until safety reached
                * Nodes in secondary failure domain with lowest load until safety reached
        This test does:
            * Create several vpools, storagerouters, storagedrivers, MDS services and failure domains
            * Each MDS service has an initial capacity of 10 vDisks
            * For each MDS service 10 vDisks are created --> Capacity 100% used
            * Capacities for each MDS service are increased
            * Retrieve and validate preferred storage driver config for vpool1
            * Retrieve and validate preferred storage driver config for vpool2
            * Update capacity for 1 MDS service in vpool1 and validate changes in preferred storage driver config
        """
        client.set('ovs.storagedriver.mds.safety', 3)
        vpools, storagerouters, storagedrivers, services, mds_services, _, _ = self._build_service_structure(
            {'vpools': [1, 2],
             'failure_domains': [1, 2],
             'storagerouters': [(1, 1, 2), (2, 2, 1), (3, 1, 2), (4, 2, 1), (5, 1, None), (6, 2, 1)],  # (<id>, <primary_failure_domain_id>, <secondary_failure_domain_id>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 2, 4), (6, 2, 5), (7, 2, 6)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5), (7, 6), (8, 7), (9, 7)]}  # (<id>, <storagedriver_id>)
        )
        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(self._create_vdisks_for_mds_service(10, len(vdisks) + 1, mds_service=mds_service))
        #                                | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | LOAD (in percent) |
        mds_services[1].capacity = 11  # |       1       |   1   |       1    |      2       |       90,9        |
        mds_services[1].save()
        mds_services[2].capacity = 20  # |       1       |   1   |       1    |      2       |       50,0        |
        mds_services[2].save()
        mds_services[3].capacity = 12  # |       2       |   1   |       2    |      1       |       83,3        |
        mds_services[3].save()
        mds_services[4].capacity = 14  # |       3       |   1   |       1    |      2       |       71,4        |
        mds_services[4].save()
        mds_services[5].capacity = 16  # |       4       |   1   |       2    |      1       |       62,5        |
        mds_services[5].save()
        mds_services[6].capacity = 11  # |       4       |   2   |       2    |      1       |       90,9        |
        mds_services[6].save()
        mds_services[7].capacity = 13  # |       5       |   2   |       1    |      -       |       76,9        |
        mds_services[7].save()
        mds_services[8].capacity = 19  # |       6       |   2   |       2    |      1       |       52,6        |
        mds_services[8].save()
        mds_services[9].capacity = 15  # |       6       |   2   |       2    |      1       |       66,6        |
        mds_services[9].save()
        config_vpool1 = MDSServiceController.get_mds_storagedriver_config_set(vpools[1])
        config_vpool2 = MDSServiceController.get_mds_storagedriver_config_set(vpools[2])
        expected_vpool1 = {storagerouters[1].guid: [{'host': '10.0.0.1', 'port': 2},
                                                    {'host': '10.0.0.3', 'port': 4},
                                                    {'host': '10.0.0.4', 'port': 5}],
                           storagerouters[2].guid: [{'host': '10.0.0.2', 'port': 3},
                                                    {'host': '10.0.0.4', 'port': 5},
                                                    {'host': '10.0.0.1', 'port': 2}],
                           storagerouters[3].guid: [{'host': '10.0.0.3', 'port': 4},
                                                    {'host': '10.0.0.1', 'port': 2},
                                                    {'host': '10.0.0.4', 'port': 5}],
                           storagerouters[4].guid: [{'host': '10.0.0.4', 'port': 5},
                                                    {'host': '10.0.0.2', 'port': 3},
                                                    {'host': '10.0.0.1', 'port': 2}]}
        expected_vpool2 = {storagerouters[4].guid: [{'host': '10.0.0.4', 'port': 6},
                                                    {'host': '10.0.0.6', 'port': 8},
                                                    {'host': '10.0.0.5', 'port': 7}],
                           storagerouters[5].guid: [{'host': '10.0.0.5', 'port': 7}],
                           storagerouters[6].guid: [{'host': '10.0.0.6', 'port': 8},
                                                    {'host': '10.0.0.4', 'port': 6},
                                                    {'host': '10.0.0.5', 'port': 7}]}
        self.assertDictEqual(config_vpool1, expected_vpool1, 'Test 1a. Got:\n{0}'.format(json.dumps(config_vpool1, indent=2)))
        self.assertDictEqual(config_vpool2, expected_vpool2, 'Test 1b. Got:\n{0}'.format(json.dumps(config_vpool2, indent=2)))
        mds_services[2].capacity = 10  # |       1       |   1   |       1    |      2       |       100,0       |
        mds_services[2].save()
        config = MDSServiceController.get_mds_storagedriver_config_set(vpools[1])
        expected = {storagerouters[1].guid: [{'host': '10.0.0.1', 'port': 1},
                                             {'host': '10.0.0.3', 'port': 4},
                                             {'host': '10.0.0.4', 'port': 5}],
                    storagerouters[2].guid: [{'host': '10.0.0.2', 'port': 3},
                                             {'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.3', 'port': 4}],
                    storagerouters[3].guid: [{'host': '10.0.0.3', 'port': 4},
                                             {'host': '10.0.0.1', 'port': 1},
                                             {'host': '10.0.0.4', 'port': 5}],
                    storagerouters[4].guid: [{'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.2', 'port': 3},
                                             {'host': '10.0.0.3', 'port': 4}]}
        self.assertDictEqual(config, expected, 'Test 2. Got:\n{0}'.format(json.dumps(config, indent=2)))

    def test_syncreality(self):
        """
        Validates whether reality is synced to the model as expected
        MDSServiceController.sync_vdisk_to_reality will sync the actual disk config retrieved from the storagedriver in our model (MDS service vs vDisk junction)
        This test does:
            * Create several storagerouters, storagedrivers, MDS services
            * Create 5 vDisks which will NOT be linked to any MDS service yet
            * Store the configuration in storage driver
            * Run the sync with reality
            * Verify that the entries in the junction table have been created as expected
        """
        def _test_scenario(scenario):
            """
            Executes a test run for a given scenario
            """
            for disk_id, mds_ids in scenario.iteritems():
                configs = []
                for mds_id in mds_ids:
                    config = type('MDSNodeConfig', (), {'address': self._generate_nc_function(True, mds_services[mds_id]),
                                                        'port': self._generate_nc_function(False, mds_services[mds_id])})()
                    configs.append(config)
                mds_backend_config = type('MDSMetaDataBackendConfig', (), {'node_configs': self._generate_bc_function(configs)})()
                StorageDriverClient.metadata_backend_config[vdisks[disk_id].volume_id] = mds_backend_config

            for vdisk_id in vdisks:
                MDSServiceController.sync_vdisk_to_reality(vdisks[vdisk_id])

            for disk_id, mds_ids in scenario.iteritems():
                expected_mds_services = [mds_services[mds_id] for mds_id in mds_ids]
                disk = vdisks[disk_id]
                self.assertEqual(len(disk.mds_services), len(expected_mds_services))
                for junction in disk.mds_services:
                    self.assertIn(junction.mds_service, expected_mds_services)

        vpools, _, _, _, mds_services, _, _ = self._build_service_structure(
            {'vpools': [1],
             'failure_domains': [1],
             'storagerouters': [(1, 1, None), (2, 1, None), (3, 1, None), (4, 1, None)],  # (<id>, <primary_failure_domain_id>, <secondary_failure_domain_id>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # (<id>, <vpool_id>, <sr_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)]}  # (<id>, <sd_id>)
        )
        vdisks = self._create_vdisks_for_mds_service(5, 1, vpool=vpools[1])
        _test_scenario({1: [1, 3, 4],
                        2: [1, 2],
                        3: [1, 3, 4],
                        4: [3, 4, 5],
                        5: [1, 4, 5]})
        _test_scenario({1: [1, 2],
                        2: [1, 2, 3, 4, 5],
                        3: [1, 2],
                        4: [5],
                        5: [1, 4, 5]})

    def test_ensure_safety(self):
        """
        Validates whether the ensure_safety call works as expected
        MDSServiceController.ensure_safety will make sure that all possible master and slave MDS services are configured correctly in volumedriver
        Following rules apply:
            * If master overloaded (load > threshold), master is demoted to slave, least loaded slave is promoted to master
            * If 1 or more slaves overloaded, new slave will be created based on lowest load
            * All master and slave services will always be on different nodes
            * Master should WILL ALWAYS be on the local node (where the vDisk is hosted)
            * Slaves will be filled up until primary safety reached (taken from primary failure domain)
            * Slaves will be filled up until secondary safety reached if secondary failure domain known
            * Final configuration:
                * Safety of 3 and secondary failure domain known
                    * [MASTER, SLAVE in primary failure domain, SLAVE in secondary failure domain] --> 1st in list in volumedriver config will be treated as master
                * Safety of 3 and secondary failure domain NOT known
                    * [MASTER, SLAVE in primary failure domain, SLAVE in primary failure domain] --> 1st in list in volumedriver config will be treated as master
        This test does:
            * Create 4 storagerouters, 4 storagedrivers, 4 MDS services
            * Create 2 vDisks for each MDS service
            * Sub-Test 1:
                * Run ensure_safety
                * Validate updated configs
            * Sub-Test 2:
                * Run ensure safety again and validate configs, nothing should have changed
            * Sub-Test 3:
                * Overload an MDS service and validate configurations are rebalanced
            * Sub-Test 4:
                * Run ensure safety again and validate configurations
            * Sub-Test 5:
                * Add MDS service on storagerouter with overloaded service
                * Verify an extra slave is added
                * Set tlogs to > threshold and verify nothing changes in config while catch up is ongoing
                * Set tlogs to < threshold and verify multiple MDS services on same storagerouter are removed
            * Sub-Test 6:
                * Migrate a disk to another storagerouter and verify master follows
            * Sub-Test 7: Update failure domain
            * Sub-Test 8: Update backup failure domain
            * Sub-Test 9: Add backup failure domain
            * Sub-Test 10: Remove backup failure domain
            * Sub-Test 11: Increase safety and some more vDisks
            * Sub-Test 12: Decrease safety
            * Sub-Test 13: Change individual vdisk's secondary failure domain
            * Sub-Test 14: Remove individual vdisk's secondary failure domain
        """
        client.set('ovs.storagedriver.mds.safety', 3)
        client.set('ovs.storagedriver.mds.maxload', 75)
        client.set('ovs.storagedriver.mds.tlogs', 100)
        vpools, storagerouters, storagedrivers, _, mds_services, service_type, failure_domains = self._build_service_structure(
            {'vpools': [1],
             'failure_domains': [1, 2],
             'storagerouters': [(1, 1, 2), (2, 1, 2), (3, 2, 1), (4, 2, 1)],  # (<id>, <primary_failure_domain_id>, <secondary_failure_domain_id>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)]}  # (<id>, <storagedriver_id>)
        )
        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(self._create_vdisks_for_mds_service(2, len(vdisks) + 1, mds_service=mds_service))

        # Sub-Test 1: Validate the start configuration which is simple, each disk has only its default local master
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY | LOAD (in percent) |
        # |    1   |       1       |   1   |     1      |      2       |    10    |       20,0        |
        # |    2   |       2       |   1   |     1      |      2       |    10    |       20,0        |
        # |    3   |       3       |   1   |     2      |      1       |    10    |       20,0        |
        # |    4   |       4       |   1   |     2      |      1       |    10    |       20,0        |
        configs = [[{'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.4', 'port': 4}]]
        loads = [['10.0.0.1', 1, 2, 0, 10, 20.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 10, 20.0],
                 ['10.0.0.3', 3, 2, 0, 10, 20.0],
                 ['10.0.0.4', 4, 2, 0, 10, 20.0]]
        self._check_reality(configs, loads, vdisks, mds_services)

        # Validate first run. Each disk should now have sufficient nodes, since there are plenty of MDS services available
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.2', 'port': 2}]]
        loads = [['10.0.0.1', 1, 2, 4, 10, 60.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 4, 10, 60.0],
                 ['10.0.0.3', 3, 2, 4, 10, 60.0],
                 ['10.0.0.4', 4, 2, 4, 10, 60.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 2: Validate whether this extra (unnecessary) run doesn't change anything, preventing reconfiguring over and over again
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 3: Validating whether an overloaded node is correctly rebalanced
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY | LOAD (in percent) |
        # |    1   |       1       |   1   |     1      |      2       |    10    |       20,0        |
        # |    2   |       2       |   1   |     1      |      2       |    2     |      100,0        |
        # |    3   |       3       |   1   |     2      |      1       |    10    |       20,0        |
        # |    4   |       4       |   1   |     2      |      1       |    10    |       20,0        |
        mds_services[2].capacity = 2
        mds_services[2].save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}],  # 4 is recycled and thus gets priority over 3 (load on 4 is higher than on 3 at this point)
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}]]  # MDS 1 will not be used because in previous run it hadn't been configured => next load is calculated which is 80 and > max_load
        loads = [['10.0.0.1', 1, 2, 5, 10, 70.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 2, 100.0],
                 ['10.0.0.3', 3, 2, 5, 10, 70.0],
                 ['10.0.0.4', 4, 2, 5, 10, 70.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 4: Validate whether the overloaded services are still handled
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}],  # 3 and 4 switch around again because load is identical and 3 will have been recycled now
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}]]
        loads = [['10.0.0.1', 1, 2, 5, 10, 70.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 2, 100.0],
                 ['10.0.0.3', 3, 2, 5, 10, 70.0],
                 ['10.0.0.4', 4, 2, 5, 10, 70.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Again, validating whether a subsequent run doesn't give unexpected changes
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 5: An MDS service will be added (next to the overloaded service), this should cause the expected to be rebalanced
        s_id = '{0}-5'.format(storagerouters[2].name)
        service = Service()
        service.name = s_id
        service.storagerouter = storagerouters[2]
        service.ports = [5]
        service.type = service_type
        service.save()
        mds_service = MDSService()
        mds_service.service = service
        mds_service.number = 0
        mds_service.capacity = 10
        mds_service.vpool = vpools[1]
        mds_service.save()
        mds_services[5] = mds_service
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.2', 'port': 5}]]
        loads = [['10.0.0.1', 1, 2, 5, 10, 70.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 2, 100.0],
                 ['10.0.0.3', 3, 2, 4, 10, 60.0],
                 ['10.0.0.4', 4, 2, 4, 10, 60.0],
                 ['10.0.0.2', 5, 0, 5, 10, 50.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # If the tlogs are not caught up, nothing should be changed
        for vdisk_id in [3, 4]:
            StorageDriverClient.catch_up[vdisks[vdisk_id].volume_id] = 1000
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # The next run, after tlogs are caught up, a master switch should be executed
        for vdisk_id in [3, 4]:
            StorageDriverClient.catch_up[vdisks[vdisk_id].volume_id] = 50
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.2', 'port': 5}]]
        loads = [['10.0.0.1', 1, 2, 5, 10, 70.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 2, 50.0],
                 ['10.0.0.3', 3, 2, 4, 10, 60.0],
                 ['10.0.0.4', 4, 2, 4, 10, 60.0],
                 ['10.0.0.2', 5, 1, 3, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 6: Validate whether a volume migration makes the master follow
        StorageDriverClient.vrouter_id[vdisks[1].volume_id] = storagedrivers[3].storagedriver_id
        configs = [[{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.2', 'port': 5}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.2', 'port': 5}]]
        loads = [['10.0.0.1', 1, 1, 5, 10, 60.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 2, 50.0],
                 ['10.0.0.3', 3, 3, 3, 10, 60.0],
                 ['10.0.0.4', 4, 2, 5, 10, 70.0],
                 ['10.0.0.2', 5, 1, 3, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Validates if a second run doesn't change anything
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Clean everything from here on out
        PersistentFactory.store.clean()
        VolatileFactory.store.clean()
        client.set('ovs.storagedriver.mds.safety', 3)
        client.set('ovs.storagedriver.mds.maxload', 75)
        client.set('ovs.storagedriver.mds.tlogs', 100)

        vpools, storagerouters, storagedrivers, _, mds_services, service_type, failure_domains = self._build_service_structure(
            {'vpools': [1],
             'failure_domains': [1, 2, 3],
             'storagerouters': [(1, 1, 2), (2, 1, 2), (3, 1, None), (4, 1, 3), (5, 2, 3), (6, 3, None), (7, 3, 1)],  # (<id>, <primary_failure_domain_id>, <secondary_failure_domain_id>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6), (7, 1, 7)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 2), (4, 3), (5, 4), (6, 5), (7, 5), (8, 6), (9, 7)]}  # (<id>, <storagedriver_id>)
        )
        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(self._create_vdisks_for_mds_service(1, len(vdisks) + 1, mds_service=mds_service))

        # Validate the start configuration which is simple, each disk has only its default local master
        configs = [[{'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.7', 'port': 9}]]
        loads = [['10.0.0.1', 1, 1, 0, 10, 10.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 10, 10.0],
                 ['10.0.0.2', 3, 1, 0, 10, 10.0],
                 ['10.0.0.3', 4, 1, 0, 10, 10.0],
                 ['10.0.0.4', 5, 1, 0, 10, 10.0],
                 ['10.0.0.5', 6, 1, 0, 10, 10.0],
                 ['10.0.0.5', 7, 1, 0, 10, 10.0],
                 ['10.0.0.6', 8, 1, 0, 10, 10.0],
                 ['10.0.0.7', 9, 1, 0, 10, 10.0]]
        self._check_reality(configs, loads, vdisks, mds_services)

        # Validate first run. Each disk should now have sufficient nodes, since there are plenty of MDS services available
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 2}]]
        loads = [['10.0.0.1', 1, 1, 2, 10, 30.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 2, 10, 30.0],
                 ['10.0.0.2', 3, 1, 1, 10, 20.0],
                 ['10.0.0.3', 4, 1, 1, 10, 20.0],
                 ['10.0.0.4', 5, 1, 1, 10, 20.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 1, 10, 20.0],
                 ['10.0.0.6', 8, 1, 4, 10, 50.0],
                 ['10.0.0.7', 9, 1, 3, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 7: Update 2 primary failure domains (Cannot be identical to secondary failure domains)
        storagerouter2 = storagerouters[2]
        storagerouter4 = storagerouters[4]
        storagerouter2.primary_failure_domain = failure_domains[3]
        storagerouter4.primary_failure_domain = failure_domains[2]
        storagerouter2.save()
        storagerouter4.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}]]
        loads = [['10.0.0.1', 1, 1, 2, 10, 30.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 1, 10, 20.0],
                 ['10.0.0.2', 3, 1, 1, 10, 20.0],
                 ['10.0.0.3', 4, 1, 1, 10, 20.0],
                 ['10.0.0.4', 5, 1, 2, 10, 30.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 2, 10, 30.0],
                 ['10.0.0.6', 8, 1, 3, 10, 40.0],
                 ['10.0.0.7', 9, 1, 3, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 8: Update a secondary failure domain (Cannot be identical to primary failure domain)
        storagerouter5 = storagerouters[5]
        storagerouter5.secondary_failure_domain = failure_domains[1]
        storagerouter5.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}]]
        loads = [['10.0.0.1', 1, 1, 3, 10, 40.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 1, 10, 20.0],
                 ['10.0.0.2', 3, 1, 1, 10, 20.0],
                 ['10.0.0.3', 4, 1, 2, 10, 30.0],
                 ['10.0.0.4', 5, 1, 2, 10, 30.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 2, 10, 30.0],
                 ['10.0.0.6', 8, 1, 2, 10, 30.0],
                 ['10.0.0.7', 9, 1, 2, 10, 30.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 9: Add a secondary failure domain (Cannot be identical to primary failure domain)
        storagerouter3 = storagerouters[3]
        storagerouter3.secondary_failure_domain = failure_domains[3]
        storagerouter3.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}]]
        loads = [['10.0.0.1', 1, 1, 3, 10, 40.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 2, 10, 30.0],
                 ['10.0.0.2', 3, 1, 1, 10, 20.0],
                 ['10.0.0.3', 4, 1, 2, 10, 30.0],
                 ['10.0.0.4', 5, 1, 2, 10, 30.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 2, 10, 30.0],
                 ['10.0.0.6', 8, 1, 2, 10, 30.0],
                 ['10.0.0.7', 9, 1, 2, 10, 30.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 10: Remove 2 secondary failure domains
        storagerouter1 = storagerouters[1]
        storagerouter7 = storagerouters[7]
        storagerouter1.secondary_failure_domain = None
        storagerouter7.secondary_failure_domain = None
        storagerouter1.save()
        storagerouter7.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}]]
        loads = [['10.0.0.1', 1, 1, 2, 10, 30.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 2, 10, 30.0],
                 ['10.0.0.2', 3, 1, 1, 10, 20.0],
                 ['10.0.0.3', 4, 1, 2, 10, 30.0],
                 ['10.0.0.4', 5, 1, 2, 10, 30.0],
                 ['10.0.0.5', 6, 1, 1, 10, 20.0],
                 ['10.0.0.5', 7, 1, 2, 10, 30.0],
                 ['10.0.0.6', 8, 1, 3, 10, 40.0],
                 ['10.0.0.7', 9, 1, 2, 10, 30.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 11: Add some more vDisks and increase safety
        client.set('ovs.storagedriver.mds.safety', 5)
        for mds_service in mds_services.itervalues():
            vdisks.update(self._create_vdisks_for_mds_service(1, len(vdisks) + 1, mds_service=mds_service))
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}]]
        loads = [['10.0.0.1', 1, 2, 5, 10, 70.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 4, 10, 60.0],
                 ['10.0.0.2', 3, 2, 4, 10, 60.0],
                 ['10.0.0.3', 4, 2, 5, 10, 70.0],
                 ['10.0.0.4', 5, 2, 5, 10, 70.0],
                 ['10.0.0.5', 6, 2, 3, 10, 50.0],
                 ['10.0.0.5', 7, 2, 3, 10, 50.0],
                 ['10.0.0.6', 8, 2, 5, 10, 70.0],
                 ['10.0.0.7', 9, 2, 5, 10, 70.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 12: Reduce safety
        client.set('ovs.storagedriver.mds.safety', 3)
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}]]
        loads = [['10.0.0.1', 1, 2, 4, 10, 60.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 4, 10, 60.0],
                 ['10.0.0.2', 3, 2, 3, 10, 50.0],
                 ['10.0.0.3', 4, 2, 4, 10, 60.0],
                 ['10.0.0.4', 5, 2, 5, 10, 70.0],
                 ['10.0.0.5', 6, 2, 3, 10, 50.0],
                 ['10.0.0.5', 7, 2, 2, 10, 40.0],
                 ['10.0.0.6', 8, 2, 5, 10, 70.0],
                 ['10.0.0.7', 9, 2, 4, 10, 60.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 13: Change individual vdisk's secondary failure domain
        vdisk5 = vdisks[5]
        vdisk5.secondary_failure_domain = failure_domains[1]
        vdisk5.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}]]
        loads = [['10.0.0.1', 1, 2, 5, 10, 70.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 4, 10, 60.0],
                 ['10.0.0.2', 3, 2, 3, 10, 50.0],
                 ['10.0.0.3', 4, 2, 4, 10, 60.0],
                 ['10.0.0.4', 5, 2, 5, 10, 70.0],
                 ['10.0.0.5', 6, 2, 3, 10, 50.0],
                 ['10.0.0.5', 7, 2, 2, 10, 40.0],
                 ['10.0.0.6', 8, 2, 4, 10, 60.0],
                 ['10.0.0.7', 9, 2, 4, 10, 60.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

        # Sub-Test 14: Remove individual vdisk's secondary failure domain
        vdisk5 = vdisks[5]
        vdisk5.secondary_failure_domain = None
        vdisk5.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.4', 'port': 5}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.2', 'port': 3}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.6', 'port': 8}]]
        loads = [['10.0.0.1', 1, 2, 4, 10, 60.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 4, 10, 60.0],
                 ['10.0.0.2', 3, 2, 4, 10, 60.0],
                 ['10.0.0.3', 4, 2, 4, 10, 60.0],
                 ['10.0.0.4', 5, 2, 5, 10, 70.0],
                 ['10.0.0.5', 6, 2, 3, 10, 50.0],
                 ['10.0.0.5', 7, 2, 2, 10, 40.0],
                 ['10.0.0.6', 8, 2, 4, 10, 60.0],
                 ['10.0.0.7', 9, 2, 4, 10, 60.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs, loads, vdisks, mds_services)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(MDSServices)
    unittest.TextTestRunner(verbosity=2).run(suite)

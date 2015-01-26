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
Delete snapshots test module
"""
import unittest
import sys
import json
from unittest import TestCase
from ovs.lib.tests.mockups import StorageDriverModule, StorageDriverClient
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore


class MDSServices(TestCase):
    """
    This test class will validate the various scenarios of the delete snapshots logic
    """

    VDisk = None
    MDSService = None
    ServiceType = None
    MDSServiceVDisk = None
    VPool = None
    PMachine = None
    Service = None
    StorageRouter = None
    StorageDriver = None
    BackendType = None
    VolatileMutex = None
    MDSServiceController = None
    logLevel = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        # Load dummy stores
        PersistentFactory.store = DummyPersistentStore()
        VolatileFactory.store = DummyVolatileStore()
        # Replace mocked classes
        sys.modules['ovs.extensions.storageserver.storagedriver'] = StorageDriverModule
        # Import required modules/classes after mocking is done
        from ovs.dal.hybrids.vdisk import VDisk
        from ovs.dal.hybrids.service import Service
        from ovs.dal.hybrids.vpool import VPool
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.hybrids.servicetype import ServiceType
        from ovs.dal.hybrids.storagedriver import StorageDriver
        from ovs.dal.hybrids.backendtype import BackendType
        from ovs.dal.hybrids.j_mdsservice import MDSService
        from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
        from ovs.extensions.generic.volatilemutex import VolatileMutex
        from ovs.lib.mdsservice import MDSServiceController
        # Globalize mocked classes
        global VDisk
        global VPool
        global Service
        global StorageRouter
        global StorageDriver
        global BackendType
        global PMachine
        global MDSService
        global ServiceType
        global MDSServiceVDisk
        global VolatileMutex
        global MDSServiceController
        _ = VDisk(), VPool(), Service(), MDSService(), MDSServiceVDisk(), ServiceType(), \
            StorageRouter(), StorageDriver(), BackendType(), PMachine(), \
            VolatileMutex('dummy'), MDSServiceController

        # Configuration
        def _get(key):
            c = PersistentFactory.get_client()
            if c.exists(key):
                return c.get(key)
            return None

        def _get_int(key):
            return int(Configuration.get(key))

        Configuration.get = staticmethod(_get)
        Configuration.getInt = staticmethod(_get_int)

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

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        pass

    def _build_service_structure(self, structure):
        """
        Builds an MDS service structure
        """
        vpools = {}
        storagerouters = {}
        storagedrivers = {}
        services = {}
        mds_services = {}
        service_type = ServiceType()
        service_type.name = 'MetadataServer'
        service_type.save()
        for vpool_id in structure['vpools']:
            vpool = VPool()
            vpool.name = str(vpool_id)
            vpool.backend_type = BackendType()
            vpool.save()
            vpools[vpool_id] = vpool
        for sr_id in structure['storagerouters']:
            storagerouter = StorageRouter()
            storagerouter.name = str(sr_id)
            storagerouter.ip = '10.0.0.{0}'.format(sr_id)
            storagerouter.pmachine = PMachine()
            storagerouter.save()
            storagerouters[sr_id] = storagerouter
        for sd_info in structure['storagedrivers']:
            sd_id, vpool_id, sr_id = sd_info
            storagedriver = StorageDriver()
            storagedriver.vpool = vpools[vpool_id]
            storagedriver.storagerouter = storagerouters[sr_id]
            storagedriver.name = str(sd_id)
            storagedriver.mountpoint_temp = '/'
            storagedriver.mountpoint_foc = '/'
            storagedriver.mountpoint_readcache2 = '/'
            storagedriver.mountpoint_writecache = '/'
            storagedriver.mountpoint_readcache1 = '/'
            storagedriver.mountpoint_temp = '/'
            storagedriver.mountpoint_md = '/'
            storagedriver.mountpoint_bfs = '/'
            storagedriver.mountpoint = '/'
            storagedriver.cluster_ip = storagerouters[sr_id].ip
            storagedriver.storage_ip = '127.0.0.1'
            storagedriver.storagedriver_id = str(sd_id)
            storagedriver.ports = [1, 2, 3]
            storagedriver.save()
            storagedrivers[sd_id] = storagedriver
        for mds_info in structure['mds_services']:
            mds_id, sd_id = mds_info
            sd = storagedrivers[sd_id]
            s_id = '{0}-{1}'.format(sd.storagerouter.name, mds_id)
            service = Service()
            service.name = s_id
            service.storagerouter = sd.storagerouter
            service.port = mds_id
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
        return vpools, storagerouters, storagedrivers, services, mds_services

    def _create_vdisks_for_mds_service(self, amount, start_id, mds_service=None, vpool=None):
        """
        Generates vdisks and appends them to a given mds_service
        """
        vdisks = {}
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
                junction.save()
            vdisks[i] = disk
        return vdisks

    def test_load_calculation(self):
        """
        Validates whether the load calculation works
        """
        vpools, storagerouters, storagedrivers, services, mds_services = self._build_service_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <sr_id>)
             'mds_services': [(1, 1)]}  # (<id>, <sd_id>)
        )
        mds_service = mds_services[1]
        self._create_vdisks_for_mds_service(2, 1, mds_service=mds_service)
        load = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 20, 'There should be a 100% load. {0}'.format(load))
        self._create_vdisks_for_mds_service(3, 3, mds_service=mds_service)
        load = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 50, 'There should be a 100% load. {0}'.format(load))
        self._create_vdisks_for_mds_service(5, 6, mds_service=mds_service)
        load = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 100, 'There should be a 100% load. {0}'.format(load))
        mds_service.capacity = -1
        mds_service.save()
        load = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 50, 'There should be a 100% load. {0}'.format(load))

    def test_storagedriver_config_set(self):
        """
        Validates whether storagedriver configuration is generated as expected
        """
        PersistentFactory.get_client().set('ovs.storagedriver.mds.safety', '3')
        vpools, storagerouters, storagedrivers, services, mds_services = self._build_service_structure(
            {'vpools': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 2, 4), (6, 2, 5), (7, 2, 6)],  # (<id>, <vpool_id>, <sr_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5), (7, 6), (8, 7), (9, 7)]}  # (<id>, <sd_id>)
        )
        vdisks = {}
        start_id = 1
        for mds_service in mds_services.itervalues():
            vdisks.update(self._create_vdisks_for_mds_service(10, start_id, mds_service=mds_service))
            start_id += 10
        mds_services[1].capacity = 11  # on 1, vpool 1
        mds_services[1].save()
        mds_services[2].capacity = 20  # on 1, vpool 1
        mds_services[2].save()
        mds_services[3].capacity = 12  # on 2, vpool 1
        mds_services[3].save()
        mds_services[4].capacity = 14  # on 3, vpool 1
        mds_services[4].save()
        mds_services[5].capacity = 16  # on 4, vpool 1
        mds_services[5].save()
        mds_services[6].capacity = 11  # on 4, vpool 2
        mds_services[6].save()
        mds_services[7].capacity = 13  # on 5, vpool 2
        mds_services[7].save()
        mds_services[8].capacity = 19  # on 6, vpool 2
        mds_services[8].save()
        mds_services[9].capacity = 15  # on 6, vpool 2
        mds_services[9].save()
        config = MDSServiceController.get_mds_storagedriver_config_set(vpools[1])
        expected = {storagerouters[1].guid: [{'host': '10.0.0.1', 'port': 2},
                                             {'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.3', 'port': 4}],
                    storagerouters[2].guid: [{'host': '10.0.0.2', 'port': 3},
                                             {'host': '10.0.0.1', 'port': 2},
                                             {'host': '10.0.0.4', 'port': 5}],
                    storagerouters[3].guid: [{'host': '10.0.0.3', 'port': 4},
                                             {'host': '10.0.0.1', 'port': 2},
                                             {'host': '10.0.0.4', 'port': 5}],
                    storagerouters[4].guid: [{'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.1', 'port': 2},
                                             {'host': '10.0.0.3', 'port': 4}]}
        self.assertDictEqual(config, expected, 'Test 1. Got:\n{0}'.format(json.dumps(config, indent=2)))
        mds_services[2].capacity = 10  # on 1, vpool 1
        mds_services[2].save()
        config = MDSServiceController.get_mds_storagedriver_config_set(vpools[1])
        expected = {storagerouters[1].guid: [{'host': '10.0.0.1', 'port': 1},
                                             {'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.3', 'port': 4}],
                    storagerouters[2].guid: [{'host': '10.0.0.2', 'port': 3},
                                             {'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.3', 'port': 4}],
                    storagerouters[3].guid: [{'host': '10.0.0.3', 'port': 4},
                                             {'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.2', 'port': 3}],
                    storagerouters[4].guid: [{'host': '10.0.0.4', 'port': 5},
                                             {'host': '10.0.0.3', 'port': 4},
                                             {'host': '10.0.0.2', 'port': 3}]}
        self.assertDictEqual(config, expected, 'Test 2. Got:\n{0}'.format(json.dumps(config, indent=2)))

    def test_syncreality(self):
        """
        Validates whether reality is synced to the model as expected
        """
        def _generate_backend_config(_scenario, _vdisks, _mds_services):
            """
            Injects a backend config that would be returned by the storagedriver
            """
            def _generate_nc_function(address, mds_service):
                """
                Generates the lambda that will return the address or ip
                """
                if address is True:
                    return lambda s: mds_service.service.storagerouter.ip
                return lambda s: int(mds_service.service.port)

            def _generate_bc_function(_configs):
                """
                Generates the lambda that will return the config list
                """
                return lambda s: _configs

            for disk_id in _scenario:
                configs = []
                for mds_id in _scenario[disk_id]:
                    config = type('MDSNodeConfig', (), {'address': _generate_nc_function(True, _mds_services[mds_id]),
                                                        'port': _generate_nc_function(False, _mds_services[mds_id])})()
                    configs.append(config)
                mds_backend_config = type('MDSMetaDataBackendConfig', (), {'node_configs': _generate_bc_function(configs)})()
                StorageDriverClient.metadata_backend_config[_vdisks[disk_id].volume_id] = mds_backend_config

        def _validate_scenario(_scenario, _vdisks, _mds_services):
            """
            Validates a scenario with the model
            """
            for disk_id in _scenario:
                expected_mds_services = []
                for mds_id in _scenario[disk_id]:
                    expected_mds_services.append(_mds_services[mds_id])
                disk = _vdisks[disk_id]
                self.assertEqual(len(disk.mds_services), len(expected_mds_services))
                for junction in disk.mds_services:
                    self.assertIn(junction.mds_service, expected_mds_services)

        def _test_scenario(scenario, _vdisks, _mds_services):
            """
            Executes a testrun for a given scenario
            """
            _generate_backend_config(scenario, _vdisks, _mds_services)
            for vdisk_id in _vdisks:
                MDSServiceController.sync_vdisk_to_reality(_vdisks[vdisk_id])
            _validate_scenario(scenario, _vdisks, _mds_services)

        vpools, _, _, _, mds_services = self._build_service_structure(
            {'vpools': [1],
             'storagerouters': [1, 2, 3, 4],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # (<id>, <vpool_id>, <sr_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)]}  # (<id>, <sd_id>)
        )
        vdisks = self._create_vdisks_for_mds_service(5, 1, vpool=vpools[1])
        _test_scenario({1: [1, 3, 4], 2: [1, 2], 3: [1, 3, 4], 4: [3, 4, 5], 5: [1, 4, 5]},
                       vdisks, mds_services)
        _test_scenario({1: [1, 2], 2: [1, 2, 3, 4, 5], 3: [1, 2], 4: [5], 5: [1, 4, 5]},
                       vdisks, mds_services)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(MDSServices)
    unittest.TextTestRunner().run(suite)

# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
MDSService test module
"""
import json
import unittest
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.hybrids.service import Service
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import MetadataServerClient, StorageDriverConfiguration
from ovs.extensions.storageserver.tests.mockups import MDSClient, StorageRouterClient, LocalStorageRouterClient
from ovs.lib.mdsservice import MDSServiceController


class MDSServices(unittest.TestCase):
    """
    This test class will validate the various scenarios of the MDSService logic
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.volatile, self.persistent = DalHelper.setup()
        Configuration.set('/ovs/framework/logging|path', '/var/log/ovs')
        Configuration.set('/ovs/framework/logging|level', 'DEBUG')
        Configuration.set('/ovs/framework/logging|default_file', 'generic')
        Configuration.set('/ovs/framework/logging|default_name', 'logger')
        self.maxDiff = None

    def tearDown(self):
        """
        Clean up test suite
        """
        DalHelper.teardown()

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
            if _mds_service.service.storagerouter_guid is not None:
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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': []}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        mds_service = structure['mds_services'][1]
        vdisks = DalHelper.create_vdisks_for_mds_service(amount=2, start_id=1, mds_service=mds_service)
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 20, 'There should be a 20% load. {0}'.format(load))
        self.assertEqual(load_plus, 30, 'There should be a 30% plus load. {0}'.format(load_plus))
        vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=3, start_id=len(vdisks) + 1, mds_service=mds_service))
        load, load_plus = MDSServiceController.get_mds_load(mds_service)
        self.assertEqual(load, 50, 'There should be a 50% load. {0}'.format(load))
        self.assertEqual(load_plus, 60, 'There should be a 60% plus load. {0}'.format(load_plus))
        vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=5, start_id=len(vdisks) + 1, mds_service=mds_service))
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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 2, 4), (6, 2, 5), (7, 2, 6)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5), (7, 6), (8, 7), (9, 7)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True), (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False), (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        vpools = structure['vpools']
        mds_services = structure['mds_services']
        storagerouters = structure['storagerouters']
        vdisks = {}

        for vpool in vpools.itervalues():
            Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 3)

        for mds_service in mds_services.itervalues():
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=10, start_id=len(vdisks) + 1, mds_service=mds_service))
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
                vdisk = vdisks[disk_id]
                mds_backend_config = DalHelper.generate_mds_metadata_backend_config([mds_services[mds_id] for mds_id in mds_ids])
                for config in mds_backend_config.node_configs():
                    MDSClient(config).create_namespace(vdisk.volume_id)
                vdisk.storagedriver_client.update_metadata_backend_config(vdisk.volume_id, mds_backend_config)

            for vdisk_id in vdisks:
                MDSServiceController.sync_vdisk_to_reality(vdisks[vdisk_id])

            for disk_id, mds_ids in scenario.iteritems():
                expected_mds_services = [mds_services[mds_id] for mds_id in mds_ids]
                disk = vdisks[disk_id]
                self.assertEqual(len(disk.mds_services), len(expected_mds_services))
                for junction in disk.mds_services:
                    self.assertIn(junction.mds_service, expected_mds_services)

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1],
             'storagerouters': [1, 2, 3, 4],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # (<id>, <vpool_id>, <sr_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 2, 1, False), (3, 3, 1, False), (4, 4, 1, False)]}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        storagedrivers = structure['storagedrivers']
        mds_services = structure['mds_services']

        vdisks = DalHelper.create_vdisks_for_mds_service(amount=5, start_id=1, storagedriver=storagedrivers[1])
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

    def test_ensure_safety_of_3(self):
        """
        Validates whether the ensure_safety call works as expected
        Default safety used to be 3 (Versions Boston, Chicago, Denver)
        Default safety changed to 2 (Eugene, ...)
        MDSServiceController.ensure_safety will make sure that all possible master and slave MDS services are configured correctly in volumedriver
        Following rules apply:
            * If master overloaded (load > threshold), master is demoted to slave, least loaded slave is promoted to master
            * If 1 or more slaves overloaded, new slave will be created based on lowest load
            * All master and slave services will always be on different nodes
            * Master WILL ALWAYS be on the local node (where the vDisk is hosted)
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
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 1, False), (4, 2, 2, True),
                                       (5, 3, 2, False), (6, 3, 1, True), (7, 4, 2, False), (8, 4, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vpool = structure['vpools'][1]
        mds_services = structure['mds_services']
        service_type = structure['service_types']['MetadataServer']
        storagedrivers = structure['storagedrivers']
        storagerouters = structure['storagerouters']

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 3)

        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=2, start_id=len(vdisks) + 1, mds_service=mds_service))

        # Sub-Test 1: Validate the start configuration which is simple, each disk has only its default local master
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      2       |    10    |
        # |    2   |       2       |   1   |     1      |      2       |    10    |
        # |    3   |       3       |   1   |     2      |      1       |    10    |
        # |    4   |       4       |   1   |     2      |      1       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 2: Validate whether this extra (unnecessary) run doesn't change anything, preventing reconfiguring over and over again
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 3: Validating whether an overloaded node is correctly rebalanced
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      2       |    10    |
        # |    2   |       2       |   1   |     1      |      2       |    2     |
        # |    3   |       3       |   1   |     2      |      1       |    10    |
        # |    4   |       4       |   1   |     2      |      1       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Again, validating whether a subsequent run doesn't give unexpected changes
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

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
        mds_service.vpool = vpool
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # If the tlogs are not caught up, nothing should be changed
        MDSClient.set_catchup('10.0.0.2:5', vdisks[3].volume_id, 1000)
        MDSClient.set_catchup('10.0.0.2:5', vdisks[4].volume_id, 1000)
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # The next run, after tlogs are caught up, a master switch should be executed
        MDSClient.set_catchup('10.0.0.2:5', vdisks[3].volume_id, 50)
        MDSClient.set_catchup('10.0.0.2:5', vdisks[4].volume_id, 50)
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 6: Validate whether a volume migration makes the master follow
        vdisks[1].storagedriver_client.migrate(vdisks[1].volume_id, storagedrivers[3].storagedriver_id, False)
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Validates if a second run doesn't change anything
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Clean everything from here on out
        self.volatile._clean()
        self.persistent._clean()

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1, 2, 3],
             'storagerouters': [1, 2, 3, 4, 5, 6, 7],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6), (7, 1, 7)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 2), (4, 3), (5, 4), (6, 5), (7, 5), (8, 6), (9, 7)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 1, False), (4, 2, 2, True),
                                       (5, 3, 1, False), (6, 4, 1, False), (7, 4, 3, True), (8, 5, 2, False),
                                       (9, 5, 3, True), (10, 6, 3, False), (11, 7, 3, False), (12, 7, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vpool = structure['vpools'][1]
        domains = structure['domains']
        mds_services = structure['mds_services']
        storagerouters = structure['storagerouters']
        storagerouter_domains = structure['storagerouter_domains']

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 3)

        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=1, start_id=len(vdisks) + 1, mds_service=mds_service))

        # Validate the start configuration which is simple, each disk has only its default local master
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      2       |    10    |
        # |    2   |       2       |   1   |     1      |      2       |    10    |
        # |    3   |       2       |   1   |     1      |      2       |    10    |
        # |    4   |       3       |   1   |     1      |      -       |    10    |
        # |    5   |       4       |   1   |     1      |      3       |    10    |
        # |    6   |       5       |   1   |     2      |      3       |    10    |
        # |    7   |       5       |   1   |     2      |      3       |    10    |
        # |    8   |       6       |   1   |     3      |      -       |    10    |
        # |    9   |       7       |   1   |     3      |      1       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 7: Update 2 primary failure domains (Cannot be identical to secondary failure domains)
        storagerouter_domains[3].domain = domains[3]  # sr 2: primary domain 1 -> 3
        storagerouter_domains[6].domain = domains[2]  # sr 4: primary domain 1 -> 2
        storagerouter_domains[3].save()
        storagerouter_domains[6].save()
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      2       |    10    |
        # |    2   |       2       |   1   |    [3]     |      2       |    10    |
        # |    3   |       2       |   1   |    [3]     |      2       |    10    |
        # |    4   |       3       |   1   |     1      |      -       |    10    |
        # |    5   |       4       |   1   |    [2]     |      3       |    10    |
        # |    6   |       5       |   1   |     2      |      3       |    10    |
        # |    7   |       5       |   1   |     2      |      3       |    10    |
        # |    8   |       6       |   1   |     3      |      -       |    10    |
        # |    9   |       7       |   1   |     3      |      1       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 8: Update a secondary failure domain (Cannot be identical to primary failure domain)
        storagerouter_domains[9].domain = domains[1]  # sr 5: sec domain 3 -> 1
        storagerouter_domains[9].save()
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      2       |    10    |
        # |    2   |       2       |   1   |     3      |      2       |    10    |
        # |    3   |       2       |   1   |     3      |      2       |    10    |
        # |    4   |       3       |   1   |     1      |      -       |    10    |
        # |    5   |       4       |   1   |     2      |      3       |    10    |
        # |    6   |       5       |   1   |     2      |     [1]      |    10    |
        # |    7   |       5       |   1   |     2      |     [1]      |    10    |
        # |    8   |       6       |   1   |     3      |      -       |    10    |
        # |    9   |       7       |   1   |     3      |      1       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 9: Add a secondary failure domain (Cannot be identical to primary failure domain)
        srd = StorageRouterDomain()
        srd.backup = True
        srd.domain = domains[3]
        srd.storagerouter = storagerouters[3]
        srd.save()
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      2       |    10    |
        # |    2   |       2       |   1   |     3      |      2       |    10    |
        # |    3   |       2       |   1   |     3      |      2       |    10    |
        # |    4   |       3       |   1   |     1      |     [3]      |    10    |
        # |    5   |       4       |   1   |     2      |      3       |    10    |
        # |    6   |       5       |   1   |     2      |      1       |    10    |
        # |    7   |       5       |   1   |     2      |      1       |    10    |
        # |    8   |       6       |   1   |     3      |      -       |    10    |
        # |    9   |       7       |   1   |     3      |      1       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 10: Remove 2 secondary failure domains
        storagerouter_domains[2].delete()   # sr 1: sec domain 2 -> -
        storagerouter_domains[12].delete()  # sr 7: sec domain 1 -> -
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |     [-]      |    10    |
        # |    2   |       2       |   1   |     3      |      2       |    10    |
        # |    3   |       2       |   1   |     3      |      2       |    10    |
        # |    4   |       3       |   1   |     1      |      3       |    10    |
        # |    5   |       4       |   1   |     2      |      3       |    10    |
        # |    6   |       5       |   1   |     2      |      1       |    10    |
        # |    7   |       5       |   1   |     2      |      1       |    10    |
        # |    8   |       6       |   1   |     3      |      -       |    10    |
        # |    9   |       7       |   1   |     3      |     [-]      |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 11: Add some more vDisks and increase safety
        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 5)
        for mds_service in mds_services.itervalues():
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=1, start_id=len(vdisks) + 1, mds_service=mds_service))
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 12: Reduce safety
        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 3)
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY |
        # |    1   |       1       |   1   |     1      |      -       |    10    |
        # |    2   |       2       |   1   |     3      |      2       |    10    |
        # |    3   |       2       |   1   |     3      |      2       |    10    |
        # |    4   |       3       |   1   |     1      |      3       |    10    |
        # |    5   |       4       |   1   |     2      |      3       |    10    |
        # |    6   |       5       |   1   |     2      |      1       |    10    |
        # |    7   |       5       |   1   |     2      |      1       |    10    |
        # |    8   |       6       |   1   |     3      |      -       |    10    |
        # |    9   |       7       |   1   |     3      |      -       |    10    |
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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

    def test_ensure_safety_of_2(self):
        """
        Validates whether the ensure_safety call works as expected
        Default safety used to be 3 (Versions Boston, Chicago, Denver)
        Default safety changed to 2 (Eugene, ...)
        MDSServiceController.ensure_safety will make sure that all possible master and slave MDS services are configured correctly in volumedriver
        Following rules apply:
            * If master overloaded (load > threshold), master is demoted to slave, least loaded slave is promoted to master
            * If 1 or more slaves overloaded, new slave will be created based on lowest load
            * All master and slave services will always be on different nodes
            * Master WILL ALWAYS be on the local node (where the vDisk is hosted)
            * Slaves will be filled up until primary safety reached (taken from primary failure domain)
            * Slaves will be filled up until secondary safety reached if secondary failure domain known
            * Final configuration:
                * Safety of 2 and secondary failure domain known
                    * [MASTER primary failure domain and local, SLAVE in secondary failure domain] --> 1st in list in volumedriver config will be treated as master
                * Safety of 2 and secondary failure domain NOT known
                    * [MASTER primary failure domain and local, SLAVE in primary failure domain and NOT local] --> 1st in list in volumedriver config will be treated as master
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
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 1, False), (4, 2, 2, True),
                                       (5, 3, 2, False), (6, 3, 1, True), (7, 4, 2, False), (8, 4, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vpool = structure['vpools'][1]
        mds_services = structure['mds_services']
        service_type = structure['service_types']['MetadataServer']
        storagedrivers = structure['storagedrivers']
        storagerouters = structure['storagerouters']

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_maxload'.format(vpool.guid), 55)
        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=2, start_id=len(vdisks) + 1, mds_service=mds_service))

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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Validate first run. Each disk should now have sufficient nodes, since there are plenty of MDS services available
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.2', 'port': 2}]]
        loads = [['10.0.0.1', 1, 2, 2, 10, 40.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 2, 10, 40.0],
                 ['10.0.0.3', 3, 2, 2, 10, 40.0],
                 ['10.0.0.4', 4, 2, 2, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 2: Validate whether this extra (unnecessary) run doesn't change anything, preventing reconfiguring over and over again
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 3: Validating whether an overloaded node is correctly rebalanced
        # | MDS ID | STORAGEROUTER | VPOOL | PRIMARY FD | SECONDARY FD | CAPACITY | LOAD (in percent) |
        # |    1   |       1       |   1   |     1      |      2       |    10    |       20,0        |
        # |    2   |       2       |   1   |     1      |      2       |    2     |      100,0        |
        # |    3   |       3       |   1   |     2      |      1       |    10    |       20,0        |
        # |    4   |       4       |   1   |     2      |      1       |    10    |       20,0        |
        mds_services[2].capacity = 2
        mds_services[2].save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}]]
        loads = [['10.0.0.1', 1, 2, 3, 10, 50.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 2, 100.0],
                 ['10.0.0.3', 3, 2, 3, 10, 50.0],
                 ['10.0.0.4', 4, 2, 2, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 4: Validate whether the overloaded services are still handled
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.3', 'port': 3}]]
        loads = [['10.0.0.1', 1, 2, 3, 10, 50.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 2, 100.0],
                 ['10.0.0.3', 3, 2, 3, 10, 50.0],
                 ['10.0.0.4', 4, 2, 2, 10, 40.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Again, validating whether a subsequent run doesn't give unexpected changes
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 5: An MDS service will be added (next to the overloaded service), this should cause the expected to be rebalanced
        service = Service()
        service.name = '{0}-5'.format(storagerouters[2].name)
        service.storagerouter = storagerouters[2]
        service.ports = [5]
        service.type = service_type
        service.save()
        mds_service = MDSService()
        mds_service.service = service
        mds_service.number = 0
        mds_service.capacity = 10
        mds_service.vpool = vpool
        mds_service.save()
        mds_services[5] = mds_service
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.2', 'port': 5}]]
        loads = [['10.0.0.1', 1, 2, 3, 10, 50.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 2, 0, 2, 100.0],
                 ['10.0.0.3', 3, 2, 2, 10, 40.0],
                 ['10.0.0.4', 4, 2, 2, 10, 40.0],
                 ['10.0.0.2', 5, 0, 3, 10, 30.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # If the tlogs are not caught up, nothing should be changed
        MDSClient.set_catchup('10.0.0.2:5', vdisks[3].volume_id, 1000)
        MDSClient.set_catchup('10.0.0.2:5', vdisks[4].volume_id, 1000)
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # The next run, after tlogs are caught up, a master switch should be executed
        MDSClient.set_catchup('10.0.0.2:5', vdisks[3].volume_id, 50)
        MDSClient.set_catchup('10.0.0.2:5', vdisks[4].volume_id, 50)
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.2', 'port': 5}]]
        loads = [['10.0.0.1', 1, 2, 3, 10, 50.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 2, 50.0],
                 ['10.0.0.3', 3, 2, 2, 10, 40.0],
                 ['10.0.0.4', 4, 2, 2, 10, 40.0],
                 ['10.0.0.2', 5, 1, 1, 10, 20.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 6: Validate whether a volume migration makes the master follow
        vdisks[1].storagedriver_client.migrate(vdisks[1].volume_id, storagedrivers[3].storagedriver_id, False)
        configs = [[{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.2', 'port': 5}, {'ip': '10.0.0.3', 'port': 3}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.4', 'port': 4}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.3', 'port': 3}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 4}, {'ip': '10.0.0.2', 'port': 5}]]
        loads = [['10.0.0.1', 1, 1, 4, 10, 50.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 2, 50.0],
                 ['10.0.0.3', 3, 3, 1, 10, 40.0],
                 ['10.0.0.4', 4, 2, 2, 10, 40.0],
                 ['10.0.0.2', 5, 1, 1, 10, 20.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Clean everything from here on out
        self.volatile._clean()
        self.persistent._clean()

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1, 2, 3],
             'storagerouters': [1, 2, 3, 4, 5, 6, 7],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6), (7, 1, 7)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 2), (4, 3), (5, 4), (6, 5), (7, 5), (8, 6), (9, 7)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 1, False), (4, 2, 2, True),
                                       (5, 3, 1, False), (6, 4, 1, False), (7, 4, 3, True), (8, 5, 2, False),
                                       (9, 5, 3, True), (10, 6, 3, False), (11, 7, 3, False), (12, 7, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vpool = structure['vpools'][1]
        domains = structure['domains']
        mds_services = structure['mds_services']
        storagerouters = structure['storagerouters']
        storagerouter_domains = structure['storagerouter_domains']

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_maxload'.format(vpool.guid), 35)

        vdisks = {}
        for mds_service in mds_services.itervalues():
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=1, start_id=len(vdisks) + 1, mds_service=mds_service))

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
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Validate first run. Each disk should now have sufficient nodes, since there are plenty of MDS services available
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 2}]]
        loads = [['10.0.0.1', 1, 1, 1, 10, 20.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 1, 10, 20.0],
                 ['10.0.0.2', 3, 1, 0, 10, 10.0],
                 ['10.0.0.3', 4, 1, 0, 10, 10.0],
                 ['10.0.0.4', 5, 1, 0, 10, 10.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 1, 10, 20.0],
                 ['10.0.0.6', 8, 1, 2, 10, 30.0],
                 ['10.0.0.7', 9, 1, 2, 10, 30.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 7: Update 2 primary failure domains (Cannot be identical to secondary failure domains)
        storagerouter_domains[3].domain = domains[3]
        storagerouter_domains[6].domain = domains[2]
        storagerouter_domains[3].save()
        storagerouter_domains[6].save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.3', 'port': 4}]]
        loads = [['10.0.0.1', 1, 1, 1, 10, 20.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 10, 10.0],
                 ['10.0.0.2', 3, 1, 0, 10, 10.0],
                 ['10.0.0.3', 4, 1, 1, 10, 20.0],
                 ['10.0.0.4', 5, 1, 0, 10, 10.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 1, 10, 20.0],
                 ['10.0.0.6', 8, 1, 2, 10, 30.0],
                 ['10.0.0.7', 9, 1, 2, 10, 30.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 8: Update a secondary failure domain (Cannot be identical to primary failure domain)
        storagerouter_domains[9].domain = domains[1]
        storagerouter_domains[9].save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.3', 'port': 4}]]
        loads = [['10.0.0.1', 1, 1, 2, 10, 30.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 10, 10.0],
                 ['10.0.0.2', 3, 1, 0, 10, 10.0],
                 ['10.0.0.3', 4, 1, 2, 10, 30.0],
                 ['10.0.0.4', 5, 1, 0, 10, 10.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 1, 10, 20.0],
                 ['10.0.0.6', 8, 1, 1, 10, 20.0],
                 ['10.0.0.7', 9, 1, 1, 10, 20.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 9: Add a secondary failure domain (Cannot be identical to primary failure domain)
        srd = StorageRouterDomain()
        srd.backup = True
        srd.domain = domains[3]
        srd.storagerouter = storagerouters[3]
        srd.save()
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.3', 'port': 4}]]
        loads = [['10.0.0.1', 1, 1, 1, 10, 20.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 1, 10, 20.0],
                 ['10.0.0.2', 3, 1, 0, 10, 10.0],
                 ['10.0.0.3', 4, 1, 2, 10, 30.0],
                 ['10.0.0.4', 5, 1, 0, 10, 10.0],
                 ['10.0.0.5', 6, 1, 2, 10, 30.0],
                 ['10.0.0.5', 7, 1, 1, 10, 20.0],
                 ['10.0.0.6', 8, 1, 1, 10, 20.0],
                 ['10.0.0.7', 9, 1, 1, 10, 20.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Sub-Test 10: Remove 2 secondary failure domains
        storagerouter_domains[2].delete()
        storagerouter_domains[12].delete()
        configs = [[{'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.5', 'port': 7}],
                   [{'ip': '10.0.0.2', 'port': 3}, {'ip': '10.0.0.5', 'port': 6}],
                   [{'ip': '10.0.0.3', 'port': 4}, {'ip': '10.0.0.2', 'port': 2}],
                   [{'ip': '10.0.0.4', 'port': 5}, {'ip': '10.0.0.6', 'port': 8}],
                   [{'ip': '10.0.0.5', 'port': 6}, {'ip': '10.0.0.1', 'port': 1}],
                   [{'ip': '10.0.0.5', 'port': 7}, {'ip': '10.0.0.3', 'port': 4}],
                   [{'ip': '10.0.0.6', 'port': 8}, {'ip': '10.0.0.7', 'port': 9}],
                   [{'ip': '10.0.0.7', 'port': 9}, {'ip': '10.0.0.2', 'port': 3}]]
        loads = [['10.0.0.1', 1, 1, 1, 10, 20.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 1, 10, 20.0],
                 ['10.0.0.2', 3, 1, 1, 10, 20.0],
                 ['10.0.0.3', 4, 1, 1, 10, 20.0],
                 ['10.0.0.4', 5, 1, 0, 10, 10.0],
                 ['10.0.0.5', 6, 1, 1, 10, 20.0],
                 ['10.0.0.5', 7, 1, 1, 10, 20.0],
                 ['10.0.0.6', 8, 1, 1, 10, 20.0],
                 ['10.0.0.7', 9, 1, 1, 10, 20.0]]
        for vdisk_id in sorted(vdisks):
            MDSServiceController.ensure_safety(vdisks[vdisk_id])
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

    def test_role_assignments(self):
        """
        Validates whether the role assignment and ex-master behavior is correct:
        * When a slave is configured as a master, the ex-master should not be immediately recycled as a slave to prevent
          race conditions in the StorageDriver. It should be left out, and then in a next call be included again.
        * When an ex-master is recycled, it should be explicitly set to the slave role again
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1, 2, 3],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        mds_services = structure['mds_services']
        storagedrivers = structure['storagedrivers']

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 3)
        Configuration.set('/ovs/vpools/{0}/mds_config|mds_maxload'.format(vpool.guid), 10)

        vdisks = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services[1])
        vdisk = vdisks[1]

        # Sub-Test 1: Validate the start configuration which is simple, each disk has only its default local master
        # | MDS ID | STORAGEROUTER | VPOOL | CAPACITY | LOAD (in percent) |
        # |    1   |       1       |   1   |    10    |       10,0        |
        # |    2   |       2       |   1   |    10    |        0,0        |
        # |    3   |       3       |   1   |    10    |        0,0        |
        configs = [[{'ip': '10.0.0.1', 'port': 1}]]
        loads = [['10.0.0.1', 1, 1, 0, 10, 10.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 0, 0, 10,  0.0],
                 ['10.0.0.3', 3, 0, 0, 10,  0.0]]
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)

        # Validate first run. Each disk should now have sufficient nodes, since there are plenty of MDS services available
        configs = [[{'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.3', 'port': 3}]]
        loads = [['10.0.0.1', 1, 1, 0, 10, 10.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 0, 1, 10, 10.0],
                 ['10.0.0.3', 3, 0, 1, 10, 10.0]]

        StorageRouterClient.mds_recording = []
        MDSServiceController.ensure_safety(vdisk)
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)
        self.assertListEqual(StorageRouterClient.mds_recording, [['10.0.0.1:1', '10.0.0.2:2', '10.0.0.3:3'],
                                                                 '10.0.0.1:1: Master (I)',
                                                                 '10.0.0.2:2: Slave (E)',
                                                                 '10.0.0.3:3: Slave (E)'])

        vdisk.storagedriver_client.migrate(vdisk.volume_id, storagedrivers[2].storagedriver_id, False)

        configs = [[{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}, {'ip': '10.0.0.3', 'port': 3}]]
        loads = [['10.0.0.1', 1, 0, 1, 10, 10.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 10, 10.0],
                 ['10.0.0.3', 3, 0, 1, 10, 10.0]]

        StorageRouterClient.mds_recording = []
        MDSServiceController.ensure_safety(vdisk)
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)
        self.assertListEqual(StorageRouterClient.mds_recording, [['10.0.0.2:2', '10.0.0.3:3'],
                                                                 '10.0.0.2:2: Master (I)',
                                                                 ['10.0.0.2:2', '10.0.0.1:1', '10.0.0.3:3'],
                                                                 '10.0.0.2:2: Master (I)',
                                                                 '10.0.0.1:1: Slave (E)',
                                                                 '10.0.0.3:3: Slave (E)'])

        config = vdisk.info['metadata_backend_config']
        mds_client = MDSClient(None, key='{0}:{1}'.format(config[0]['ip'], config[0]['port']))
        self.assertEqual(mds_client._get_role(vdisk.volume_id), MetadataServerClient.MDS_ROLE.MASTER)
        self.assertTrue(mds_client._has_namespace(vdisk.volume_id))
        mds_client = MDSClient(None, key='{0}:{1}'.format(config[1]['ip'], config[1]['port']))
        self.assertEqual(mds_client._get_role(vdisk.volume_id), MetadataServerClient.MDS_ROLE.SLAVE)
        self.assertTrue(mds_client._has_namespace(vdisk.volume_id))
        mds_client = MDSClient(None, key='{0}:{1}'.format(config[2]['ip'], config[2]['port']))
        self.assertEqual(mds_client._get_role(vdisk.volume_id), MetadataServerClient.MDS_ROLE.SLAVE)
        self.assertTrue(mds_client._has_namespace(vdisk.volume_id))

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 2)
        MDSServiceController.ensure_safety(vdisk)

        configs = [[{'ip': '10.0.0.2', 'port': 2}, {'ip': '10.0.0.1', 'port': 1}]]
        loads = [['10.0.0.1', 1, 0, 1, 10, 10.0],  # Storage Router IP, MDS service port, #masters, #slaves, capacity, load
                 ['10.0.0.2', 2, 1, 0, 10, 10.0],
                 ['10.0.0.3', 3, 0, 0, 10,  0.0]]
        self._check_reality(configs=configs, loads=loads, vdisks=vdisks, mds_services=mds_services)
        mds_client = MDSClient(None, key='{0}:{1}'.format(config[1]['ip'], config[1]['port']))
        self.assertTrue(mds_client._has_namespace(vdisk.volume_id))
        mds_client = MDSClient(None, key='{0}:{1}'.format(config[2]['ip'], config[2]['port']))
        self.assertFalse(mds_client._has_namespace(vdisk.volume_id))

    def test_mds_checkup(self):
        """
        Validates the MDS checkup logic: Does it add services when required, does it remove services when required
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        mds_service = structure['mds_services'][1]

        Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 3)
        Configuration.set('/ovs/vpools/{0}/mds_config|mds_maxload'.format(vpool.guid), 70)

        mds_service.capacity = 10
        mds_service.save()

        MDSServiceController.mds_checkup()
        self.assertEqual(len(vpool.mds_services), 1)
        self.assertEqual(MDSServiceController.get_mds_load(mds_service), (0, 10))

        DalHelper.create_vdisks_for_mds_service(amount=8, start_id=1, mds_service=mds_service)
        MDSServiceController.mds_checkup()
        self.assertEqual(len(vpool.mds_services), 2)
        mds_service2 = [mdss for mdss in vpool.mds_services if mdss.guid != mds_service.guid][0]
        self.assertEqual(MDSServiceController.get_mds_load(mds_service), (80, 90))
        self.assertEqual(MDSServiceController.get_mds_load(mds_service2), (8, 9))

        config = StorageDriverConfiguration('storagedriver', vpool.guid, vpool.storagedrivers[0].storagedriver_id)
        contents = LocalStorageRouterClient.configurations[config.key]
        mds_nodes = contents.get('metadata_server', {}).get('mds_nodes', [])
        mds_nodes.sort(key=lambda i: i['port'])
        self.assertEqual(len(mds_nodes), 2)
        self.assertDictEqual(mds_nodes[0], {'host': '10.0.0.1',
                                            'scratch_directory': '/tmp/unittest/sr_1/disk_1/partition_1/1_db_mds_1/scratch',
                                            'port': 1,
                                            'db_directory': '/tmp/unittest/sr_1/disk_1/partition_1/1_db_mds_1/db'})
        self.assertDictEqual(mds_nodes[1], {'host': '10.0.0.1',
                                            'scratch_directory': '/tmp/unittest/sr_1/disk_1/partition_1/1_db_mds_2/scratch',
                                            'port': 10000,
                                            'db_directory': '/tmp/unittest/sr_1/disk_1/partition_1/1_db_mds_2/db'})

        mds_service.capacity = 0
        mds_service.save()
        MDSServiceController.mds_checkup()  # Migrate disks away
        MDSServiceController.mds_checkup()  # Remove obsolete MDS

        contents = LocalStorageRouterClient.configurations[config.key]
        mds_nodes = contents.get('metadata_server', {}).get('mds_nodes', [])
        self.assertEqual(len(mds_nodes), 1)
        self.assertDictEqual(mds_nodes[0], {'host': '10.0.0.1',
                                            'scratch_directory': '/tmp/unittest/sr_1/disk_1/partition_1/1_db_mds_2/scratch',
                                            'port': 10000,
                                            'db_directory': '/tmp/unittest/sr_1/disk_1/partition_1/1_db_mds_2/db'})

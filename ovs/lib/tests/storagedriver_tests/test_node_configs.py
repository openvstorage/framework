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
Test module for some generic library validations
"""
import copy
import unittest
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.tests.helpers import DalHelper
from ovs_extensions.constants.config import CONFIG_STORE_LOCATION
from ovs_extensions.constants.vpools import HOSTS_CONFIG_PATH
from ovs.extensions.db.arakooninstaller import ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.tests.mockups import StorageRouterClient
from ovs.lib.storagedriver import StorageDriverController


class NodeConfigTest(unittest.TestCase):
    """
    This test class will validate node config related code
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.persistent = DalHelper.setup(fake_sleep=True)[1]
        Configuration.set('/ovs/framework/arakoon_clusters|voldrv', 'voldrv')
        Configuration.set('/ovs/framework/rdma', False)

    def tearDown(self):
        """
        Clean up the unittest
        """
        DalHelper.teardown(fake_sleep=True)

    def test_distances(self):
        """
        Validates different node distances generated (to be passed into the StorageDriver)
        """
        # Single node cluster, no domains
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        storagedrivers = structure['storagedrivers']
        expected = {1: {}}  # No distances, since no other nodes exist
        for sd_id, sd in storagedrivers.iteritems():
            self.assertDictEqual(sd._cluster_node_config()['node_distance_map'], expected[sd_id])

        # Two nodes, no domains
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        storagedrivers = structure['storagedrivers']
        expected = {1: {'2': StorageDriver.DISTANCES.NEAR},  # No domain, so everything is near
                    2: {'1': StorageDriver.DISTANCES.NEAR}}
        for sd_id, sd in storagedrivers.iteritems():
            self.assertDictEqual(sd._cluster_node_config()['node_distance_map'], expected[sd_id])

        # Two nodes, one domain, and only one node is is in the domain
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1],
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'storagerouter_domains': [(1, 1, 1, False)]}  # (id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        storagedrivers = structure['storagedrivers']
        expected = {1: {'2': StorageDriver.DISTANCES.INFINITE},  # The other one is not in the same domain: infinity
                    2: {'1': StorageDriver.DISTANCES.NEAR}}  # No domain, so everything is near
        for sd_id, sd in storagedrivers.iteritems():
            self.assertDictEqual(sd._cluster_node_config()['node_distance_map'], expected[sd_id])

        # Two nodes, one domain, and both are in the domain
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1],
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 2, 1, False)]}  # (id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        storagedrivers = structure['storagedrivers']
        expected = {1: {'2': StorageDriver.DISTANCES.NEAR},  # Both are in the same (primary) domain: near
                    2: {'1': StorageDriver.DISTANCES.NEAR}}
        for sd_id, sd in storagedrivers.iteritems():
            self.assertDictEqual(sd._cluster_node_config()['node_distance_map'], expected[sd_id])

        # Some more complex scenarios
        # StorageRouter | Primary | Secondary
        #    1          |    1    |     2
        #    2          |    1    |     3
        #    3          |    2    |     3
        #    4          |    2    |     1
        #    5          |    3    |
        #    6          |    3    |    1,2
        #    7          |         |     1
        #    8          |         |
        #    9          |    4    |
        #   10          |    1    |     5
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1, 2, 3, 4, 5],
             'storagerouters': range(1, 11),
             'storagedrivers': [(i, 1, i) for i in range(1, 11)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'storagerouter_domains': [(1,  1,  1, False), (2,  1,  2, True),
                                       (3,  2,  1, False), (4,  2,  3, True),
                                       (5,  3,  2, False), (6,  3,  3, True),
                                       (7,  4,  2, False), (8,  4,  1, True),
                                       (9,  5,  3, False),
                                       (10, 6,  3, False), (11, 6,  1, True), (12, 6, 2, True),
                                       (13, 7,  1, True),
                                       (14, 9,  4, False),
                                       (15, 10, 1, False), (16, 10, 5, True)]}  # (id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        storagedrivers = structure['storagedrivers']
        expected = {1:  {'2':  StorageDriver.DISTANCES.NEAR,
                         '3':  StorageDriver.DISTANCES.FAR,
                         '4':  StorageDriver.DISTANCES.FAR,
                         '5':  StorageDriver.DISTANCES.INFINITE,
                         '6':  StorageDriver.DISTANCES.INFINITE,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.NEAR},
                    2:  {'1':  StorageDriver.DISTANCES.NEAR,
                         '3':  StorageDriver.DISTANCES.INFINITE,
                         '4':  StorageDriver.DISTANCES.INFINITE,
                         '5':  StorageDriver.DISTANCES.FAR,
                         '6':  StorageDriver.DISTANCES.FAR,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.NEAR},
                    3:  {'1':  StorageDriver.DISTANCES.INFINITE,
                         '2':  StorageDriver.DISTANCES.INFINITE,
                         '4':  StorageDriver.DISTANCES.NEAR,
                         '5':  StorageDriver.DISTANCES.FAR,
                         '6':  StorageDriver.DISTANCES.FAR,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.INFINITE},
                    4:  {'1':  StorageDriver.DISTANCES.FAR,
                         '2':  StorageDriver.DISTANCES.FAR,
                         '3':  StorageDriver.DISTANCES.NEAR,
                         '5':  StorageDriver.DISTANCES.INFINITE,
                         '6':  StorageDriver.DISTANCES.INFINITE,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.FAR},
                    5:  {'1':  StorageDriver.DISTANCES.INFINITE,
                         '2':  StorageDriver.DISTANCES.INFINITE,
                         '3':  StorageDriver.DISTANCES.INFINITE,
                         '4':  StorageDriver.DISTANCES.INFINITE,
                         '6':  StorageDriver.DISTANCES.NEAR,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.INFINITE},
                    6:  {'1':  StorageDriver.DISTANCES.FAR,
                         '2':  StorageDriver.DISTANCES.FAR,
                         '3':  StorageDriver.DISTANCES.FAR,
                         '4':  StorageDriver.DISTANCES.FAR,
                         '5':  StorageDriver.DISTANCES.NEAR,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.FAR},
                    7:  {'1':  StorageDriver.DISTANCES.NEAR,
                         '2':  StorageDriver.DISTANCES.NEAR,
                         '3':  StorageDriver.DISTANCES.NEAR,
                         '4':  StorageDriver.DISTANCES.NEAR,
                         '5':  StorageDriver.DISTANCES.NEAR,
                         '6':  StorageDriver.DISTANCES.NEAR,
                         '8':  StorageDriver.DISTANCES.NEAR,
                         '9':  StorageDriver.DISTANCES.NEAR,
                         '10': StorageDriver.DISTANCES.NEAR},
                    8:  {'1':  StorageDriver.DISTANCES.NEAR,
                         '2':  StorageDriver.DISTANCES.NEAR,
                         '3':  StorageDriver.DISTANCES.NEAR,
                         '4':  StorageDriver.DISTANCES.NEAR,
                         '5':  StorageDriver.DISTANCES.NEAR,
                         '6':  StorageDriver.DISTANCES.NEAR,
                         '7':  StorageDriver.DISTANCES.NEAR,
                         '9':  StorageDriver.DISTANCES.NEAR,
                         '10': StorageDriver.DISTANCES.NEAR},
                    9:  {'1':  StorageDriver.DISTANCES.INFINITE,
                         '2':  StorageDriver.DISTANCES.INFINITE,
                         '3':  StorageDriver.DISTANCES.INFINITE,
                         '4':  StorageDriver.DISTANCES.INFINITE,
                         '5':  StorageDriver.DISTANCES.INFINITE,
                         '6':  StorageDriver.DISTANCES.INFINITE,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '10': StorageDriver.DISTANCES.INFINITE},
                    10: {'1':  StorageDriver.DISTANCES.NEAR,
                         '2':  StorageDriver.DISTANCES.NEAR,
                         '3':  StorageDriver.DISTANCES.INFINITE,
                         '4':  StorageDriver.DISTANCES.INFINITE,
                         '5':  StorageDriver.DISTANCES.INFINITE,
                         '6':  StorageDriver.DISTANCES.INFINITE,
                         '7':  StorageDriver.DISTANCES.INFINITE,
                         '8':  StorageDriver.DISTANCES.INFINITE,
                         '9':  StorageDriver.DISTANCES.INFINITE}}
        for sd_id, sd in storagedrivers.iteritems():
            try:
                self.assertDictEqual(sd._cluster_node_config()['node_distance_map'], expected[sd_id])
            except:
                print 'Error processing: {0}'.format(sd_id)
                raise

    def test_node_config_checkup(self):
        """
        Validates correct working of cluster registry checkup
        """
        base_structure = {'1': {'vrouter_id': '1',
                                'message_host': '10.0.1.1',
                                'message_port': 1,
                                'xmlrpc_host': '10.0.0.1',
                                'xmlrpc_port': 2,
                                'failovercache_host': '10.0.1.1',
                                'failovercache_port': 3,
                                'network_server_uri': 'tcp://10.0.1.1:4',
                                'node_distance_map': None},
                          '2': {'vrouter_id': '2',
                                'message_host': '10.0.1.2',
                                'message_port': 1,
                                'xmlrpc_host': '10.0.0.2',
                                'xmlrpc_port': 2,
                                'failovercache_host': '10.0.1.2',
                                'failovercache_port': 3,
                                'network_server_uri': 'tcp://10.0.1.2:4',
                                'node_distance_map': None}}

        def _validate_node_config(_config, _expected_map):
            expected = copy.deepcopy(base_structure[_config.vrouter_id])
            expected['node_distance_map'] = _expected_map[_config.vrouter_id]
            self.assertDictEqual(expected, {'vrouter_id': _config.vrouter_id,
                                            'message_host': _config.message_host,
                                            'message_port': _config.message_port,
                                            'xmlrpc_host': _config.xmlrpc_host,
                                            'xmlrpc_port': _config.xmlrpc_port,
                                            'failovercache_host': _config.failovercache_host,
                                            'failovercache_port': _config.failovercache_port,
                                            'network_server_uri': _config.network_server_uri,
                                            'node_distance_map': _config.node_distance_map})

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'domains': [1, 2],
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 2, 1, False)]}  # (id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        storagerouters = structure['storagerouters']
        vpool = structure['vpools'][1]
        arakoon_installer = ArakoonInstaller(cluster_name='voldrv')
        arakoon_installer.create_cluster(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.SD,
                                         ip=storagerouters[1].ip,
                                         base_dir='/tmp')

        # Initial run, it will now be configured
        StorageRouterClient.node_config_recordings = []
        result = StorageDriverController.cluster_registry_checkup()
        self.assertDictEqual(result, {vpool.guid: {'success': True,
                                                   'changes': True}})
        self.assertListEqual(sorted(StorageRouterClient.node_config_recordings), ['1', '2'])
        expected_map = {'1': {'2': StorageDriver.DISTANCES.NEAR},
                        '2': {'1': StorageDriver.DISTANCES.NEAR}}
        configs = vpool.clusterregistry_client.get_node_configs()
        for config in configs:
            _validate_node_config(config, expected_map)

        # Running it again should not change anything
        StorageRouterClient.node_config_recordings = []
        result = StorageDriverController.cluster_registry_checkup()
        self.assertDictEqual(result, {vpool.guid: {'success': True,
                                                   'changes': False}})
        self.assertListEqual(sorted(StorageRouterClient.node_config_recordings), [])
        expected_map = {'1': {'2': StorageDriver.DISTANCES.NEAR},
                        '2': {'1': StorageDriver.DISTANCES.NEAR}}
        configs = vpool.clusterregistry_client.get_node_configs()
        for config in configs:
            _validate_node_config(config, expected_map)

        # Validate some error paths
        domain = structure['domains'][2]
        junction = structure['storagerouters'][1].domains[0]
        junction.domain = domain
        junction.save()
        vpool_config_path = Configuration.get_configuration_path(HOSTS_CONFIG_PATH.format(vpool.guid, 1))
        StorageRouterClient.exceptions['server_revision'] = {vpool_config_path: Exception('ClusterNotReachableException')}
        StorageRouterClient.node_config_recordings = []
        result = StorageDriverController.cluster_registry_checkup()
        self.assertDictEqual(result, {vpool.guid: {'success': True,
                                                   'changes': True}})
        self.assertListEqual(sorted(StorageRouterClient.node_config_recordings), ['2'])
        expected_map = {'1': {'2': StorageDriver.DISTANCES.INFINITE},
                        '2': {'1': StorageDriver.DISTANCES.INFINITE}}
        configs = vpool.clusterregistry_client.get_node_configs()
        for config in configs:
            _validate_node_config(config, expected_map)

    def test_configuration(self):
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        storagedrivers = structure['storagedrivers']
        from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
        storagedriver_configuration = StorageDriverConfiguration(vpool.guid, storagedrivers[1].storagedriver_id)
        config = storagedriver_configuration.configuration

        # Test basic functionality of the save and std objects
        self.assertEquals(config.dls_config.dls_type, 'Arakoon')
        self.assertEquals(config.filedriver_config.fd_extent_cache_capacity, 1024)
        config.dls_config.dls_type = 'Arakoon2'
        config.filedriver_config.fd_extent_cache_capacity = 1025
        self.assertNotEqual(config.dls_config.dls_type, 'Arakoon')
        storagedriver_configuration.save()
        self.assertEquals(config.dls_config.dls_type, 'Arakoon2')
        self.assertEquals(config.filedriver_config.fd_extent_cache_capacity, 1025)

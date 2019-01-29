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
Test module for vDisk set and get configuration params functionality
DTL allocation rules:
    - First priority to StorageRouters located in the vDisk's StorageRouter's Recovery Domain
    - Second priority to StorageRouters located in the vDisk's StorageRouter's Regular Domain
    - If Domains configured, but no StorageRouters are found matching any of the Domains on the vDisk's StorageRouter, a random SR in the same vPool is chosen
    - If no Domains configured on the vDisk StorageRouter, any other StorageRouter on which the vPool has been extended is chosen
"""
import copy
import logging
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.tests.mockups import DTLConfig
from ovs.lib.vdisk import VDiskController
from ovs_extensions.testing.testcase import LogTestCase


class VDiskTest(LogTestCase):
    """
    This test class will validate various vDisk functionality
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        super(VDiskTest, self).setUp()

        DalHelper.setup(fake_sleep=True)

    def tearDown(self):
        """
        Clean up the unittest
        """
        super(VDiskTest, self).tearDown()

        DalHelper.teardown(fake_sleep=True)

    def test_set_and_get_config_params(self):
        """
        Test the set_config_params functionality by validation through the get_config_params functionality
            - Verify default configuration for newly created vDisk
            - Attempt to set disallowed values
            - Attempt to sync and async mode without specifying DTL target
            - Set SCO size
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        storagedrivers = structure['storagedrivers']

        # Create vDisk and validate default configuration
        vdisk_1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        configuration = VDiskController.get_config_params(vdisk_guid=vdisk_1.guid)
        expected_keys = {'sco_size', 'dtl_mode', 'write_buffer', 'dtl_target', 'pagecache_ratio', 'cache_quota'}
        self.assertEqual(first=expected_keys,
                         second=set(configuration.keys()),
                         msg='Keys returned by get_config_params do not match the expected keys')
        tlog_multiplier = vdisk_1.storagedriver_client.get_tlog_multiplier(vdisk_1.volume_id)
        default_sco_size = vdisk_1.storagedriver_client.get_sco_multiplier(vdisk_1.volume_id) / 1024 * 4
        non_disposable_sco_factor = vdisk_1.storagedriver_client.get_sco_cache_max_non_disposable_factor(vdisk_1.volume_id)
        default_values = {'sco_size': default_sco_size,
                          'dtl_mode': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC,
                          'dtl_target': [],
                          'cache_quota': {'fragment': None,
                                          'block': None},
                          'write_buffer': int(tlog_multiplier * default_sco_size * non_disposable_sco_factor),
                          'pagecache_ratio': 1.0}
        for key, value in default_values.iteritems():
            self.assertEqual(first=configuration[key],
                             second=value,
                             msg='Value for "{0}" does not match expected default value: {1} vs {2}'.format(key, configuration[key], value))

        # Attempt to set incorrect values
        new_config_params = {'dtl_mode': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC,
                             'sco_size': 4,
                             'dtl_target': [],
                             'cache_quota': {'fragment': 5 * 1024 ** 3,
                                             'block': 1024 ** 3},
                             'write_buffer': 128}
        for key, values in {'dtl_mode': ['unknown', StorageDriverClient.VOLDRV_DTL_ASYNC],
                            'sco_size': list(set(range(257)).difference({4, 8, 16, 32, 64, 128})) + [-1],
                            'dtl_target': [{}, (), 0],
                            'cache_quota': [{'fragment': -1}, {'fragment': 1 * 1024.0 ** 3 - 1}, {'fragment': 1024 ** 4 + 1},
                                            {'block': -1}, {'block': 0.1 * 1024.0 ** 3 - 1}, {'block': 1024.0 ** 4 + 1}],
                            'write_buffer': [-1] + range(128) + range(10241, 10300),
                            'pagecache_ratio': [-0.1, 0, 1.1]}.iteritems():
            for value in values:
                config_params = copy.deepcopy(new_config_params)
                config_params[key] = value
                with self.assertRaises(RuntimeError):
                    VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=config_params)

        # Set SCO size
        set_config = copy.deepcopy(new_config_params)
        set_config['sco_size'] = 32
        VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=set_config)
        get_config = VDiskController.get_config_params(vdisk_guid=vdisk_1.guid)
        for key in set_config.iterkeys():
            self.assertEqual(first=set_config[key],
                             second=get_config[key],
                             msg='Actual value for key "{0}" differs from expected. Expected: {1}  -  Actual: {2}'.format(key, set_config[key], get_config[key]))

        # Verify cache_quota
        vdisk_1.discard()
        self.assertEqual(first={'fragment': 5 * 1024.0 ** 3,
                                'block': 1024.0 ** 3},
                         second=vdisk_1.cache_quota)

        # Restore default cache_quota again
        new_config_params['cache_quota'] = None
        VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=new_config_params)
        vdisk_1.discard()
        self.assertIsNone(vdisk_1.cache_quota)

        # Set pagecache_ratio
        capacity = vdisk_1.storagedriver_client.get_metadata_cache_capacity(str(vdisk_1.volume_id))
        self.assertEqual(capacity, 8192)  # 1GiB volume has by default 8192 pages cached
        set_config = copy.deepcopy(new_config_params)
        set_config['pagecache_ratio'] = 0.5
        VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=set_config)
        get_config = VDiskController.get_config_params(vdisk_guid=vdisk_1.guid)
        self.assertEqual(get_config['pagecache_ratio'], 0.5)
        capacity = vdisk_1.storagedriver_client.get_metadata_cache_capacity(str(vdisk_1.volume_id))
        self.assertEqual(capacity, 4096)

        # Small volumes and pagecache_ratio
        vdisk_small = VDisk(VDiskController.create_new(volume_name='vdisk_small', volume_size=1024 * 8, storagedriver_guid=storagedrivers[1].guid))
        capacity = vdisk_small.storagedriver_client.get_metadata_cache_capacity(str(vdisk_small.volume_id))
        self.assertEqual(capacity, 1)

    def test_dtl_modifications_single_node_cluster_without_domains(self):
        """
        Various validations related to DTL target changes on a single node cluster without Domains
            * vPool DTL: True
                * Set DTL for vDisk
                * Disable DTL for vDisk
                * Switch DTL mode for vDisk
            * vPool DTL: False
                * Set DTL for vDisk
                * Disable DTL for vDisk
        """
        structure = DalHelper.build_dal_structure(structure={'vpools': [1],
                                                             'storagerouters': [1],
                                                             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
                                                             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>),
                                                             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1)]})  # (<id>, <storagedriver_id>, <vpool_id>, <mds_id>)
        # Set DTL with DTL enabled for vPool (default)
        vdisks = structure['vdisks']
        vdisk = vdisks[1]
        with self.assertRaises(Exception), self.assertLogs(level=logging.DEBUG) as logging_watcher:
            VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        logger_logs = logging_watcher.get_message_severity_map()
        logs = [log for log in logger_logs if 'No possible StorageRouters' in log and vdisk.name in log]
        vdisk.discard()
        self.assertEqual(first=1, second=len(logs))
        self.assertEqual(first='ERROR', second=logger_logs[logs[0]])
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_standalone', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Disable DTL with DTL enabled for vPool
        vdisk = vdisks[2]
        dtl_config = DTLConfig(host='10.0.0.1', mode='Synchronous', port=10000)
        vdisk.storagedriver_client.set_manual_dtl_config(volume_id=vdisk.volume_id, config=dtl_config)  # Set config otherwise set_config_params won't do anything since current config is already 'no_sync'
        vdisk.invalidate_dynamics('info')
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Set DTL with DTL disabled for vPool
        vpool_1 = structure['vpools'][1]
        DalHelper.set_vpool_storage_driver_configuration(vpool=vpool_1, config={'filesystem': {'fs_dtl_host': '',
                                                                                               'fs_dtl_config_mode': StorageDriverClient.VOLDRV_DTL_MANUAL_MODE}})
        vpool_1.invalidate_dynamics('configuration')
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[3]
        with self.assertRaises(Exception), self.assertLogs(level=logging.DEBUG) as logging_watcher:
            VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        logger_logs = logging_watcher.get_message_severity_map()
        logs = [log for log in logger_logs if 'No possible StorageRouters' in log and vdisk.name in log]
        vdisk.discard()
        self.assertEqual(first=1, second=len(logs))
        self.assertEqual(first='ERROR', second=logger_logs[logs[0]])
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Disable DTL with DTL disabled for vPool
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[4]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

    def test_dtl_modifications_single_node_cluster_with_domains(self):
        """
        Various validations related to DTL target changes on a single node cluster with Domains
            * vPool DTL: True
                * Set DTL for vDisk
                * Disable DTL for vDisk
            * vPool DTL: False
                * Set DTL for vDisk
                * Disable DTL for vDisk
                * Set DTL for vDisk on vPool which only runs on 1 node
        """
        structure = DalHelper.build_dal_structure(structure={'vpools': [1],
                                                             'storagerouters': [1],
                                                             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
                                                             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>),
                                                             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_id>)
                                                             'domains': [1, 2],
                                                             'storagerouter_domains': [(1, 1, 1, True), (2, 1, 2, False)]})  # (<srd_id>, <sr_id>, <domain_id>, <backup>)
        vdisks = structure['vdisks']
        vpool_1 = structure['vpools'][1]
        domain_1 = structure['domains'][1]
        domain_2 = structure['domains'][2]

        # Set DTL with DTL enabled for vPool (default)
        vdisk = vdisks[1]
        with self.assertRaises(Exception), self.assertLogs(level=logging.DEBUG) as logging_watcher:
            VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync', 'dtl_target': [domain_1.guid]})
        logger_logs = logging_watcher.get_message_severity_map()
        logs = [log for log in logger_logs if 'No possible StorageRouters' in log and vdisk.name in log]
        vdisk.discard()
        self.assertEqual(first=1, second=len(logs))
        self.assertEqual(first='ERROR', second=logger_logs[logs[0]])
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_standalone', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Disable DTL with specifying DTL targets
        vdisk = vdisks[2]
        with self.assertRaises(ValueError):
            VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync', 'dtl_target': [domain_1.guid]})

        # Disable DTL with DTL enabled for vPool
        dtl_config = DTLConfig(host='10.0.0.1', mode='Synchronous', port=10000)
        vdisk.storagedriver_client.set_manual_dtl_config(volume_id=vdisk.volume_id, config=dtl_config)  # Set config otherwise set_config_params won't do anything since current config is already 'no_sync'
        vdisk.invalidate_dynamics('info')
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Set DTL with DTL disabled for vPool
        DalHelper.set_vpool_storage_driver_configuration(vpool=vpool_1, config={'filesystem': {'fs_dtl_host': '',
                                                                                               'fs_dtl_config_mode': StorageDriverClient.VOLDRV_DTL_MANUAL_MODE}})
        vpool_1.invalidate_dynamics('configuration')
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[3]
        with self.assertRaises(Exception), self.assertLogs(level=logging.DEBUG) as logging_watcher:
            VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync', 'dtl_target': [domain_1.guid, domain_2.guid]})
        logger_logs = logging_watcher.get_message_severity_map()
        logs = [log for log in logger_logs if 'No possible StorageRouters' in log and vdisk.name in log]
        vdisk.discard()
        self.assertEqual(first=1, second=len(logs))
        self.assertEqual(first='ERROR', second=logger_logs[logs[0]])
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Disable DTL with DTL disabled for vPool
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[4]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

    def test_dtl_modifications_multi_node_cluster_without_domains(self):
        """
        Various validations related to DTL target changes on a multi node cluster without Domains
            * vPool DTL: True
                * Set DTL for vDisk
                * Disable DTL for vDisk
            * vPool DTL: False
                * Set DTL for vDisk
                * Disable DTL for vDisk
        """
        structure = DalHelper.build_dal_structure(structure={'vpools': [1, 2],
                                                             'storagerouters': [1, 2, 3],
                                                             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
                                                             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>),
                                                             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 4, 2, 1)]})  # (<id>, <storagedriver_id>, <vpool_id>, <mds_id>)
        # Set DTL with DTL enabled for vPool (default)
        vdisks = structure['vdisks']
        vdisk = vdisks[1]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        possible_sds = [sd for sd in structure['storagedrivers'].values() if sd.storagerouter_guid != vdisk.storagerouter_guid]
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Disable DTL with DTL enabled for vPool
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Switch DTL mode (default is a_sync)
        vdisk = vdisks[2]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'sync'})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Set DTL with DTL disabled for vPool
        vpool_1 = structure['vpools'][1]
        DalHelper.set_vpool_storage_driver_configuration(vpool=vpool_1, config={'filesystem': {'fs_dtl_host': '',
                                                                                               'fs_dtl_config_mode': StorageDriverClient.VOLDRV_DTL_MANUAL_MODE}})
        vpool_1.invalidate_dynamics('configuration')
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[3]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        possible_sds = [sd for sd in structure['storagedrivers'].values() if sd.storagerouter_guid != vdisk.storagerouter_guid]
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Disable DTL with DTL disabled for vPool
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[4]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Set DTL for vDisk on vPool only on 1 node
        vdisk = vdisks[5]
        with self.assertRaises(Exception), self.assertLogs(level=logging.DEBUG) as logging_watcher:
            VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        vdisk.discard()
        logger_logs = logging_watcher.get_message_severity_map()
        logs = [log for log in logger_logs if 'No possible StorageRouters' in log and vdisk.name in log]
        self.assertEqual(first=1, second=len(logs))
        self.assertEqual(first='ERROR', second=logger_logs[logs[0]])
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_standalone', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

    def test_dtl_modifications_multi_node_cluster_with_domains(self):
        """
        Various validations related to DTL target changes on a multi node cluster with Domains
            * vPool DTL: True
                * Set DTL for vDisk to Domains 1, 2
                * Disable DTL for vDisk
                * Set DTL for vDisk to Domain 1
                * Set DTL for vDisk to unspecified target
            * vPool DTL: False
                * Set DTL for vDisk
                * Disable DTL for vDisk
        """
        structure = DalHelper.build_dal_structure(structure={'vpools': [1],
                                                             'storagerouters': [1, 2, 3, 4, 5, 6],
                                                             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)],  # (<id>, <vpool_id>, <storagerouter_id>)
                                                             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],  # (<id>, <storagedriver_id>),
                                                             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                                                                        (6, 1, 1, 1), (7, 1, 1, 1), (8, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_id>)
                                                             'domains': [1, 2],
                                                             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True),
                                                                                       (3, 2, 1, False), (4, 2, 2, True),
                                                                                       (5, 3, 2, False),
                                                                                       (6, 4, 2, False), (7, 4, 1, True),
                                                                                       (8, 5, 2, True),
                                                                                       (9, 6, 2, False)]})  # (<srd_id>, <sr_id>, <domain_id>, <backup>)
        # SR | reg dom | rec dom |
        #  1 |  dom1   |  dom2   |
        #  2 |  dom1   |  dom2   |
        #  3 |  dom2   |         |
        #  4 |  dom2   |  dom1   |
        #  5 |         |  dom2   |
        #  6 |  dom2   |         |

        domain_1 = structure['domains'][1]
        domain_2 = structure['domains'][2]
        storagerouters = structure['storagerouters']

        # Set DTL with DTL enabled for vPool (default)
        vdisks = structure['vdisks']
        vdisk = vdisks[1]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync', 'dtl_target': [domain_1.guid, domain_2.guid]})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        possible_sds = [storagerouters[3].storagedrivers[0], storagerouters[4].storagedrivers[0]]
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=2, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Disable DTL with DTL enabled for vPool
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Set DTL to StorageRouter in regular Domain
        vdisk = vdisks[2]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync', 'dtl_target': [domain_1.guid]})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=1, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertEqual(first=dtl_config.host, second=storagerouters[2].storagedrivers[0].storage_ip)
        self.assertEqual(first=dtl_config.port, second=storagerouters[2].storagedrivers[0].ports['dtl'])

        # Set DTL to any target
        vdisk = vdisks[3]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Set DTL with DTL disabled for vPool
        vpool_1 = structure['vpools'][1]
        DalHelper.set_vpool_storage_driver_configuration(vpool=vpool_1, config={'filesystem': {'fs_dtl_host': '',
                                                                                               'fs_dtl_config_mode': StorageDriverClient.VOLDRV_DTL_MANUAL_MODE}})
        vpool_1.invalidate_dynamics('configuration')
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[4]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync', 'dtl_target': [domain_1.guid, domain_2.guid]})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        possible_sds = [storagerouters[3].storagedrivers[0], storagerouters[4].storagedrivers[0]]
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=2, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Disable DTL with DTL disabled for vPool
        self.assertFalse(expr=vpool_1.configuration['dtl_enabled'])

        vdisk = vdisks[5]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'no_sync'})
        vdisk.discard()
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='disabled', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertIsNone(obj=vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id))

        # Set DTL to StorageRouter in regular Domain on DTL disabled vPool
        vdisk = vdisks[6]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync', 'dtl_target': [domain_1.guid]})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=1, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertEqual(first=dtl_config.host, second=storagerouters[2].storagedrivers[0].storage_ip)
        self.assertEqual(first=dtl_config.port, second=storagerouters[2].storagedrivers[0].ports['dtl'])

        # Set DTL to any target on DTL disabled vPool
        vdisk = vdisks[7]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        self.assertTrue(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertIn(member=dtl_config.host, container=[sd.storage_ip for sd in possible_sds])
        self.assertIn(member=dtl_config.port, container=[sd.ports['dtl'] for sd in possible_sds])

        # Set DTL to any target in regular Domain, then add recovery Domain --> checkup_required
        DalHelper.set_vpool_storage_driver_configuration(vpool=vpool_1, config={'filesystem': {'fs_dtl_host': '',
                                                                                               'fs_dtl_config_mode': StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE}})
        vpool_1.invalidate_dynamics('configuration')
        self.assertTrue(expr=vpool_1.configuration['dtl_enabled'])

        for domain in storagerouters[1].domains:
            if domain.backup is True:
                domain.delete()  # Remove recovery Domain for StorageRouter hosting the vDisk

        vdisk = vdisks[8]
        VDiskController.set_config_params(vdisk.guid, new_config_params={'dtl_mode': 'a_sync'})
        vdisk.discard()
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id=vdisk.volume_id)
        self.assertFalse(expr=vdisk.has_manual_dtl)
        self.assertEqual(first='ok_sync', second=vdisk.dtl_status)
        self.assertEqual(first=0, second=len(vdisk.domains_dtl_guids))
        self.assertEqual(first=dtl_config.mode, second=StorageDriverClient.VDISK_DTL_MODE_MAP['a_sync'])
        self.assertEqual(first=dtl_config.host, second=storagerouters[2].storagedrivers[0].storage_ip)
        self.assertEqual(first=dtl_config.port, second=storagerouters[2].storagedrivers[0].ports['dtl'])

        DalHelper.build_dal_structure(structure={'storagerouter_domains': [(10, 1, 2, True)]},  # Restore recovery Domain for StorageRouter hosting the vDisk
                                      previous_structure=structure)

        vdisk.invalidate_dynamics('dtl_status')
        self.assertEqual(first='checkup_required', second=vdisk.dtl_status)

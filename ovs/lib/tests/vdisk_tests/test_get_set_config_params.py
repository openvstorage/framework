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
"""
import copy
import unittest
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.tests.mockups import StorageRouterClient
from ovs.lib.vdisk import VDiskController


class VDiskTest(unittest.TestCase):
    """
    This test class will validate various vDisk functionality
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        DalHelper.setup(fake_sleep=True)
        Configuration.set('/ovs/framework/storagedriver|mds_tlogs', 100)
        Configuration.set('/ovs/framework/storagedriver|mds_maxload', 75)
        Configuration.set('/ovs/framework/storagedriver|mds_safety', 2)

    def tearDown(self):
        """
        Clean up the unittest
        """
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
                          'cache_quota': None,
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
                             'cache_quota': 5 * 1024.0 ** 3,
                             'write_buffer': 128}
        for key, values in {'dtl_mode': ['unknown', StorageDriverClient.VOLDRV_DTL_ASYNC],
                            'sco_size': list(set(range(257)).difference({4, 8, 16, 32, 64, 128})) + [-1],
                            'dtl_target': ['', {}, (), 0],
                            'cache_quota': [-1, 0.1 * 1024.0 ** 3 - 1, 1024.0 ** 4 + 1],
                            'write_buffer': [-1] + range(128) + range(10241, 10300),
                            'pagecache_ratio': [-0.1, 0, 1.1]}.iteritems():
            for value in values:
                config_params = copy.deepcopy(new_config_params)
                config_params[key] = value
                with self.assertRaises(RuntimeError):
                    VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=config_params)

        # Attempt to set DTL mode sync or async without specifying a target
        for dtl_mode in [StorageDriverClient.FRAMEWORK_DTL_SYNC, StorageDriverClient.FRAMEWORK_DTL_ASYNC]:
            config_params = copy.deepcopy(new_config_params)
            config_params['dtl_mode'] = dtl_mode
            with self.assertRaises(ValueError):
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
        self.assertEqual(first=5 * 1024.0 ** 3,
                         second=vdisk_1.cache_quota)

        # Restore default cache_quota again
        new_config_params['cache_quota'] = None
        VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=new_config_params)
        vdisk_1.discard()
        self.assertIsNone(vdisk_1.cache_quota)

        # Set pagecache_ratio
        capacity = vdisk_1.storagedriver_client.get_metadata_cache_capacity(str(vdisk_1.volume_id))
        self.assertEqual(capacity, 4096)  # 1GiB volume has by default 4096 pages cached
        set_config = copy.deepcopy(new_config_params)
        set_config['pagecache_ratio'] = 0.5
        VDiskController.set_config_params(vdisk_guid=vdisk_1.guid, new_config_params=set_config)
        get_config = VDiskController.get_config_params(vdisk_guid=vdisk_1.guid)
        self.assertEqual(get_config['pagecache_ratio'], 0.5)
        capacity = vdisk_1.storagedriver_client.get_metadata_cache_capacity(str(vdisk_1.volume_id))
        self.assertEqual(capacity, 2048)

        # Small volumes and pagecache_ratio
        vdisk_small = VDisk(VDiskController.create_new(volume_name='vdisk_small', volume_size=1024 * 8, storagedriver_guid=storagedrivers[1].guid))
        capacity = vdisk_small.storagedriver_client.get_metadata_cache_capacity(str(vdisk_small.volume_id))
        self.assertEqual(capacity, 1)

    def test_dtl_target(self):
        """
        Validates whether the DTL target is set to sane values, despite incorrect values being passed in
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'storagerouters': [1, 2, 3],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 3)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3)],  # (<id>, <storagedriver_id>),
             'domains': [1, 2],
             'storagerouter_domains': [(1, 1, 1, False), (2, 2, 1, False), (3, 3, 2, False)]}  # (<srd_id>, <sr_id>, <domain_id>, <backup>)
        )
        domains = structure['domains']
        storagerouters = sorted(structure['storagerouters'].values(), key=lambda i: i.guid)
        storagedrivers = {}
        for sr in storagerouters:
            storagedrivers[sr] = sorted(sr.storagedrivers, key=lambda i: i.guid)
        base_params = {'sco_size': 4,
                       'write_buffer': 128,
                       'dtl_mode': 'sync'}  # Irrelevant for this test

        vdisk = VDisk(VDiskController.create_new(volume_name='vdisk_1',
                                                 volume_size=1024 ** 3,
                                                 storagedriver_guid=storagedrivers[structure['storagerouters'][1]][0].guid))
        params = copy.deepcopy(base_params)
        params['dtl_target'] = [domains[1].guid]
        VDiskController.set_config_params(vdisk.guid, new_config_params=params)
        config = StorageRouterClient._dtl_config_cache[vdisk.vpool_guid][vdisk.volume_id]
        self.assertEqual(config.host, storagedrivers[structure['storagerouters'][2]][0].storage_ip)
        self.assertEqual(len(vdisk.domains_dtl), 1)
        self.assertEqual(vdisk.domains_dtl[0].domain_guid, domains[1].guid)

        params = copy.deepcopy(base_params)
        params['dtl_target'] = [domains[2].guid]
        with self.assertRaises(Exception):
            VDiskController.set_config_params(vdisk.guid, new_config_params=params)
        config = StorageRouterClient._dtl_config_cache[vdisk.vpool_guid][vdisk.volume_id]
        self.assertEqual(config.host, storagedrivers[structure['storagerouters'][2]][0].storage_ip)
        self.assertEqual(len(vdisk.domains_dtl), 1)
        self.assertEqual(vdisk.domains_dtl[0].domain_guid, domains[1].guid)

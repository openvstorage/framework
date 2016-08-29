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
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic import fakesleep
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.tests.mockups import MockStorageRouterClient
from ovs.lib.tests.helpers import Helper
from ovs.lib.vdisk import VDiskController


class VDiskTest(unittest.TestCase):
    """
    This test class will validate various vDisk functionality
    """
    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        cls.persistent = PersistentFactory.get_client()
        cls.persistent.clean()
        cls.volatile = VolatileFactory.get_client()
        cls.volatile.clean()
        MockStorageRouterClient.clean()

        fakesleep.monkey_patch()
        EtcdConfiguration.set('/ovs/framework/storagedriver|mds_tlogs', 100)
        EtcdConfiguration.set('/ovs/framework/storagedriver|mds_maxload', 75)
        EtcdConfiguration.set('/ovs/framework/storagedriver|mds_safety', 3)

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()
        MockStorageRouterClient.clean()

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        fakesleep.monkey_restore()

    def tearDown(self):
        """
        Clean up the unittest
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()

    def test_set_and_get_config_params(self):
        """
        Test the set_config_params functionality by validation through the get_config_params functionality
            - Verify default configuration for newly created vDisk
            - Attempt to set disallowed values
            - Attempt to sync and async mode without specifying DTL target
            - Set SCO size
        """
        vpools, storagerouters, storagedrivers, _, mds_services, _, _, _ = Helper.build_service_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )

        # Create vDisk and validate default configuration
        vdisk_1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        configuration = VDiskController.get_config_params(vdisk_guid=vdisk_1.guid)
        expected_keys = {'sco_size', 'dtl_mode', 'dedupe_mode', 'write_buffer', 'dtl_target', 'cache_strategy', 'readcache_limit', 'metadata_cache_size'}
        self.assertEqual(first=expected_keys,
                         second=set(configuration.keys()),
                         msg='Keys returned by get_config_params do not match the expected keys')
        tlog_multiplier = vdisk_1.storagedriver_client.get_tlog_multiplier(vdisk_1.volume_id)
        default_sco_size = vdisk_1.storagedriver_client.get_sco_multiplier(vdisk_1.volume_id) / 1024 * 4
        non_disposable_sco_factor = vdisk_1.storagedriver_client.get_sco_cache_max_non_disposable_factor(vdisk_1.volume_id)
        default_values = {'sco_size': default_sco_size,
                          'dtl_mode': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC,
                          'dedupe_mode': StorageDriverClient.FRAMEWORK_LOCATION_BASED,
                          'dtl_target': [],
                          'write_buffer': int(tlog_multiplier * default_sco_size * non_disposable_sco_factor),
                          'cache_strategy': StorageDriverClient.FRAMEWORK_CACHE_ON_READ,
                          'readcache_limit': None,
                          'metadata_cache_size': StorageDriverClient.METADATA_CACHE_PAGE_SIZE * 1024}
        for key, value in default_values.iteritems():
            self.assertEqual(first=configuration[key],
                             second=value,
                             msg='Value for "{0}" does not match expected default value'.format(key))

        # Attempt to set incorrect values
        new_config_params = {'dtl_mode': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC,
                             'sco_size': 4,
                             'dtl_target': [],
                             'dedupe_mode': StorageDriverClient.FRAMEWORK_LOCATION_BASED,
                             'write_buffer': 128,
                             'cache_strategy': StorageDriverClient.FRAMEWORK_CACHE_ON_READ}
        for key, values in {'dtl_mode': ['unknown', StorageDriverClient.VOLDRV_DTL_ASYNC],
                            'sco_size': list(set(range(257)).difference({4, 8, 16, 32, 64, 128})) + [-1],
                            'dtl_target': ['', {}, (), 0],
                            'dedupe_mode': ['unknown', StorageDriverClient.VOLDRV_LOCATION_BASED],
                            'write_buffer': [-1] + range(128) + range(10241, 10300),
                            'cache_strategy': ['unknown', StorageDriverClient.VOLDRV_CACHE_ON_READ],
                            'readcache_limit': [-1, 0] + range(10241, 10300),
                            'metadata_cache_size': [-1] + range(256 * 24)}.iteritems():
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

        # @TODO: Add much more functionality for set_config_params

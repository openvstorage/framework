# Copyright (C) 2017 iNuron NV
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
Test module for vDisk events functionality
"""
import unittest
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import MDSMetaDataBackendConfig, MDSNodeConfig
from ovs.extensions.storageserver.tests.mockups import StorageRouterClient
from ovs.lib.tests.helpers import Helper
from ovs.lib.vdisk import VDiskController


class VDiskEventsTest(unittest.TestCase):
    """
    This test class will validate various vDisk events functionality
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
        StorageRouterClient.clean()

        Configuration.set('/ovs/framework/storagedriver|mds_tlogs', 100)
        Configuration.set('/ovs/framework/storagedriver|mds_maxload', 75)
        Configuration.set('/ovs/framework/storagedriver|mds_safety', 2)

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()
        StorageRouterClient.clean()

    def tearDown(self):
        """
        Clean up the unittest
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()

    def test_reusing_devicename(self):
        """
        Validates whether the framework can handle out of sync processed events when a vDisk with the same devicename
        is created and removed over and over
        """
        structure = Helper.build_service_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        storagedriver = structure['storagedrivers'][1]
        mds_service = structure['mds_services'][1]
        backend_config = MDSMetaDataBackendConfig([MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                                                 port=mds_service.service.ports[0])])
        devicename = '/test.raw'
        size = 1024 ** 3
        srclient = StorageRouterClient(vpool.guid, None)

        # A normal flow would be:
        # * create volume, resize event,
        # * delete volume, delete event,
        # * create volume, resize event,
        # * delete volume, delete event

        # Let's test the normal flow
        first_volume_id = srclient.create_volume(devicename, backend_config, size, storagedriver.storagedriver_id)
        VDiskController.resize_from_voldrv(first_volume_id, size, devicename, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 1)
        srclient.unlink(devicename)
        VDiskController.delete_from_voldrv(first_volume_id)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 0)
        second_volume_id = srclient.create_volume(devicename, backend_config, size, storagedriver.storagedriver_id)
        VDiskController.resize_from_voldrv(second_volume_id, size, devicename, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 1)
        srclient.unlink(devicename)
        VDiskController.delete_from_voldrv(second_volume_id)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 0)

        # Out of sync - scenario 1
        first_volume_id = srclient.create_volume(devicename, backend_config, size, storagedriver.storagedriver_id)
        VDiskController.resize_from_voldrv(first_volume_id, size, devicename, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 1)
        srclient.unlink(devicename)
        second_volume_id = srclient.create_volume(devicename, backend_config, size, storagedriver.storagedriver_id)
        VDiskController.resize_from_voldrv(second_volume_id, size, devicename, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 2)
        VDiskController.delete_from_voldrv(first_volume_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 1)
        srclient.unlink(devicename)
        VDiskController.delete_from_voldrv(second_volume_id)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 0)

        # Out of sync - scenario 2
        first_volume_id = srclient.create_volume(devicename, backend_config, size, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 0)
        srclient.unlink(devicename)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 0)
        second_volume_id = srclient.create_volume(devicename, backend_config, size, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 0)
        VDiskController.resize_from_voldrv(first_volume_id, size, devicename, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 0)
        VDiskController.resize_from_voldrv(second_volume_id, size, devicename, storagedriver.storagedriver_id)
        self.assertEqual(len(srclient.list_volumes()), 1)
        self.assertEqual(len(vpool.vdisks), 1)
        srclient.unlink(devicename)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 1)
        VDiskController.delete_from_voldrv(first_volume_id)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 1)
        VDiskController.delete_from_voldrv(second_volume_id)
        self.assertEqual(len(srclient.list_volumes()), 0)
        self.assertEqual(len(vpool.vdisks), 0)

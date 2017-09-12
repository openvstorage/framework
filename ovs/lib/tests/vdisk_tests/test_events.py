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
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.storageserver.storagedriver import MDSMetaDataBackendConfig, MDSNodeConfig
from ovs.extensions.storageserver.tests.mockups import StorageRouterClient
from ovs.lib.vdisk import VDiskController


class VDiskEventsTest(unittest.TestCase):
    """
    This test class will validate various vDisk events functionality
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        DalHelper.setup()

    def tearDown(self):
        """
        Clean up the unittest
        """
        DalHelper.teardown()

    def test_reusing_devicename(self):
        """
        Validates whether the framework can handle out of sync processed events when a vDisk with the same devicename
        is created and removed over and over
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        storagedriver = structure['storagedrivers'][1]
        mds_service = structure['mds_services'][1]
        # noinspection PyArgumentList
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

    def test_sync(self):
        """
        Validates whether the sync works as expected
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        storagedriver = structure['storagedrivers'][1]
        mds_service = structure['mds_services'][1]
        # noinspection PyArgumentList
        backend_config = MDSMetaDataBackendConfig([MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                                                 port=mds_service.service.ports[0])])
        srclient = StorageRouterClient(vpool.guid, None)

        VDiskController.create_new('one', 1024 ** 3, storagedriver.guid)
        VDiskController.create_new('two', 1024 ** 3, storagedriver.guid)

        vdisks = VDiskList.get_vdisks()
        self.assertEqual(len(vdisks), 2)
        self.assertEqual(len(srclient.list_volumes()), 2)

        VDiskController.sync_with_reality()
        vdisks = VDiskList.get_vdisks()
        self.assertEqual(len(vdisks), 2)
        self.assertEqual(len(srclient.list_volumes()), 2)

        volume_id = srclient.create_volume('/three.raw', backend_config, 1024 ** 3, storagedriver.storagedriver_id)

        vdisks = VDiskList.get_vdisks()
        self.assertEqual(len(vdisks), 2)
        self.assertEqual(len(srclient.list_volumes()), 3)

        VDiskController.sync_with_reality()
        vdisks = VDiskList.get_vdisks()
        self.assertEqual(len(vdisks), 3)
        self.assertEqual(len(srclient.list_volumes()), 3)

        vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
        self.assertEqual(vdisk.devicename, '/three.raw')

        vdisk = VDisk()
        vdisk.volume_id = 'foo'
        vdisk.name = 'foo'
        vdisk.devicename = 'foo.raw'
        vdisk.size = 1024 ** 3
        vdisk.vpool = vpool
        vdisk.save()
        vdisks = VDiskList.get_vdisks()
        self.assertEqual(len(vdisks), 4)
        self.assertEqual(len(srclient.list_volumes()), 3)

        VDiskController.sync_with_reality()
        vdisks = VDiskList.get_vdisks()
        self.assertEqual(len(vdisks), 3)
        self.assertEqual(len(srclient.list_volumes()), 3)

        with self.assertRaises(ObjectNotFoundException):
            vdisk.save()

    def test_folder_renames(self):
        """
        Validates whether folder renames are correctly processed
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        storagedriver = structure['storagedrivers'][1]
        vdisk1 = VDisk(VDiskController.create_new('foo/one.raw', 1024 ** 3, storagedriver.guid))
        vdisk2 = VDisk(VDiskController.create_new('bar/two.raw', 1024 ** 3, storagedriver.guid))
        vdisk3 = VDisk(VDiskController.create_new('three.raw', 1024 ** 3, storagedriver.guid))

        VDiskController.rename_from_voldrv(old_path='/thr', new_path='/test', storagedriver_id=storagedriver.storagedriver_id)
        vdisk1.discard()
        vdisk2.discard()
        vdisk3.discard()
        self.assertEqual(vdisk1.devicename, '/foo/one.raw')
        self.assertEqual(vdisk2.devicename, '/bar/two.raw')
        self.assertEqual(vdisk3.devicename, '/three.raw')

        VDiskController.rename_from_voldrv(old_path='/foo', new_path='/bar', storagedriver_id=storagedriver.storagedriver_id)
        vdisk1.discard()
        vdisk2.discard()
        vdisk3.discard()
        self.assertEqual(vdisk1.devicename, '/bar/one.raw')
        self.assertEqual(vdisk2.devicename, '/bar/two.raw')
        self.assertEqual(vdisk3.devicename, '/three.raw')

        VDiskController.rename_from_voldrv(old_path='/bar', new_path='/foo', storagedriver_id=storagedriver.storagedriver_id)
        vdisk1.discard()
        vdisk2.discard()
        vdisk3.discard()
        self.assertEqual(vdisk1.devicename, '/foo/one.raw')
        self.assertEqual(vdisk2.devicename, '/foo/two.raw')
        self.assertEqual(vdisk3.devicename, '/three.raw')

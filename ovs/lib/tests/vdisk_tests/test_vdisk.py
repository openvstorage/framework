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
Test module for vDisk functionality
"""
import unittest
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.vdisklist import VDiskList
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
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

        EtcdConfiguration.set('/ovs/framework/storagedriver|mds_tlogs', 100)
        EtcdConfiguration.set('/ovs/framework/storagedriver|mds_maxload', 75)
        EtcdConfiguration.set('/ovs/framework/storagedriver|mds_safety', 2)

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()
        MockStorageRouterClient.clean()

    def tearDown(self):
        """
        Clean up the unittest
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()

    def test_create_new(self):
        """
        Test the create new volume functionality
        """
        _, _, storagedrivers, _, _, _, _, _ = Helper.build_service_structure(
            {'vpools': [1, 2],
             'domains': [],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': []}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        two_tib = 2 * 1024 ** 4

        # Verify maximum size of 2TiB
        self.assertRaises(excClass=ValueError,
                          callableObj=VDiskController.create_new,
                          name='vdisk_1',
                          size=two_tib + 1,
                          storagedriver_guid=storagedrivers[1].guid)

        # Create volume of maximum size
        vdisk_name = 'vdisk_1'
        VDiskController.create_new(name=vdisk_name, size=two_tib, storagedriver_guid=storagedrivers[1].guid)

        # Attempt to create same volume on same vPool
        self.assertRaises(excClass=RuntimeError,
                          callableObj=VDiskController.create_new,
                          name=vdisk_name,
                          size=two_tib,
                          storagedriver_guid=storagedrivers[1].guid)

        # Attempt to create same volume on another vPool
        VDiskController.create_new(name=vdisk_name, size=two_tib, storagedriver_guid=storagedrivers[2].guid)

    def test_delete(self):
        """
        Test the delete of a vDisk
        Create 2 vDisks with identical names on 2 different vPools and delete them
        """
        vpools, _, storagedrivers, _, _, _, _, _ = Helper.build_service_structure(
            {'vpools': [1, 2],
             'domains': [],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': []}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vdisk1 = VDisk(VDiskController.create_new(name='vdisk_1', size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        vdisk2 = VDisk(VDiskController.create_new(name='vdisk_1', size=1024 ** 3, storagedriver_guid=storagedrivers[2].guid))

        # Delete vDisk1 and make some assertions
        VDiskController.delete(diskguid=vdisk1.guid)
        self.assertRaises(excClass=ObjectNotFoundException,
                          callableObj=VDisk,
                          guid=vdisk1.guid)
        self.assertEqual(first=len(VDiskController.list_volumes()),
                         second=1,
                         msg='Expected to find only 1 volume in Storage Driver list_volumes')
        self.assertIn(member=vdisk2,
                      container=VDiskList.get_vdisks(),
                      msg='vDisk2 should still be modeled')

        # Delete vDisk2 and make some assertions
        VDiskController.delete(diskguid=vdisk2.guid)
        self.assertRaises(excClass=ObjectNotFoundException,
                          callableObj=VDisk,
                          guid=vdisk2.guid)
        self.assertEqual(first=len(VDiskController.list_volumes()),
                         second=0,
                         msg='Expected to find no more volumes in Storage Driver list_volumes')

    def test_list_volumes(self):
        """
        Test the list volumes functionality
        If vpool_guid is provided, list all volumes related to that vPool
        If vpool_guid is not provided, list all volumes
        """
        vpools, _, storagedrivers, _, _, _, _, _ = Helper.build_service_structure(
            {'vpools': [1, 2],
             'domains': [],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': []}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vpool1 = vpools[1]
        vpool2 = vpools[2]
        VDiskController.create_new(name='vdisk_1', size=1024 ** 4, storagedriver_guid=storagedrivers[1].guid)
        VDiskController.create_new(name='vdisk_1', size=1024 ** 4, storagedriver_guid=storagedrivers[2].guid)
        VDiskController.create_new(name='vdisk_2', size=1024 ** 4, storagedriver_guid=storagedrivers[2].guid)
        VDiskController.create_new(name='vdisk_3', size=1024 ** 4, storagedriver_guid=storagedrivers[2].guid)
        all_vdisks = VDiskList.get_vdisks()

        # List all volumes
        sd_volume_ids = set(VDiskController.list_volumes())
        model_volume_ids = set([vdisk.volume_id for vdisk in all_vdisks])
        self.assertEqual(first=len(sd_volume_ids),
                         second=4,
                         msg='Expected to retrieve all 4 volumes')
        self.assertEqual(first=sd_volume_ids,
                         second=model_volume_ids,
                         msg='Volume IDs from Storage Driver not identical to volume IDs in model. SD: {0}  -  Model: {1}'.format(sd_volume_ids, model_volume_ids))

        # List all volumes of vpools[1]
        sd_vpool1_volume_ids = set(VDiskController.list_volumes(vpool_guid=vpool1.guid))
        model_vpool1_volume_ids = set([vdisk.volume_id for vdisk in all_vdisks if vdisk.vpool == vpool1])
        self.assertEqual(first=len(sd_vpool1_volume_ids),
                         second=1,
                         msg='Expected to retrieve 1 volume')
        self.assertEqual(first=sd_vpool1_volume_ids,
                         second=model_vpool1_volume_ids,
                         msg='Volume IDs for vPool1 from Storage Driver not identical to volume IDs in model. SD: {0}  -  Model: {1}'.format(sd_vpool1_volume_ids, model_vpool1_volume_ids))

        # List all volumes of vpools[2]
        sd_vpool2_volume_ids = set(VDiskController.list_volumes(vpool_guid=vpool2.guid))
        model_vpool2_volume_ids = set([vdisk.volume_id for vdisk in all_vdisks if vdisk.vpool == vpool2])
        self.assertEqual(first=len(sd_vpool2_volume_ids),
                         second=3,
                         msg='Expected to retrieve 3 volumes')
        self.assertEqual(first=sd_vpool2_volume_ids,
                         second=model_vpool2_volume_ids,
                         msg='Volume IDs for vPool2 from Storage Driver not identical to volume IDs in model. SD: {0}  -  Model: {1}'.format(sd_vpool2_volume_ids, model_vpool2_volume_ids))

    def test_set_as_template(self):
        """
        Test the set as template functionality
        """
        _, _, storagedrivers, _, _, _, _, _ = Helper.build_service_structure(
            {'vpools': [1],
             'domains': [],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': []}  # (<id>, <storagerouter_id>, <domain_id>)
        )
        vdisk = VDisk(VDiskController.create_new(name='vdisk_1', size=1024 ** 4, storagedriver_guid=storagedrivers[1].guid))

        # Set as template and validate the model
        self.assertFalse(expr=vdisk.is_vtemplate, msg='Dynamic property "is_vtemplate" should be False')
        VDiskController.set_as_template(vdisk.guid)
        self.assertTrue(expr=vdisk.is_vtemplate, msg='Dynamic property "is_vtemplate" should be True')

        # Try again and verify job succeeds, previously we raised error when setting as template an additional time
        VDiskController.set_as_template(vdisk.guid)
        self.assertTrue(expr=vdisk.is_vtemplate, msg='Dynamic property "is_vtemplate" should still be True')

    def test_clean_devicename(self):
        test = {'Foo Bar': '/foo_bar.raw',
                '/Foo Bar .raw': '/foo_bar_.raw',
                'foo-bar.rawtest': '/foo-bar.rawtest.raw',
                'test///folder': '/test/folder.raw',
                'foobar-flat.vmdk': '/foobar-flat.vmdk',
                '//test.raw': '/test.raw',
                'test/.raw': '/test/.raw.raw',
                '//d\'!@#%xfoo Bar/te_b --asdfS SA AS lolz///f.wrv.': '/dxfoo_bar/te_b_--asdfs_sa_as_lolz/f.wrv.raw'}
        for raw, expected in test.iteritems():
            result = VDiskController.clean_devicename(raw)
            self.assertEqual(result, expected)

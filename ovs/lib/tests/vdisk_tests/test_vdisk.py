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
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.tests.mockups import MockStorageRouterClient
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

    def test_set_as_template(self):
        """
        Test the set as template functionality
        """
        backend_type = BackendType()
        backend_type.name = 'BackendType'
        backend_type.code = 'BT'
        backend_type.save()

        vpool = VPool()
        vpool.name = 'vpool'
        vpool.status = 'RUNNING'
        vpool.backend_type = backend_type
        vpool.save()

        vdisk = VDisk()
        vdisk.name = 'vdisk_1'
        vdisk.devicename = 'vdisk_1'
        vdisk.volume_id = 'vdisk_1'
        vdisk.vpool = vpool
        vdisk.size = 0
        vdisk.save()

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

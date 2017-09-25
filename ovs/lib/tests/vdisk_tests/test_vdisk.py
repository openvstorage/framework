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
import time
import unittest
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.servicefactory import ServiceFactory
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

    def tearDown(self):
        """
        Clean up the unittest
        """
        DalHelper.teardown(fake_sleep=True)

    @staticmethod
    def _roll_out_dtl_services(vpool, storagerouters):
        """
        Deploy and start the DTL service on all storagerouters
        :param storagerouters: StorageRouters to deploy and start a DTL service on
        :return: None
        """
        service_manager = ServiceFactory.get_manager()
        service_name = 'dtl_{0}'.format(vpool.name)
        for sr in storagerouters.values():
            client = SSHClient(sr, 'root')
            service_manager.add_service(name=service_name, client=client)
            service_manager.start_service(name=service_name, client=client)

    def test_create_new(self):
        """
        Test the create new volume functionality
            - Attempt to create a vDisk larger than 2 TiB
            - Create a vDisk of exactly 2 TiB
            - Attempt to create a vDisk with identical name
            - Attempt to create a vDisk with identical devicename
            - Create a vDisk with identical name on another vPool
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)]}  # (<id>, <storagedriver_id>)
        )
        vpools = structure['vpools']
        mds_services = structure['mds_services']
        storagedrivers = structure['storagedrivers']
        storagerouters = structure['storagerouters']
        size_64_tib = 64 * 1024 ** 4

        # Verify maximum size of 64TiB
        vdisk_name_1 = 'vdisk_1'
        vdisk_name_2 = 'vdisk_2'
        with self.assertRaises(ValueError):
            VDiskController.create_new(volume_name=vdisk_name_1, volume_size=size_64_tib + 1, storagedriver_guid=storagedrivers[1].guid)
        self.assertTrue(expr=len(VDiskList.get_vdisks()) == 0, msg='Expected to find 0 vDisks after failure 1')

        # Create volume of maximum size
        VDiskController.create_new(volume_name=vdisk_name_1, volume_size=size_64_tib, storagedriver_guid=storagedrivers[1].guid)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 1, msg='Expected to find 1 vDisk')
        self.assertTrue(expr=vdisks[0].storagerouter_guid == storagerouters[1].guid, msg='Storage Router does not match expected value')
        self.assertTrue(expr=vdisks[0].size == size_64_tib, msg='Size does not match expected value')
        self.assertTrue(expr=vdisks[0].name == vdisk_name_1, msg='Name does not match expected value')
        self.assertTrue(expr=vdisks[0].vpool == vpools[1], msg='vPool does not match expected value')
        self.assertTrue(expr=vdisks[0].devicename == VDiskController.clean_devicename(vdisk_name_1), msg='Devicename does not match expected value')

        # Attempt to create same volume on same vPool
        with self.assertRaises(RuntimeError):
            VDiskController.create_new(volume_name=vdisk_name_1, volume_size=size_64_tib, storagedriver_guid=storagedrivers[1].guid)
        self.assertTrue(expr=len(VDiskList.get_vdisks()) == 1, msg='Expected to find 1 vDisk after failure 2')

        # Attempt to create volume with identical devicename on same vPool
        with self.assertRaises(RuntimeError):
            VDiskController.create_new(volume_name='{0}%^$'.format(vdisk_name_1), volume_size=size_64_tib, storagedriver_guid=storagedrivers[1].guid)
        self.assertTrue(expr=len(VDiskList.get_vdisks()) == 1, msg='Expected to find 1 vDisk after failure 3')

        # Create same volume on another vPool
        vdisk2 = VDisk(VDiskController.create_new(volume_name=vdisk_name_2, volume_size=size_64_tib, storagedriver_guid=storagedrivers[2].guid))
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks')
        self.assertTrue(expr=vdisk2.storagerouter_guid == storagerouters[1].guid, msg='Storage Router does not match expected value')
        self.assertTrue(expr=vdisk2.size == size_64_tib, msg='Size does not match expected value')
        self.assertTrue(expr=vdisk2.name == vdisk_name_2, msg='Name does not match expected value')
        self.assertTrue(expr=vdisk2.vpool == vpools[2], msg='vPool does not match expected value')
        self.assertTrue(expr=vdisk2.devicename == VDiskController.clean_devicename(vdisk_name_2), msg='Devicename does not match expected value')

        # Attempt to create vDisk on Storage Driver without MDS service
        mds_services[1].service.storagerouter = storagerouters[2]
        mds_services[1].service.save()
        with self.assertRaises(RuntimeError):
            VDiskController.create_new(volume_name='vdisk_3', volume_size=size_64_tib, storagedriver_guid=storagedrivers[1].guid)
        self.assertTrue(expr=len(VDiskList.get_vdisks()) == 2, msg='Expected to find 2 vDisks after failure 4')

    def test_create_from_template(self):
        """
        Test the create from template functionality
            - Create a vDisk and convert to vTemplate
            - Attempt to create from template from a vDisk which is not a vTemplate
            - Create from template basic scenario
            - Attempt to create from template using same name
            - Attempt to create from template using same devicename
            - Attempt to create from template using Storage Router on which vPool is not extended
            - Attempt to create from template using non-existing Storage Driver
            - Attempt to create from template using Storage Driver which does not have an MDS service
            - Create from template on another Storage Router
            - Create from template without specifying a Storage Router
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1, 2, 3],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        mds_services = structure['mds_services']
        storagedrivers = structure['storagedrivers']
        storagerouters = structure['storagerouters']
        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)

        template = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        vdisk_name = 'from_template_1'
        VDiskController.set_as_template(vdisk_guid=template.guid)
        self.assertTrue(expr=template.is_vtemplate, msg='Dynamic property "is_vtemplate" should be True')

        # Create from vDisk which is not a vTemplate
        template.storagedriver_client._set_object_type(template.volume_id, 'BASE')
        template.invalidate_dynamics(['info', 'is_vtemplate'])
        with self.assertRaises(RuntimeError):
            VDiskController.create_from_template(vdisk_guid=template.guid, name=vdisk_name, storagerouter_guid=storagerouters[1].guid)

        # Create from template
        template.storagedriver_client._set_object_type(template.volume_id, 'TEMPLATE')
        template.invalidate_dynamics(['info', 'is_vtemplate'])
        info = VDiskController.create_from_template(vdisk_guid=template.guid, name=vdisk_name, storagerouter_guid=storagerouters[1].guid)
        expected_keys = ['vdisk_guid', 'name', 'backingdevice']
        self.assertEqual(first=set(info.keys()),
                         second=set(expected_keys),
                         msg='Create from template returned not the expected keys')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected 2 vDisks')
        vdisk = [vdisk for vdisk in vdisks if vdisk.is_vtemplate is False][0]
        self.assertTrue(expr=vdisk.name == vdisk_name, msg='vDisk name is incorrect. Expected: {0}  -  Actual: {1}'.format(vdisk_name, vdisk.name))
        self.assertTrue(expr=vdisk.parent_vdisk == template, msg='The parent of the vDisk is incorrect')

        # Attempt to create from template using same name
        with self.assertRaises(RuntimeError):
            VDiskController.create_from_template(vdisk_guid=template.guid, name=vdisk_name, storagerouter_guid=storagerouters[1].guid)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected 2 vDisks after failed attempt 1')

        # Attempt to create from template using same devicename
        with self.assertRaises(RuntimeError):
            VDiskController.create_from_template(vdisk_guid=template.guid, name='^{0}$*'.format(vdisk_name), storagerouter_guid=storagerouters[1].guid)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected 2 vDisks after failed attempt 2')

        # Attempt to create from template on Storage Router on which vPool is not extended
        with self.assertRaises(RuntimeError):
            VDiskController.create_from_template(vdisk_guid=template.guid, name='from_template_2', storagerouter_guid=storagerouters[3].guid)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected 2 vDisks after failed attempt 3')

        # Attempt to create on non-existing Storage Driver
        storagedrivers[1].storagedriver_id = 'non-existing'
        storagedrivers[1].save()
        with self.assertRaises(RuntimeError):
            VDiskController.create_from_template(vdisk_guid=template.guid, name='from_template_2')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected 2 vDisks after failed attempt 4')
        storagedrivers[1].storagedriver_id = '1'
        storagedrivers[1].save()

        # Attempt to create on Storage Driver without MDS service
        mds_services[1].service.storagerouter = storagerouters[3]
        mds_services[1].service.save()
        with self.assertRaises(RuntimeError):
            VDiskController.create_from_template(vdisk_guid=template.guid, name='from_template_2', storagerouter_guid=storagerouters[1].guid)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected 2 vDisks after failed attempt 5')
        mds_services[1].service.storagerouter = storagerouters[1]
        mds_services[1].service.save()

        # Create from template on another Storage Router
        vdisk2 = VDisk(VDiskController.create_from_template(vdisk_guid=template.guid, name='from_template_2', storagerouter_guid=storagerouters[2].guid)['vdisk_guid'])
        self.assertTrue(expr=vdisk2.storagerouter_guid == storagerouters[2].guid, msg='Expected vdisk2 to be hosted by Storage Router 2')

        # Create from template without specifying Storage Router
        vdisk3 = VDisk(VDiskController.create_from_template(vdisk_guid=template.guid, name='from_template_3')['vdisk_guid'])
        self.assertTrue(expr=vdisk3.storagerouter_guid == template.storagerouter_guid, msg='Expected vdisk3 to be hosted by Storage Router 1')

    def test_delete(self):
        """
        Test the delete of a vDisk
            - Create 2 vDisks with identical names on 2 different vPools
            - Delete 1st vDisk and verify other still remains on correct vPool
            - Delete 2nd vDisk and verify no more volumes left
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)]}  # (<id>, <storagedriver_id>)
        )
        domains = structure['domains']
        storagedrivers = structure['storagedrivers']

        vdisk1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        vdisk2 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[2].guid))

        vdisk_domain = VDiskDomain()
        vdisk_domain.domain = domains[1]
        vdisk_domain.vdisk = vdisk1
        vdisk_domain.save()

        # Delete vDisk1 and make some assertions
        VDiskController.delete(vdisk_guid=vdisk1.guid)
        with self.assertRaises(ObjectNotFoundException):
            VDisk(vdisk1.guid)
        self.assertEqual(first=len(VDiskController.list_volumes()),
                         second=1,
                         msg='Expected to find only 1 volume in Storage Driver list_volumes')
        self.assertIn(member=vdisk2,
                      container=VDiskList.get_vdisks(),
                      msg='vDisk2 should still be modeled')

        # Delete vDisk2 and make some assertions
        VDiskController.delete(vdisk_guid=vdisk2.guid)
        with self.assertRaises(ObjectNotFoundException):
            VDisk(vdisk2.guid)
        self.assertEqual(first=len(VDiskController.list_volumes()),
                         second=0,
                         msg='Expected to find no more volumes in Storage Driver list_volumes')

    def test_clone(self):
        """
        Test the clone functionality
            - Create a vDisk with name 'clone1'
            - Clone the vDisk and make some assertions
            - Attempt to clone again using same name and same devicename
            - Attempt to clone on Storage Router which is not linked to the vPool on which the original vDisk is hosted
            - Attempt to clone on Storage Driver without MDS service
            - Attempt to clone from snapshot which is not yet completely synced to backend
            - Attempt to delete the snapshot from which a clone was made
            - Clone the vDisk on another Storage Router
            - Clone another vDisk with name 'clone1' linked to another vPool
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'storagerouters': [1, 2, 3],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)]}  # (<id>, <storagedriver_id>)
        )
        vpools = structure['vpools']
        mds_services = structure['mds_services']
        service_type = structure['service_types']['MetadataServer']
        storagedrivers = structure['storagedrivers']
        storagerouters = structure['storagerouters']
        self._roll_out_dtl_services(vpool=vpools[1], storagerouters=storagerouters)
        self._roll_out_dtl_services(vpool=vpools[2], storagerouters=storagerouters)

        # Basic clone scenario
        vdisk1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        clone1_info = VDiskController.clone(vdisk_guid=vdisk1.guid,
                                            name='clone1')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks')

        clones = VDiskList.get_by_parentsnapshot(vdisk1.snapshot_ids[1][0])
        self.assertTrue(expr=len(clones) == 1, msg='Expected to find 1 vDisk with parent snapshot')
        self.assertTrue(expr=len(vdisk1.child_vdisks) == 1, msg='Expected to find 1 child vDisk')

        for expected_key in ['vdisk_guid', 'name', 'backingdevice']:
            self.assertTrue(expr=expected_key in clone1_info, msg='Expected to find key "{0}" in clone_info'.format(expected_key))
        self.assertTrue(expr=clones[0].guid == clone1_info['vdisk_guid'], msg='Guids do not match')
        self.assertTrue(expr=clones[0].name == clone1_info['name'], msg='Names do not match')
        self.assertTrue(expr=clones[0].devicename == clone1_info['backingdevice'], msg='Device names do not match')

        # Attempt to clone again with same name
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk1.guid,
                                  name='clone1')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks after failed clone attempt 1')

        # Attempt to clone again with a name which will have identical devicename
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk1.guid,
                                  name='clone1%')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks after failed clone attempt 2')

        # Attempt to clone on Storage Router on which vPool is not extended
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk1.guid,
                                  name='clone2',
                                  storagerouter_guid=storagerouters[2].guid)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks after failed clone attempt 3')

        # Attempt to clone on non-existing Storage Driver
        storagedrivers[1].storagedriver_id = 'non-existing'
        storagedrivers[1].save()
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk1.guid,
                                  name='clone2')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks after failed clone attempt 4')
        storagedrivers[1].storagedriver_id = '1'
        storagedrivers[1].save()

        # Attempt to clone on Storage Driver without MDS service
        mds_services[1].service.storagerouter = storagerouters[3]
        mds_services[1].service.save()
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk1.guid,
                                  name='clone2')
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks after failed clone attempt 5')
        mds_services[1].service.storagerouter = storagerouters[1]
        mds_services[1].service.save()

        # Attempt to clone by providing snapshot_id not synced to backend
        self.assertTrue(expr=len(vdisk1.snapshots) == 1, msg='Expected to find only 1 snapshot before cloning')
        self.assertTrue(expr=len(vdisk1.snapshot_ids[1]) == 1, msg='Expected to find only 1 snapshot ID before cloning')
        metadata = {'label': 'label1',
                    'timestamp': int(time.time()),
                    'is_sticky': False,
                    'in_backend': False,
                    'is_automatic': True,
                    'is_consistent': True}
        snapshot_id = VDiskController.create_snapshot(vdisk_guid=vdisk1.guid, metadata=metadata)
        self.assertTrue(expr=len(vdisk1.snapshots) == 2, msg='Expected to find 2 snapshots')
        self.assertTrue(expr=len(vdisk1.snapshot_ids[1]) == 2, msg='Expected to find 2 snapshot IDs')
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk1.guid,
                                  name='clone2',
                                  snapshot_id=snapshot_id)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 2, msg='Expected to find 2 vDisks after failed clone attempt 6')

        # Update backend synced flag and retry
        vdisk1.storagedriver_client._set_snapshot_in_backend(vdisk1.volume_id, snapshot_id, True)
        vdisk1.invalidate_dynamics(['snapshots', 'snapshot_ids'])
        VDiskController.clone(vdisk_guid=vdisk1.guid,
                              name='clone2',
                              snapshot_id=snapshot_id)
        vdisks = VDiskList.get_vdisks()
        vdisk1.invalidate_dynamics()
        self.assertTrue(expr=len(vdisks) == 3, msg='Expected to find 3 vDisks')
        self.assertTrue(expr=len(vdisk1.child_vdisks) == 2, msg='Expected to find 2 child vDisks')
        self.assertTrue(expr=len(vdisk1.snapshots) == 2, msg='Expected to find 2 snapshots after cloning from a specified snapshot')
        self.assertTrue(expr=len(vdisk1.snapshot_ids[1]) == 2, msg='Expected to find 2 snapshot IDs after cloning from a specified snapshot')

        # Attempt to delete the snapshot that has clones
        with self.assertRaises(RuntimeError):
            VDiskController.delete_snapshot(vdisk_guid=vdisk1.guid,
                                            snapshot_id=snapshot_id)

        # Clone on specific Storage Router
        storagedriver = StorageDriver()
        storagedriver.vpool = vpools[1]
        storagedriver.storagerouter = storagerouters[2]
        storagedriver.name = '3'
        storagedriver.mountpoint = '/'
        storagedriver.cluster_ip = storagerouters[2].ip
        storagedriver.storage_ip = '127.0.0.1'
        storagedriver.storagedriver_id = '3'
        storagedriver.ports = {'management': 1,
                               'xmlrpc': 2,
                               'dtl': 3,
                               'edge': 4}
        storagedriver.save()

        s_id = '{0}-1'.format(storagedriver.storagerouter.name)
        service = Service()
        service.name = s_id
        service.storagerouter = storagedriver.storagerouter
        service.ports = [3]
        service.type = service_type
        service.save()
        mds_service = MDSService()
        mds_service.service = service
        mds_service.number = 0
        mds_service.capacity = 10
        mds_service.vpool = storagedriver.vpool
        mds_service.save()

        clone3 = VDisk(VDiskController.clone(vdisk_guid=vdisk1.guid,
                                             name='clone3',
                                             storagerouter_guid=storagerouters[2].guid)['vdisk_guid'])
        self.assertTrue(expr=clone3.storagerouter_guid == storagerouters[2].guid, msg='Incorrect Storage Router on which the clone is attached')

        # Clone vDisk with existing name on another vPool
        vdisk2 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[2].guid))
        clone_vdisk2 = VDisk(VDiskController.clone(vdisk_guid=vdisk2.guid,
                                                   name='clone1')['vdisk_guid'])
        self.assertTrue(expr=clone_vdisk2.vpool == vpools[2], msg='Cloned vDisk with name "clone1" was created on incorrect vPool')
        self.assertTrue(expr=len([vdisk for vdisk in VDiskList.get_vdisks() if vdisk.name == 'clone1']) == 2, msg='Expected to find 2 vDisks with name "clone1"')

        # Attempt to clone without specifying snapshot and snapshot fails to sync to backend
        StorageRouterClient.synced = False
        vdisk2 = VDisk(VDiskController.create_new(volume_name='vdisk_2', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        with self.assertRaises(RuntimeError):
            VDiskController.clone(vdisk_guid=vdisk2.guid,
                                  name='clone4')
        vdisk2.invalidate_dynamics()
        self.assertTrue(expr=len(vdisk2.snapshots) == 0, msg='Expected to find 0 snapshots after clone failure')
        self.assertTrue(expr=len(vdisk2.snapshot_ids[1]) == 0, msg='Expected to find 0 snapshot IDs after clone failure')
        self.assertTrue(expr=len(vdisk2.child_vdisks) == 0, msg='Expected to find 0 children after clone failure')
        StorageRouterClient.synced = True

    def test_create_snapshot(self):
        """
        Test the create snapshot functionality
            - Create a vDisk
            - Attempt to create a snapshot providing incorrect parameters
            - Create a snapshot and make some assertions
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        storagedrivers = structure['storagedrivers']

        vdisk1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            VDiskController.create_snapshot(vdisk_guid=vdisk1.guid,
                                            metadata='')

        now = int(time.time())
        snapshot_id = VDiskController.create_snapshot(vdisk_guid=vdisk1.guid, metadata={'timestamp': now,
                                                                                        'label': 'label1',
                                                                                        'is_consistent': True,
                                                                                        'is_automatic': True,
                                                                                        'is_sticky': False})
        self.assertTrue(expr=len(vdisk1.snapshots) == 1, msg='Expected to find 1 snapshot')
        self.assertTrue(expr=len(vdisk1.snapshot_ids[1]) == 1, msg='Expected to find 1 snapshot ID')
        snapshot = vdisk1.snapshots[0]
        expected_keys = {'guid', 'timestamp', 'label', 'is_consistent', 'is_automatic', 'is_sticky', 'in_backend', 'stored'}
        self.assertEqual(first=expected_keys,
                         second=set(snapshot.keys()),
                         msg='Set of expected keys differs from reality. Expected: {0}  -  Reality: {1}'.format(expected_keys, set(snapshot.keys())))

        for key, value in {'guid': snapshot_id,
                           'label': 'label1',
                           'stored': 0,
                           'is_sticky': False,
                           'timestamp': now,
                           'in_backend': True,
                           'is_automatic': True,
                           'is_consistent': True}.iteritems():
            self.assertEqual(first=value,
                             second=snapshot[key],
                             msg='Value for key "{0}" does not match reality. Expected: {1}  -  Reality: {2}'.format(key, value, snapshot[key]))

    def test_delete_snapshot(self):
        """
        Test the delete snapshot functionality
            - Create a vDisk and take a snapshot
            - Attempt to delete a non-existing snapshot
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        storagedrivers = structure['storagedrivers']

        vdisk1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 3, storagedriver_guid=storagedrivers[1].guid))
        VDiskController.create_snapshot(vdisk_guid=vdisk1.guid, metadata={'timestamp': int(time.time()),
                                                                          'label': 'label1',
                                                                          'is_consistent': True,
                                                                          'is_automatic': True,
                                                                          'is_sticky': False})
        self.assertTrue(expr=len(vdisk1.snapshots) == 1, msg='Expected to find 1 snapshot')
        self.assertTrue(expr=len(vdisk1.snapshot_ids[1]) == 1, msg='Expected to find 1 snapshot ID')
        with self.assertRaises(RuntimeError):
            VDiskController.delete_snapshot(vdisk_guid=vdisk1.guid,
                                            snapshot_id='non-existing')

        VDiskController.delete_snapshot(vdisk_guid=vdisk1.guid,
                                        snapshot_id=vdisk1.snapshot_ids[1][0])
        self.assertTrue(expr=len(vdisk1.snapshots) == 0, msg='Expected to find no more snapshots')
        self.assertTrue(expr=len(vdisk1.snapshot_ids[1]) == 0, msg='Expected to find no more snapshot IDs')

    def test_list_volumes(self):
        """
        Test the list volumes functionality
            - Create 1 vDisk on vPool1 and create 3 vDisks on vPool2
            - List all volumes
            - List the volumes on vPool1
            - List the volumes on vPool2
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1), (2, 2, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)]}  # (<id>, <storagedriver_id>)
        )
        vpools = structure['vpools']
        storagedrivers = structure['storagedrivers']

        vpool1 = vpools[1]
        vpool2 = vpools[2]
        VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[1].guid)
        VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[2].guid)
        VDiskController.create_new(volume_name='vdisk_2', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[2].guid)
        VDiskController.create_new(volume_name='vdisk_3', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[2].guid)
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
            - Create a vDisk
            - Set it as template and make some assertions
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        storagedrivers = structure['storagedrivers']

        vdisk_1 = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[1].guid))
        metadata = {'is_consistent': True,
                    'is_automatic': True,
                    'is_sticky': False}
        for x in range(5):
            metadata['label'] = 'label{0}'.format(x)
            metadata['timestamp'] = int(time.time())
            VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid, metadata=metadata)
        self.assertTrue(expr=len(vdisk_1.snapshots) == 5, msg='Expected to find 5 snapshots')
        self.assertTrue(expr=len(vdisk_1.snapshot_ids[1]) == 5, msg='Expected to find 5 snapshot IDs')

        # Set as template and validate the model
        self.assertFalse(expr=vdisk_1.is_vtemplate, msg='Dynamic property "is_vtemplate" should be False')
        VDiskController.set_as_template(vdisk_guid=vdisk_1.guid)
        self.assertTrue(expr=vdisk_1.is_vtemplate, msg='Dynamic property "is_vtemplate" should be True')
        self.assertTrue(expr=len(vdisk_1.snapshots) == 1, msg='Expected to find only 1 snapshot after converting to template')
        self.assertTrue(expr=len(vdisk_1.snapshot_ids[1]) == 1, msg='Expected to find only 1 snapshot ID after converting to template')

        # Try again and verify job succeeds, previously we raised error when setting as template an additional time
        VDiskController.set_as_template(vdisk_1.guid)
        self.assertTrue(expr=vdisk_1.is_vtemplate, msg='Dynamic property "is_vtemplate" should still be True')

        # Clone vDisk and verify both child and parent can no longer be set as template
        vdisk_2 = VDisk(VDiskController.create_new(volume_name='vdisk_2', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[1].guid))
        vdisk_2_clone_1 = VDisk(VDiskController.clone(vdisk_guid=vdisk_2.guid,
                                                      name='vdisk_2_clone_1')['vdisk_guid'])
        with self.assertRaises(RuntimeError):
            VDiskController.set_as_template(vdisk_guid=vdisk_2.guid)
        with self.assertRaises(RuntimeError):
            VDiskController.set_as_template(vdisk_guid=vdisk_2_clone_1.guid)

    def test_event_migrate_from_volumedriver(self):
        """
        Test migrate from volumedriver event
        """
        _ = self
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 2)]}  # (<id>, <storagedriver_id>)
        )
        vpool = structure['vpools'][1]
        storagedrivers = structure['storagedrivers']
        storagerouters = structure['storagerouters']
        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)

        vdisk = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 4, storagedriver_guid=storagedrivers[1].guid))
        vdisk.storagedriver_client.migrate(vdisk.volume_id, storagedrivers[2].storagedriver_id, False)
        VDiskController.migrate_from_voldrv(volume_id=vdisk.volume_id, new_owner_id=storagedrivers[2].storagedriver_id)
        self.assertEqual(vdisk.storagedriver_id, storagedrivers[2].storagedriver_id)

    def test_event_resize_from_volumedriver(self):
        """
        Test resize from volumedriver event
            - Create a vDisk using the resize event
            - Resize the created vDisk using the same resize event
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        vpools = structure['vpools']
        storagedrivers = structure['storagedrivers']
        mds_service = structure['mds_services'][1]

        # Create volume using resize from voldrv
        device_name = '/vdisk.raw'
        srclient = StorageRouterClient(vpools[1].guid, None)
        mds_backend_config = DalHelper.generate_mds_metadata_backend_config([mds_service])
        volume_id = srclient.create_volume(device_name, mds_backend_config, 1024 ** 4, str(storagedrivers[1].storagedriver_id))
        VDiskController.resize_from_voldrv(volume_id=volume_id,
                                           volume_size=1024 ** 4,
                                           volume_path=device_name,
                                           storagedriver_id=storagedrivers[1].storagedriver_id)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 1,
                        msg='Expected to find 1 vDisk in model')
        self.assertEqual(first=vdisks[0].name,
                         second='vdisk',
                         msg='Volume name should be vdisk')
        self.assertEqual(first=vdisks[0].volume_id,
                         second=volume_id,
                         msg='Volume ID should be {0}'.format(volume_id))
        self.assertEqual(first=vdisks[0].devicename,
                         second=device_name,
                         msg='Device name should be {0}'.format(device_name))
        self.assertEqual(first=vdisks[0].size,
                         second=1024 ** 4,
                         msg='Size should be 1 TiB')

        # Resize volume using resize from voldrv
        VDiskController.resize_from_voldrv(volume_id=volume_id,
                                           volume_size=2 * 1024 ** 4,
                                           volume_path=device_name,
                                           storagedriver_id=storagedrivers[1].storagedriver_id)
        vdisks = VDiskList.get_vdisks()
        self.assertTrue(expr=len(vdisks) == 1,
                        msg='Expected to find 1 vDisk in model')
        self.assertEqual(first=vdisks[0].name,
                         second='vdisk',
                         msg='Volume name should be vdisk')
        self.assertEqual(first=vdisks[0].size,
                         second=2 * 1024 ** 4,
                         msg='Size should be 2 TiB')

    def test_clean_devicename(self):
        """
        Validates whether a devicename is properly cleaned
        * Test several names and validate the returned devicename
        """
        test = {'Foo Bar': '/Foo_Bar.raw',
                '/Foo Bar .raw': '/Foo_Bar_.raw',
                'foo-bar.rawtest': '/foo-bar.rawtest.raw',
                'test///folder': '/test/folder.raw',
                'foobar-flat.vmdk': '/foobar-flat.vmdk.raw',
                '//test.raw': '/test.raw',
                'test/.raw': '/test/.raw.raw',
                '//d\'!@#%xfoo Bar/te_b --asdfS SA AS lolz///f.wrv.': '/dxfoo_Bar/te_b_--asdfS_SA_AS_lolz/f.wrv..raw'}
        for raw, expected in test.iteritems():
            result = VDiskController.clean_devicename(raw)
            self.assertEqual(result, expected)

    def test_extract_volumename(self):
        """
        Validates whether a correct volumename is yielded from a given devicename
        * Test several devicenames and validate the returned name
        """
        test = {'/foo/Bar/something.raw': 'something',
                '/Some.long.dotted.name.raw': 'Some.long.dotted.name',
                '': '',
                'f4qav@#$%sd.raw': 'f4qav@#$%sd'}
        for devicename, expected in test.iteritems():
            result = VDiskController.extract_volumename(devicename)
            self.assertEqual(result, expected)

    def test_remove_snapshots(self):
        """
        Validates whether the remove_snapshots call works as expected. Due to openvstorage/framework#1534
        it needs to handle some backwards compatibiltiy.
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1)]}  # (<id>, <storagedriver_id>)
        )
        storagedriver = structure['storagedrivers'][1]

        vdisk = VDisk(VDiskController.create_new(volume_name='vdisk_1', volume_size=1024 ** 4, storagedriver_guid=storagedriver.guid))
        snapshots = []
        for i in xrange(10):
            metadata = {'label': 'label{0}'.format(i),
                        'timestamp': int(time.time()),
                        'is_sticky': False,
                        'in_backend': True,
                        'is_automatic': True,
                        'is_consistent': True}
            snapshots.append(VDiskController.create_snapshot(vdisk_guid=vdisk.guid, metadata=metadata))
        vdisk.invalidate_dynamics(['snapshots', 'snapshot_ids'])
        self.assertEqual(len(vdisk.snapshots), 10)
        self.assertEqual(len(vdisk.snapshot_ids[1]), 10)
        snapshot_id = snapshots[0]

        # Old format
        results = VDiskController.delete_snapshots({vdisk.guid: snapshot_id})
        expected = {vdisk.guid: [True, snapshot_id]}
        self.assertDictEqual(results, expected)
        self.assertEqual(len(vdisk.snapshots), 9)
        self.assertEqual(len(vdisk.snapshot_ids[1]), 9)
        results = VDiskController.delete_snapshots({vdisk.guid: snapshot_id})
        expected = {vdisk.guid: [False, results[vdisk.guid][1]]}
        self.assertDictEqual(results, expected)
        self.assertRegexpMatches(results[vdisk.guid][1], '^Snapshot (.*?) does not belong to vDisk')
        self.assertEqual(len(vdisk.snapshots), 9)
        self.assertEqual(len(vdisk.snapshot_ids[1]), 9)
        results = VDiskController.delete_snapshots({'foo': snapshot_id})
        expected = {'foo': [False, results['foo'][1]]}
        self.assertDictEqual(results, expected)
        self.assertRegexpMatches(results['foo'][1], 'VDisk with guid (.*?) could not be found')

        # New format
        snapshot_id1 = snapshots[1]
        snapshot_id2 = snapshots[2]
        results = VDiskController.delete_snapshots({vdisk.guid: [snapshot_id1, snapshot_id2]})
        expected = {vdisk.guid: {'success': True,
                                 'error': None,
                                 'results': {snapshot_id1: [True, snapshot_id1],
                                             snapshot_id2: [True, snapshot_id2]}}}
        self.assertDictEqual(results, expected)
        self.assertEqual(len(vdisk.snapshots), 7)
        self.assertEqual(len(vdisk.snapshot_ids[1]), 7)
        snapshot_id2 = snapshots[3]
        results = VDiskController.delete_snapshots({vdisk.guid: [snapshot_id1, snapshot_id2]})
        expected = {vdisk.guid: {'success': False,
                                 'error': results[vdisk.guid]['error'],
                                 'results': {snapshot_id1: [False, results[vdisk.guid]['results'][snapshot_id1][1]],
                                             snapshot_id2: [True, snapshot_id2]}}}
        self.assertDictEqual(results, expected)
        self.assertEquals(results[vdisk.guid]['error'], 'One or more snapshots could not be removed')
        self.assertRegexpMatches(results[vdisk.guid]['results'][snapshot_id1][1], '^Snapshot (.*?) does not belong to vDisk')
        self.assertEqual(len(vdisk.snapshots), 6)
        self.assertEqual(len(vdisk.snapshot_ids[1]), 6)
        results = VDiskController.delete_snapshots({'foo': [snapshot_id1]})
        expected = {'foo': {'success': False,
                            'error': results['foo']['error'],
                            'results': {}}}
        self.assertDictEqual(results, expected)
        self.assertRegexpMatches(results['foo']['error'], 'VDisk with guid (.*?) could not be found')

        snapshot_id = snapshots[4]
        VDiskController.clone(vdisk.guid, 'clone', snapshot_id)
        results = VDiskController.delete_snapshots({vdisk.guid: [snapshot_id]})
        expected = {vdisk.guid: {'success': False,
                                 'error': results[vdisk.guid]['error'],
                                 'results': {snapshot_id: [False, results[vdisk.guid]['results'][snapshot_id][1]]}}}
        self.assertDictEqual(results, expected)
        self.assertEquals(results[vdisk.guid]['error'], 'One or more snapshots could not be removed')
        self.assertRegexpMatches(results[vdisk.guid]['results'][snapshot_id][1], '^Snapshot (.*?) has [0-9]+ volume(.?) cloned from it, cannot remove$')

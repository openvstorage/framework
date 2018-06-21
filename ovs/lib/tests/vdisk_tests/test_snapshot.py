# Copyright (C) 2018 iNuron NV
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
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.threadhelpers import Waiter
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.tests.mockups import LockedClient
from ovs.lib.helpers.generic.scrubber import ScrubShared
from ovs.lib.generic import GenericController
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
        ScrubShared._test_hooks = {}
        ScrubShared._unittest_data = {'setup': False}

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
        self.assertTrue(expr=len(vdisk1.snapshot_ids) == 1, msg='Expected to find 1 snapshot ID')
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
        self.assertTrue(expr=len(vdisk1.snapshot_ids) == 1, msg='Expected to find 1 snapshot ID')
        with self.assertRaises(RuntimeError):
            VDiskController.delete_snapshot(vdisk_guid=vdisk1.guid,
                                            snapshot_id='non-existing')

        VDiskController.delete_snapshot(vdisk_guid=vdisk1.guid,
                                        snapshot_id=vdisk1.snapshot_ids[0])
        self.assertTrue(expr=len(vdisk1.snapshots) == 0, msg='Expected to find no more snapshots')
        self.assertTrue(expr=len(vdisk1.snapshot_ids) == 0, msg='Expected to find no more snapshot IDs')

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
        self.assertEqual(len(vdisk.snapshot_ids), 10)
        snapshot_id = snapshots[0]

        # Old format
        results = VDiskController.delete_snapshots({vdisk.guid: snapshot_id})
        expected = {vdisk.guid: [True, snapshot_id]}
        self.assertDictEqual(results, expected)
        self.assertEqual(len(vdisk.snapshots), 9)
        self.assertEqual(len(vdisk.snapshot_ids), 9)
        results = VDiskController.delete_snapshots({vdisk.guid: snapshot_id})
        expected = {vdisk.guid: [False, results[vdisk.guid][1]]}
        self.assertDictEqual(results, expected)
        self.assertRegexpMatches(results[vdisk.guid][1], '^Snapshot (.*?) does not belong to vDisk')
        self.assertEqual(len(vdisk.snapshots), 9)
        self.assertEqual(len(vdisk.snapshot_ids), 9)
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
        self.assertEqual(len(vdisk.snapshot_ids), 7)
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
        self.assertEqual(len(vdisk.snapshot_ids), 6)
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

    def test_delete_snapshot_scrubbing_lock(self):
        """
        Tests the skip-if-scrubbed logic
        """
        snapshot_while_scrub_results = []

        def delete_snapshot_while_scrubbing(*args, **kwargs):
            _ = args, kwargs
            try:
                snapshot_while_scrub_results.append(VDiskController.delete_snapshot(vdisk_1.guid, vdisk_1.snapshot_ids[0]))
            except RuntimeError as ex:
                snapshot_while_scrub_results.append(ex)

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        vdisk_1 = vdisks[1]

        # Create automatic snapshot for both vDisks
        success, fail = GenericController.snapshot_all_vdisks()
        self.assertEqual(first=len(fail), second=0, msg='Expected 0 failed snapshots')
        self.assertEqual(first=len(success), second=1, msg='Expected 1 successful snapshots')
        self.assertEqual(first=len(vdisk_1.snapshot_ids), second=1, msg='Expected 1 snapshot ID for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_1.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_1.name))

        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names[0:1]))}  # only 1 disks -> 1 thread
        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}

        hooks = {'post_vdisk_scrub_registration': delete_snapshot_while_scrubbing}  # Make the scrubber wait
        ScrubShared._test_hooks.update(hooks)
        GenericController.execute_scrub()

        # Ensure delete snapshot fails for vdisk_1 because it is being scrubbed
        result_while_scrub = snapshot_while_scrub_results[0]
        self.assertIsInstance(result_while_scrub, Exception, 'Expected an exception to have occurred')
        self.assertEqual(str(result_while_scrub), 'VDisk is being scrubbed. Unable to remove snapshots at this time', 'Excpetion should be about disk being scrubbed')
        self.assertEqual(first=len(vdisk_1.snapshot_ids), second=1, msg='Expected 1 snapshot ID for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_1.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_1.name))

    @staticmethod
    def generate_scrub_related_info(structure, proxy_amount=1, skip_threads_for=None):
        """
        Retrieve the thread and proxy names that the scrubber would get
        :param structure: DAL structure to use
        :param proxy_amount: Amount of proxies to spawn
        :param skip_threads_for: Skip generating thread names for a storagerouter
        :return Tuple with all generated info
        :rtype: tuple[list[str], list[str], dict]
        """
        vpools = structure['vpools']
        storagerouters = structure['storagerouters']
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        thread_names = []
        proxy_names = []
        vdisk_namespaces = {}
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
            for storagerouter in storagerouters.values():
                for partition in storagerouter.disks[0].partitions:
                    for proxy_index in xrange(0, proxy_amount):
                        proxy_name = 'ovs-albaproxy_{0}_{1}_{2}_scrub_{3}'.format(vpool.name, storagerouter.name,
                                                                                  partition.guid, proxy_index)
                        proxy_names.append(proxy_name)
                    if skip_threads_for and skip_threads_for == storagerouter:
                        continue
                    for index in range(2):
                        thread_names.append('execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition.guid, index))
        return proxy_names, thread_names, vdisk_namespaces

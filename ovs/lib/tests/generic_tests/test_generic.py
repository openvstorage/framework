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
Generic test module
"""
import os
import json
import time
import datetime
import unittest
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.generic.threadhelpers import Waiter
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.tests.mockups import LockedClient, StorageRouterClient
from ovs.lib.generic import GenericController
from ovs.lib.tests.helpers import Helper
from ovs.lib.vdisk import VDiskController


class Generic(unittest.TestCase):
    """
    This test class will validate the various scenarios of the Generic logic
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

    @classmethod
    def tearDownClass(cls):
        """
        Tear down changes made during setUpClass
        """
        # Configuration._unittest_data = {}
        cls.persistent = PersistentFactory.get_client()
        cls.persistent.clean()
        cls.volatile = VolatileFactory.get_client()
        cls.volatile.clean()

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.persistent.clean()
        self.volatile.clean()
        Helper.clean()
        StorageRouterClient.clean()

    def tearDown(self):
        """
        Clean up test suite
        """
        self.persistent.clean()
        self.volatile.clean()
        StorageRouterClient.clean()

    def test_snapshot_all_vdisks(self):
        """
        Tests GenericController.snapshot_all_vdisks functionality
        """
        structure = Helper.build_service_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk_1 = structure['vdisks'][1]
        vdisk_2 = structure['vdisks'][2]

        # Create automatic snapshot for both vDisks
        success, fail = GenericController.snapshot_all_vdisks()
        self.assertEqual(first=len(fail), second=0, msg='Expected 0 failed snapshots')
        self.assertEqual(first=len(success), second=2, msg='Expected 2 successful snapshots')
        self.assertEqual(first=len(vdisk_1.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_2.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_2.name))

        # Ensure automatic snapshot fails for vdisk_1 and succeeds for vdisk_2
        vdisk_1.storagedriver_client._set_snapshot_in_backend(volume_id=vdisk_1.volume_id, snapshot_id=vdisk_1.snapshots[0]['guid'], in_backend=False)
        success, fail = GenericController.snapshot_all_vdisks()
        self.assertEqual(first=len(fail), second=1, msg='Expected 1 failed snapshot')
        self.assertEqual(first=fail[0], second=vdisk_1.guid, msg='Expected vDisk {0} to have failed'.format(vdisk_1.name))
        self.assertEqual(first=len(success), second=1, msg='Expected 1 successful snapshot')
        self.assertEqual(first=success[0], second=vdisk_2.guid, msg='Expected vDisk {0} to have succeeded'.format(vdisk_2.name))
        self.assertEqual(first=len(vdisk_1.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_2.snapshots), second=2, msg='Expected 2 snapshots for vDisk {0}'.format(vdisk_2.name))

    def test_clone_snapshot(self):
        """
        Validates that a snapshot that has clones will not be deleted while other snapshots will be deleted
        """
        # Setup
        # There are 2 disks, second one cloned from a snapshot of the first
        structure = Helper.build_service_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk_1 = structure['vdisks'][1]
        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        base = datetime.datetime.now().date()
        base_timestamp = self._make_timestamp(base, datetime.timedelta(1))
        minute = 60
        hour = minute * 60
        for h in [6, 12, 18]:
            timestamp = base_timestamp + (hour * h)
            VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid,
                                            metadata={'label': 'snapshot_{0}:30'.format(str(h)),
                                                      'is_consistent': True,
                                                      'timestamp': str(timestamp)})

        structure = Helper.build_service_structure(structure={'vdisks': [(2, 1, 1, 1)]},
                                                   previous_structure=structure)
        clone_vdisk = structure['vdisks'][2]
        base_snapshot_guid = vdisk_1.snapshots[0]['guid']  # Oldest
        clone_vdisk.parentsnapshot = base_snapshot_guid
        clone_vdisk.save()

        for day in range(10):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            for h in [6, 12, 18]:
                timestamp = base_timestamp + (hour * h)
                VDiskController.create_snapshot(vdisk_guid=clone_vdisk.guid,
                                                metadata={'label': 'snapshot_{0}:30'.format(str(h)),
                                                          'is_consistent': True,
                                                          'timestamp': str(timestamp)})
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * 2)
            GenericController.delete_snapshots(timestamp=base_timestamp + (minute * 30))
        self.assertIn(base_snapshot_guid, [snap['guid'] for snap in vdisk_1.snapshots], 'Snapshot was deleted while there are still clones of it')

    def test_different_snapshot_flags(self):
        """
        Tests the GenericController.delete_snapshots() call, but with different snapshot flags
            Scenario 1: is_automatic: True, is_consistent: True --> Automatically created consistent snapshots should be deleted
            Scenario 2: is_automatic: True, is_consistent: False --> Automatically created non-consistent snapshots should be deleted
            Scenario 3: is_automatic: False, is_consistent: True --> Manually created consistent snapshots should be deleted
            Scenario 4: is_automatic: False, is_consistent: False --> Manually created non-consistent snapshots should be deleted
            Scenario 5: is_sticky: True --> Sticky snapshots of any kind should never be deleted (Only possible to delete manually)
        """
        minute = 60
        hour = minute * 60

        for scenario in range(5):
            structure = Helper.build_service_structure(
                {'vpools': [1],
                 'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
                 'mds_services': [(1, 1)],
                 'storagerouters': [1],
                 'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
            )
            base = datetime.datetime.now().date()
            vdisk_1 = structure['vdisks'][1]
            is_sticky = False
            sticky_hours = []
            if scenario % 2 == 0:
                label = 'c'
                additional_time = minute * 30
                consistent_hours = [2]
                inconsistent_hours = []
            else:
                label = 'i'
                additional_time = 0
                consistent_hours = []
                inconsistent_hours = [2]

            if scenario == 4:
                is_sticky = True
                sticky_hours = consistent_hours

            for day in xrange(35):
                base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
                self._print_message('')
                self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

                self._print_message('- Deleting snapshots')
                GenericController.delete_snapshots(timestamp=base_timestamp + (minute * 30))

                self._validate(vdisk=vdisk_1,
                               current_day=day,
                               base_date=base,
                               sticky_hours=sticky_hours,
                               consistent_hours=consistent_hours,
                               inconsistent_hours=inconsistent_hours)

                self._print_message('- Creating snapshots')
                for x in consistent_hours + inconsistent_hours:
                    timestamp = base_timestamp + (hour * x) + additional_time
                    VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid,
                                                    metadata={'label': 'ss_{0}_{1}:00'.format(label, x),
                                                              'is_sticky': is_sticky,
                                                              'timestamp': str(timestamp),
                                                              'is_automatic': scenario in [0, 1],
                                                              'is_consistent': len(consistent_hours) > 0})
            self.persistent.clean()
            self.volatile.clean()
            StorageRouterClient.clean()

    def test_happypath(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistent
        every now and then. The delete policy is executed every day
        """
        structure = Helper.build_service_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk_1 = structure['vdisks'][1]
        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        # Run the testing scenario
        travis = 'TRAVIS' in os.environ and os.environ['TRAVIS'] == 'true'
        if travis is True:
            self._print_message('Running in Travis, reducing output.')
        base = datetime.datetime.now().date()
        minute = 60
        hour = minute * 60
        consistent_hours = [6, 12, 18]
        inconsistent_hours = xrange(2, 23)

        for day in xrange(0, 35):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            # At the start of the day, delete snapshot policy runs at 00:30
            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots(timestamp=base_timestamp + (minute * 30))

            # Validate snapshots
            self._print_message('- Validating snapshots')
            self._validate(vdisk=vdisk_1,
                           current_day=day,
                           base_date=base,
                           sticky_hours=[],
                           consistent_hours=consistent_hours,
                           inconsistent_hours=inconsistent_hours)

            # During the day, snapshots are taken
            # - Create non consistent snapshot every hour, between 2:00 and 22:00
            # - Create consistent snapshot at 6:30, 12:30, 18:30
            self._print_message('- Creating snapshots')
            for h in inconsistent_hours:
                timestamp = base_timestamp + (hour * h)
                VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid,
                                                metadata={'label': 'ss_i_{0}:00'.format(str(h)),
                                                          'is_consistent': False,
                                                          'timestamp': str(timestamp)})
                if h in consistent_hours:
                    ts = (timestamp + (minute * 30))
                    VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid,
                                                    metadata={'label': 'ss_c_{0}:30'.format(str(h)),
                                                              'is_consistent': True,
                                                              'timestamp': str(ts)})

    def test_scrubbing(self):
        """
        Validates the scrubbing workflow
        * Scenario 1: Validate disabled scrub task and single vDisk scrub logic
        * Scenario 2: 1 vPool, 10 vDisks, 1 scrub role
                      Scrubbing fails for 5 vDisks, check if scrubbing completed for all other vDisks
                      Run scrubbing a 2nd time and verify scrubbing now works for failed vDisks
        * Scenario 3: 1 vPool, 10 vDisks, 5 scrub roles
                      Check if vDisks are divided among all threads
        * Scenario 4: 3 vPools, 9 vDisks, 5 scrub roles
                      Validate 6 threads will be spawned and used out of a potential of 15 (5 scrub roles * 3 vPools)
                      We limit max amount of threads spawned per vPool to 2 in case 3 to 5 vPools are present
        * Scenario 5: Smaller use-cases
                      1 vPool, 0 vDisks
                      1 vPool, 1 vDisk which is vTemplate
        """
        for i in xrange(1, 6):
            Configuration.set('/ovs/framework/hosts/{0}/ports'.format(i), {'storagedriver': [10000, 10100]})

        ##############
        # Scenario 1 #
        ##############
        structure = Helper.build_service_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk = structure['vdisks'][1]
        vpool = structure['vpools'][1]
        storagerouter = structure['storagerouters'][1]
        System._machine_id = {storagerouter.ip: '1'}
        Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps({}, indent=4), raw=True)
        LockedClient.scrub_controller = {'possible_threads': None,
                                         'volumes': {},
                                         'waiter': Waiter(1)}
        LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': False,
                                                                     'scrub_work': [0]}

        # Remove SCRUB partition from StorageRouter and try to scrub on it
        storagerouter.disks[0].partitions[0].roles = []
        storagerouter.disks[0].partitions[0].save()
        with self.assertRaises(RuntimeError) as raise_info:
            VDiskController.scrub_single_vdisk(vdisk.guid, storagerouter.guid)
        self.assertEqual(first='No scrub locations found on StorageRouter {0}'.format(storagerouter.name),
                         second=raise_info.exception.message,
                         msg='Incorrect error message caught')

        # Restore SCRUB partition and attempt to scrub
        storagerouter.disks[0].partitions[0].roles = [DiskPartition.ROLES.SCRUB]
        storagerouter.disks[0].partitions[0].save()
        storagerouter.invalidate_dynamics('partition_config')
        with self.assertRaises(Exception) as raise_info:
            VDiskController.scrub_single_vdisk(vdisk.guid, storagerouter.guid)
        self.assertIn(member='Error when scrubbing vDisk {0}'.format(vdisk.guid),
                      container=raise_info.exception.message,
                      msg='Incorrect error message caught')

        # Make sure scrubbing succeeds now
        LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                     'scrub_work': [0]}
        VDiskController.scrub_single_vdisk(vdisk.guid, storagerouter.guid)
        with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
            self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                             second=0,
                             msg='Scrubbed vDisk {0} does not have the expected amount of scrubbing items: {1}'.format(vdisk.name, 0))

        ##############
        # Scenario 2 #
        ##############
        self.volatile.clean()
        self.persistent.clean()
        structure = Helper.build_service_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 1, 1, 1), (7, 1, 1, 1), (8, 1, 1, 1), (9, 1, 1, 1), (10, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vdisks = structure['vdisks']
        storagerouter_1 = structure['storagerouters'][1]
        storagerouter_2 = structure['storagerouters'][2]
        storagerouter_3 = structure['storagerouters'][3]
        System._machine_id = {storagerouter_1.ip: '1'}
        Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps({}, indent=4), raw=True)
        LockedClient.scrub_controller = {'possible_threads': ['scrub_{0}_{1}'.format(vpool.guid, storagerouter_1.guid)],
                                         'volumes': {},
                                         'waiter': Waiter(1)}

        # Have 1 StorageRouter with 0 SCRUB partitions
        storagerouter_2.disks[0].partitions[0].roles = []
        storagerouter_2.disks[0].partitions[0].save()

        # Have 1 StorageRouter with multiple SCRUB partitions
        partition = DiskPartition()
        partition.offset = 0
        partition.size = storagerouter_3.disks[0].size
        partition.aliases = ['/dev/uda-2']
        partition.state = 'OK'
        partition.mountpoint = '/tmp/unittest/sr_3/disk_1/partition_2'
        partition.disk = storagerouter_3.disks[0]
        partition.roles = [DiskPartition.ROLES.SCRUB]
        partition.save()

        # Try to start scrubbing with a StorageRouter with multiple SCRUB partitions
        with self.assertRaises(RuntimeError) as raise_info:
            GenericController.execute_scrub()
        self.assertEqual(first='Multiple SCRUB partitions defined for StorageRouter {0}'.format(storagerouter_3.name),
                         second=raise_info.exception.message,
                         msg='Incorrect error message caught')
        storagerouter_3.disks[0].partitions[1].delete()
        storagerouter_3.disks[0].partitions[0].roles = []
        storagerouter_3.disks[0].partitions[0].save()
        storagerouter_3.invalidate_dynamics('partition_config')

        # Have 0 SCRUB roles and verify error
        storagerouter_1.disks[0].partitions[0].roles = []
        storagerouter_1.disks[0].partitions[0].save()
        storagerouter_1.invalidate_dynamics('partition_config')
        with self.assertRaises(ValueError) as raise_info:
            GenericController.execute_scrub()
        self.assertEqual(first='No scrub locations found, cannot scrub',
                         second=raise_info.exception.message,
                         msg='Incorrect error message caught')
        storagerouter_1.disks[0].partitions[0].roles = [DiskPartition.ROLES.SCRUB]
        storagerouter_1.disks[0].partitions[0].save()
        storagerouter_1.invalidate_dynamics('partition_config')

        failed_vdisks = []
        successful_vdisks = []
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            success = vdisk_id % 2 == 0
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': success,
                                                                         'scrub_work': range(vdisk_id)}
            if success is True:
                successful_vdisks.append(vdisk)
            else:
                failed_vdisks.append(vdisk)

        # Execute scrubbing a 1st time
        with self.assertRaises(Exception) as raise_info:
            GenericController.execute_scrub()
        for vdisk in failed_vdisks:
            self.assertIn(vdisk.name, raise_info.exception.message)

        # Validate expected successful vDisks
        for vdisk in successful_vdisks:
            with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
                self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                                 second=0,
                                 msg='Scrubbed vDisk {0} does still have scrubbing work left'.format(vdisk.name))
        # Validate expected failed vDisks
        for vdisk in failed_vdisks:
            with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
                self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                                 second=int(vdisk.name),
                                 msg='Scrubbed vDisk {0} does not have the expected amount of scrubbing items: {1}'.format(vdisk.name, int(vdisk.name)))

        # Execute scrubbing again
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id]['success'] = True
        GenericController.execute_scrub()
        for vdisk in vdisks.values():
            with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
                self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                                 second=0,
                                 msg='Scrubbed vDisk {0} does still have scrubbing work left after scrubbing a 2nd time'.format(vdisk.name))

        ##############
        # Scenario 3 #
        ##############
        self.volatile.clean()
        self.persistent.clean()
        structure = Helper.build_service_structure(
            {'vpools': [1, 2],  # vPool 2 has no vDisks attached to it
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 1, 1, 1), (7, 1, 1, 1), (8, 1, 1, 1), (9, 1, 1, 1), (10, 1, 1, 1),
                        (11, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vdisks = structure['vdisks']
        # Have 1 volume as a template, scrubbing should not be triggered on it
        vdisk_11 = structure['vdisks'][11]
        vdisk_11.storagedriver_client.set_volume_as_template(volume_id=vdisk_11.volume_id)

        storagerouters = structure['storagerouters']
        System._machine_id = dict((sr.ip, sr.machine_id) for sr in storagerouters.values())
        Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps({}, indent=4), raw=True)

        thread_names = ['scrub_{0}_{1}'.format(vpool.guid, storagerouter.guid) for storagerouter in storagerouters.values()]
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}
        LockedClient.thread_names = thread_names[:]
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        self.assertEqual(first=len(LockedClient.thread_names),
                         second=0,
                         msg='Not all threads have been used in the process')

        ##############
        # Scenario 4 #
        ##############
        self.volatile.clean()
        self.persistent.clean()
        structure = Helper.build_service_structure(
            {'vpools': [1, 2, 3],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 2, 2, 2), (5, 2, 2, 2),
                        (6, 2, 2, 2), (7, 3, 3, 3), (8, 3, 3, 3), (9, 3, 3, 3)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagedrivers': [(1, 1, 1), (2, 2, 1), (3, 3, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpools = structure['vpools']
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']

        thread_names = []
        for vpool in vpools.values():
            Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps({}, indent=4), raw=True)
            for storagerouter in storagerouters.values():
                thread_names.append('scrub_{0}_{1}'.format(vpool.guid, storagerouter.guid))
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names) - 9)}
        LockedClient.thread_names = thread_names[:]
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        self.assertEqual(first=len(LockedClient.thread_names),
                         second=9,  # 5 srs * 3 vps = 15 threads, but only 2 will be spawned per vPool --> 15 - 6 = 9 left
                         msg='Not all threads have been used in the process')

        # 3 vPools will cause the scrubber to only launch 2 threads per vPool --> 1 possible thread should be unused per vPool
        for vpool in vpools.values():
            threads_left = [thread_name for thread_name in LockedClient.thread_names if vpool.guid in thread_name]
            self.assertEqual(first=len(threads_left),
                             second=3,
                             msg='Unexpected amount of threads left for vPool {0}'.format(vpool.name))

    # def arakoon_collapse_test(self):
    #     """
    #     Test the Arakoon collapse functionality
    #     """
    #     structure = Helper.build_service_structure(structure={'storagerouters': [1, 2]})
    #     storagerouter_1 = structure['storagerouters'][1]
    #     storagerouter_2 = structure['storagerouters'][2]
    #     System._machine_id = {storagerouter_1.ip: '1',
    #                           storagerouter_2.ip: '2'}
    #
    #     # Create new cluster
    #     for sr in [storagerouter_1, storagerouter_2]:
    #         Configuration.set('/ovs/framework/hosts/{0}/ports'.format(sr.machine_id), {'arakoon': [int(sr.machine_id) * 10000, int(sr.machine_id) * 10000 + 100]})
    #
    #     clusters_to_create = {ServiceType.ARAKOON_CLUSTER_TYPES.SD: ['voldrv'],
    #                           ServiceType.ARAKOON_CLUSTER_TYPES.CFG: ['cacc'],
    #                           ServiceType.ARAKOON_CLUSTER_TYPES.FWK: ['ovsdb'],
    #                           ServiceType.ARAKOON_CLUSTER_TYPES.ABM: ['abm-1', 'abm-2'],
    #                           ServiceType.ARAKOON_CLUSTER_TYPES.NSM: ['nsm-1_0', 'nsm-1_1', 'nsm-2_0']}
    #     # Make sure we cover all Arakoon cluster types
    #     self.assertEqual(first=sorted(clusters_to_create.keys()),
    #                      second=sorted(ServiceType.ARAKOON_CLUSTER_TYPES.keys()),
    #                      msg='An Arakoon cluster type has been removed or added')
    #
    #     for cluster_type, cluster_names in clusters_to_create.iteritems():
    #         filesystem = cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.CFG
    #         for cluster_name in cluster_names:
    #             base_dir = Helper.CLUSTER_DIR.format(cluster_name)
    #             info = ArakoonInstaller.create_cluster(cluster_name=cluster_name,
    #                                                    cluster_type=cluster_type,
    #                                                    ip=storagerouter_1.ip,
    #                                                    base_dir=base_dir)
    #             ArakoonInstaller.claim_cluster(cluster_name=cluster_name,
    #                                            master_ip=storagerouter_1.ip,
    #                                            filesystem=filesystem,
    #                                            metadata=info['metadata'])
    #             ArakoonInstaller.extend_cluster(master_ip=storagerouter_1.ip,
    #                                             new_ip=storagerouter_2.ip,
    #                                             cluster_name=cluster_name,
    #                                             base_dir=base_dir,
    #                                             filesystem=filesystem)

    ##################
    # HELPER METHODS #
    ##################
    def _print_message(self, message):
        if self.debug is True:
            print message

    def _validate(self, vdisk, current_day, base_date, sticky_hours, consistent_hours, inconsistent_hours):
        """
        This validates assumes the same policy as currently implemented in the policy code
        itself. In case the policy strategy ever changes, this unittest should be adapted as well
        or rewritten to load the implemented policy
        """

        # Implemented policy:
        # < 1d | 1d bucket | 1 | best of bucket   | 1d
        # < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        # < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        # > 1m | delete

        minute = 60
        hour = minute * 60

        self._print_message('  - {0}'.format(vdisk.name))

        # Visualisation
        if self.debug is True:
            snapshots = {}
            for snapshot in vdisk.snapshots:
                snapshots[int(snapshot['timestamp'])] = snapshot
            for day in xrange(0, current_day + 1):
                timestamp = self._make_timestamp(base_date, datetime.timedelta(1) * day)
                visual = '    - {0} '.format(datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'))
                for t in xrange(timestamp, timestamp + hour * 24, minute * 30):
                    if t in snapshots:
                        visual += 'S' if snapshots[t]['is_sticky'] else 'C' if snapshots[t]['is_consistent'] else 'R'
                    else:
                        visual += '-'
                self._print_message(visual)

        sticky = [int(s['timestamp']) for s in vdisk.snapshots if s['is_sticky'] is True]
        consistent = [int(s['timestamp']) for s in vdisk.snapshots if s['is_consistent'] is True]
        inconsistent = [int(s['timestamp']) for s in vdisk.snapshots if s['is_consistent'] is False]
        self._print_message('    - {0} consistent, {1} inconsistent, {2} sticky'.format(len(consistent), len(inconsistent), len(sticky)))

        # Check for correct amount of snapshots
        amount_sticky = len(sticky_hours) * current_day
        amount_consistent = 0
        amount_inconsistent = 0
        pointer = 0
        if pointer < current_day:
            amount_consistent += len(consistent_hours)
            amount_inconsistent += len(inconsistent_hours)
            pointer += 1
        while pointer < current_day and pointer <= 7:
            if len(consistent_hours) > 0:
                amount_consistent += 1  # One consistent snapshot per day
            else:
                amount_inconsistent += 1
            pointer += 1
        while pointer < current_day and pointer <= 28:
            if len(consistent_hours) > 0:
                amount_consistent += 1  # One consistent snapshot per week
            else:
                amount_inconsistent += 1
            pointer += 7
        self.assertEqual(first=len(sticky),
                         second=amount_sticky,
                         msg='Wrong amount of sticky snapshots: {0} vs expected {1}'.format(len(sticky), amount_sticky))
        if len(sticky) == 0:
            self.assertEqual(first=len(consistent),
                             second=amount_consistent,
                             msg='Wrong amount of consistent snapshots: {0} vs expected {1}'.format(len(consistent), amount_consistent))
            self.assertEqual(first=len(inconsistent),
                             second=amount_inconsistent,
                             msg='Wrong amount of inconsistent snapshots: {0} vs expected {1}'.format(len(inconsistent), amount_inconsistent))

        # Check of the correctness of the snapshot timestamp
        if len(consistent_hours) > 0:
            sn_type = 'consistent'
            container = consistent
            time_diff = (hour * consistent_hours[-1]) + (minute * 30)
        else:
            sn_type = 'inconsistent'
            container = inconsistent
            time_diff = (hour * inconsistent_hours[-1])

        for day in xrange(0, current_day):
            for h in sticky_hours:
                timestamp = self._make_timestamp(base_date, datetime.timedelta(1) * day) + (hour * h) + (minute * 30)
                self.assertIn(member=timestamp,
                              container=sticky,
                              msg='Expected sticky snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
            if day == (current_day - 1):
                for h in inconsistent_hours:
                    timestamp = self._make_timestamp(base_date, datetime.timedelta(1) * day) + (hour * h)
                    self.assertIn(member=timestamp,
                                  container=inconsistent,
                                  msg='Expected hourly inconsistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
                for h in consistent_hours:
                    timestamp = self._make_timestamp(base_date, datetime.timedelta(1) * day) + (hour * h) + (minute * 30)
                    self.assertIn(member=timestamp,
                                  container=consistent,
                                  msg='Expected random consistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
            elif day > (current_day - 7):
                timestamp = self._make_timestamp(base_date, datetime.timedelta(1) * day) + time_diff
                self.assertIn(member=timestamp,
                              container=container,
                              msg='Expected daily {0} snapshot for {1} at {2}'.format(sn_type, vdisk.name, self._from_timestamp(timestamp)))
            elif day % 7 == 0 and day > 28:
                timestamp = self._make_timestamp(base_date, datetime.timedelta(1) * day) + time_diff
                self.assertIn(member=timestamp,
                              container=container,
                              msg='Expected weekly {0} snapshot for {1} at {2}'.format(sn_type, vdisk.name, self._from_timestamp(timestamp)))

    @staticmethod
    def _make_timestamp(base, offset):
        return int(time.mktime((base + offset).timetuple()))

    @staticmethod
    def _from_timestamp(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

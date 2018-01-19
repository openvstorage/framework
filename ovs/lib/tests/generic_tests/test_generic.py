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
import re
import time
import datetime
import unittest
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs_extensions.generic.tests.sshclient_mock import MockedSSHClient
from ovs_extensions.generic.threadhelpers import Waiter
from ovs.extensions.storageserver.tests.mockups import LockedClient
from ovs.lib.generic import GenericController
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.vdisk import VDiskController
from ovs.log.log_handler import LogHandler


class Generic(unittest.TestCase):
    """
    This test class will validate the various scenarios of the Generic logic
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.volatile, self.persistent = DalHelper.setup()

    def tearDown(self):
        """
        Clean up test suite
        """
        DalHelper.teardown()

    def test_snapshot_all_vdisks(self):
        """
        Tests GenericController.snapshot_all_vdisks functionality
        """
        structure = DalHelper.build_dal_structure(
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
        self.assertEqual(first=len(vdisk_1.snapshot_ids), second=1, msg='Expected 1 snapshot ID for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_2.snapshot_ids), second=1, msg='Expected 1 snapshot ID for vDisk {0}'.format(vdisk_2.name))
        self.assertEqual(first=len(vdisk_1.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_2.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_2.name))

        # Ensure automatic snapshot fails for vdisk_1 and succeeds for vdisk_2
        vdisk_1.storagedriver_client._set_snapshot_in_backend(volume_id=vdisk_1.volume_id, snapshot_id=vdisk_1.snapshots[0]['guid'], in_backend=False)
        success, fail = GenericController.snapshot_all_vdisks()
        self.assertEqual(first=len(fail), second=1, msg='Expected 1 failed snapshot')
        self.assertEqual(first=fail[0], second=vdisk_1.guid, msg='Expected vDisk {0} to have failed'.format(vdisk_1.name))
        self.assertEqual(first=len(success), second=1, msg='Expected 1 successful snapshot')
        self.assertEqual(first=success[0], second=vdisk_2.guid, msg='Expected vDisk {0} to have succeeded'.format(vdisk_2.name))
        self.assertEqual(first=len(vdisk_1.snapshot_ids), second=1, msg='Expected 1 snapshot ID for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_2.snapshot_ids), second=2, msg='Expected 2 snapshot IDs for vDisk {0}'.format(vdisk_2.name))
        self.assertEqual(first=len(vdisk_1.snapshots), second=1, msg='Expected 1 snapshot for vDisk {0}'.format(vdisk_1.name))
        self.assertEqual(first=len(vdisk_2.snapshots), second=2, msg='Expected 2 snapshots for vDisk {0}'.format(vdisk_2.name))

    def test_clone_snapshot(self):
        """
        Validates that a snapshot that has clones will not be deleted while other snapshots will be deleted
        """
        # Setup
        # There are 2 disks, second one cloned from a snapshot of the first
        structure = DalHelper.build_dal_structure(
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

        structure = DalHelper.build_dal_structure(structure={'vdisks': [(2, 1, 1, 1)]},
                                                  previous_structure=structure)
        clone_vdisk = structure['vdisks'][2]
        base_snapshot_guid = vdisk_1.snapshot_ids[0]  # Oldest
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
        self.assertIn(base_snapshot_guid, vdisk_1.snapshot_ids, 'Snapshot was deleted while there are still clones of it')

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
            structure = DalHelper.build_dal_structure(
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
            self.persistent._clean()
            self.volatile._clean()

    def test_happypath(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistent
        every now and then. The delete policy is executed every day
        """
        structure = DalHelper.build_dal_structure(
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
        * Scenario 3: 1 vPool, 11 vDisks, 5 scrub roles (4 StorageRouters, one of then has 2 scrub roles)
                      Check template vDisk is NOT scrubbed
                      Check if vDisks are divided among all threads
        * Scenario 4: 3 vPools, 15 vDisks, 5 scrub roles
                      Validate 12 threads will be spawned and used out of a potential of 30 (5 scrub roles * 3 vPools * 2 threads per StorageRouter)
                      We limit max amount of threads spawned per vPool to 2 in case 3 to 5 vPools are present
        * Scenario 5: 2 vPools, 8 vDisks, 2 scrub roles
                      Validate correct vDisks are scrubbed on expected location when specifying vpool_guids and/or vdisk_guids
        * Scenario 6: Configure amount of threads per StorageRouter to 5 for SR1
                      2 vPools, 20 vDisks, 2 scrub roles
                      Validate expected amount of threads is spawned
        """
        ##############
        # Scenario 1 #
        ##############
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk = structure['vdisks'][1]
        storagerouter = structure['storagerouters'][1]
        LockedClient.scrub_controller = {'possible_threads': None,
                                         'volumes': {},
                                         'waiter': Waiter(1)}
        LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': False,
                                                                     'scrub_work': [0]}

        # Remove SCRUB partition from StorageRouter and try to scrub on it
        expected_log = 'Scrubber - Storage Router {0} is not reachable'.format(storagerouter.ip)
        storagerouter.disks[0].partitions[0].roles = []
        storagerouter.disks[0].partitions[0].save()
        with self.assertRaises(ValueError) as raise_info:
            GenericController.execute_scrub(vdisk_guids=[vdisk.guid], manual=True)
        self.assertIn(member='No scrub locations found',
                      container=raise_info.exception.message)
        self.assertNotIn(member=expected_log,
                         container=LogHandler._logs['lib_generic tasks'])

        # Restore SCRUB partition and make sure StorageRouter is unreachable
        storagerouter.disks[0].partitions[0].roles = [DiskPartition.ROLES.SCRUB]
        storagerouter.disks[0].partitions[0].save()
        storagerouter.invalidate_dynamics('partition_config')
        SSHClient._raise_exceptions[storagerouter.ip] = {'users': ['root'], 'exception': UnableToConnectException('No route to host')}
        with self.assertRaises(ValueError):
            GenericController.execute_scrub(vdisk_guids=[vdisk.guid], manual=True)
        logs = LogHandler._logs['lib_generic tasks']
        self.assertIn(member=expected_log,
                      container=logs)
        self.assertEqual(first=logs[expected_log],
                         second='warning')

        # Now actually attempt to scrub
        SSHClient._raise_exceptions = {}
        with self.assertRaises(Exception) as raise_info:
            GenericController.execute_scrub(vdisk_guids=[vdisk.guid], storagerouter_guid=storagerouter.guid, manual=True)
        self.assertIn(member='StorageRouter {0} - vDisk {1} - Scrubbing failed'.format(storagerouter.name, vdisk.name),
                      container=raise_info.exception.message)

        # Make sure scrubbing succeeds now
        LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                     'scrub_work': [0]}
        GenericController.execute_scrub(vdisk_guids=[vdisk.guid], storagerouter_guid=storagerouter.guid, manual=True)
        with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
            self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                             second=0,
                             msg='Scrubbed vDisk {0} does not have the expected amount of scrubbing items: {1}'.format(vdisk.name, 0))

        ##############
        # Scenario 2 #
        ##############
        self.volatile._clean()
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 1, 1, 1), (7, 1, 1, 1), (8, 1, 1, 1), (9, 1, 1, 1), (10, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vdisks = structure['vdisks']
        storagerouter_1 = structure['storagerouters'][1]
        storagerouter_2 = structure['storagerouters'][2]
        LockedClient.scrub_controller = {'possible_threads': ['execute_scrub_{0}_{1}_0'.format(vpool.guid, storagerouter_1.disks[0].partitions[0].guid),
                                                              'execute_scrub_{0}_{1}_1'.format(vpool.guid, storagerouter_1.disks[0].partitions[0].guid)],
                                         'volumes': {},
                                         'waiter': Waiter(1)}

        # Have 1 StorageRouter with 0 SCRUB partitions
        storagerouter_2.disks[0].partitions[0].roles = []
        storagerouter_2.disks[0].partitions[0].save()

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
        self.volatile._clean()
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],  # vPool 2 has no vDisks attached to it
             'vdisks': [(i, 1, 1, 1) for i in xrange(1, 12)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vdisks = structure['vdisks']
        storagerouter_1 = structure['storagerouters'][1]
        # Have 1 volume as a template, scrubbing should not be triggered on it
        vdisk_t = structure['vdisks'][11]
        vdisk_t.storagedriver_client.set_volume_as_template(volume_id=vdisk_t.volume_id)

        # Have 1 StorageRouter with multiple SCRUB partitions
        partition = DiskPartition()
        partition.offset = 0
        partition.size = storagerouter_1.disks[0].size
        partition.aliases = ['/dev/uda-2']
        partition.state = 'OK'
        partition.mountpoint = '/tmp/unittest/sr_1/disk_1/partition_2'
        partition.disk = storagerouter_1.disks[0]
        partition.roles = [DiskPartition.ROLES.SCRUB]
        partition.save()

        thread_names = []
        for storagerouter in structure['storagerouters'].values():
            for partition in [p for disk in storagerouter.disks for p in disk.partitions]:
                for index in range(2):
                    thread_names.append('execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition.guid, index))
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}
        LockedClient.thread_names = thread_names[:]
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        # Verify all threads have been 'consumed'
        self.assertEqual(first=len(LockedClient.thread_names),
                         second=0)
        self.assertIn(member='Scrubber - vPool {0} - vDisk {1} {2} - Is a template, not scrubbing'.format(vpool.name, vdisk_t.guid, vdisk_t.name),
                      container=LogHandler._logs['lib_generic tasks'])

        ##############
        # Scenario 4 #
        ##############
        self.volatile._clean()
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2, 3],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 2, 2), (7, 2, 2, 2), (8, 2, 2, 2), (9, 2, 2, 2), (10, 2, 2, 2),
                        (11, 3, 3, 3), (12, 3, 3, 3), (13, 3, 3, 3), (14, 3, 3, 3), (15, 3, 3, 3)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagedrivers': [(1, 1, 1), (2, 2, 1), (3, 3, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpools = structure['vpools']
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']

        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 3 vPools and 5 StorageRouters
        #   - Amount of threads that will be created: 2 * 3 * 2 = 12
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 3 * 5 * 2 = 30
        thread_names = []
        for vpool in vpools.values():
            for storagerouter in storagerouters.values():
                for partition in storagerouter.disks[0].partitions:
                    for index in range(2):
                        thread_names.append('execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition.guid, index))
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(12)}
        LockedClient.thread_names = thread_names[:]
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        self.assertEqual(first=len(LockedClient.thread_names),
                         second=18,  # 30 possible thread_names - 12 which should be created and consumed
                         msg='Not all threads have been used in the process')

        # Of the 18 threads which have been created and consumed, 6 should have been created for each vPool
        for vpool in vpools.values():
            threads_left = [thread_name for thread_name in LockedClient.thread_names if vpool.guid in thread_name]
            self.assertEqual(first=len(threads_left),
                             second=6,
                             msg='Unexpected amount of threads left for vPool {0}'.format(vpool.name))

        ##############
        # Scenario 5 #
        ##############
        self.volatile._clean()
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 2, 1, 2), (3, 3, 2, 3), (4, 4, 2, 4),
                        (5, 1, 1, 1), (6, 2, 1, 2), (7, 3, 2, 3), (8, 4, 2, 4)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpools = structure['vpools']
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']

        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        thread_names = []
        for vpool in vpools.values():
            for storagerouter in storagerouters.values():
                for partition in storagerouter.disks[0].partitions:
                    for index in range(2):
                        thread_names.append('execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition.guid, index))
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        for vdisk in vdisks.values():
            self.assertListEqual(list1=LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'],
                                 list2=[])

        # Scrub all volumes of vPool1
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'] = range(vdisk_id)
        GenericController.execute_scrub(vpool_guids=[vpools[1].guid], manual=True)
        for vdisk_id, vdisk in vdisks.iteritems():
            if vdisk.vpool == vpools[1]:
                expected_work = []
            else:
                expected_work = range(vdisk_id)
            self.assertListEqual(list1=LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'],
                                 list2=expected_work)

        # Scrub a specific vDisk
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'] = range(vdisk_id)
        GenericController.execute_scrub(vdisk_guids=[vdisks[2].guid], manual=True)
        for vdisk_id, vdisk in vdisks.iteritems():
            if vdisk == vdisks[2]:
                expected_work = []
            else:
                expected_work = range(vdisk_id)
            self.assertListEqual(list1=LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'],
                                 list2=expected_work)

        # Scrub a combination of a vPool and a vDisk
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'] = range(vdisk_id)
        GenericController.execute_scrub(vpool_guids=[vpools[2].guid], vdisk_guids=[vdisks[2].guid], manual=True)
        for vdisk_id, vdisk in vdisks.iteritems():
            if vdisk == vdisks[2] or vdisk.vpool == vpools[2]:
                expected_work = []
            else:
                expected_work = range(vdisk_id)
            self.assertListEqual(list1=LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'],
                                 list2=expected_work)

        # Scrub all volumes on specific StorageRouter
        LogHandler._logs = {}
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'] = range(vdisk_id)
        GenericController.execute_scrub(storagerouter_guid=storagerouters[2].guid)
        for vdisk_id, vdisk in vdisks.iteritems():
            self.assertListEqual(list1=LockedClient.scrub_controller['volumes'][vdisk.volume_id]['scrub_work'],
                                 list2=[])
        logs = LogHandler._logs['lib_generic tasks']
        for log in logs:
            self.assertNotRegexpMatches(text=log,
                                        unexpected_regexp='.*Scrubber - vPool [{0}|{1}] - StorageRouter {2} - .*'.format(vpools[1].name, vpools[2].name, storagerouters[1].name))

        ##############
        # Scenario 6 #
        ##############
        self.volatile._clean()
        self.persistent._clean()
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpools = structure['vpools']
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']

        # Set amount of stack threads for SR1 to 5 and leave for SR2 to default (2)
        sr_1_threads = 5
        sr_2_threads = 2
        Configuration.set(key='/ovs/framework/hosts/{0}/config|scrub_stack_threads'.format(storagerouters[1].machine_id), value=sr_1_threads)
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * <scrub_stack_threads> threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created:  2 * 2 * (2 + 5) / 2 = 14
        #       - For StorageRouter 1: 10
        #       - For StorageRouter 2: 4
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * <scrub_stack_threads> threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * (2 + 5) / 2 = 14
        thread_names = []
        for vpool in vpools.values():
            for storagerouter in storagerouters.values():
                stack_threads = sr_1_threads if storagerouter == storagerouters[1] else sr_2_threads
                for partition in storagerouter.disks[0].partitions:
                    for index in range(stack_threads):
                        thread_names.append('execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition.guid, index))
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(14)}
        LockedClient.thread_names = thread_names[:]
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        LogHandler._logs = {}
        GenericController.execute_scrub()
        # Verify all threads have been 'consumed'
        self.assertEqual(first=len(LockedClient.thread_names),
                         second=0)
        counter = 0
        for log in LogHandler._logs['lib_generic tasks']:
            if 'threads for proxy service' in log:
                match = re.match('^Scrubber - vPool [1|2] - StorageRouter ([1|2]) - .*ovs-albaproxy_.*_scrub', log)
                self.assertIsNotNone(match)
                if match.groups()[0] == storagerouters[1].name:
                    expected_threads = 5
                else:
                    expected_threads = 2
                self.assertIn(member='Spawning {0} threads for proxy'.format(expected_threads),
                              container=log)
                counter += 1
        self.assertEqual(first=4,  # Log entry for each combination of 2 vPools and 2 StorageRouters
                         second=counter)
        # @todo create scrubbing tests which test concurrency

    def test_arakoon_collapse(self):
        """
        Test the Arakoon collapse functionality
        """
        # Set up the test
        structure = DalHelper.build_dal_structure(structure={'storagerouters': [1, 2]})
        storagerouter_1 = structure['storagerouters'][1]
        storagerouter_2 = structure['storagerouters'][2]
        MockedSSHClient._run_returns[storagerouter_1.ip] = {}
        MockedSSHClient._run_returns[storagerouter_2.ip] = {}

        # Make sure we cover all Arakoon cluster types
        clusters_to_create = {ServiceType.ARAKOON_CLUSTER_TYPES.SD: [{'name': 'unittest-voldrv', 'internal': True, 'success': True}],
                              ServiceType.ARAKOON_CLUSTER_TYPES.CFG: [{'name': 'unittest-cacc', 'internal': True, 'success': True}],
                              ServiceType.ARAKOON_CLUSTER_TYPES.FWK: [{'name': 'unittest-ovsdb', 'internal': True, 'success': False}],
                              ServiceType.ARAKOON_CLUSTER_TYPES.ABM: [{'name': 'unittest-cluster-1-abm', 'internal': True, 'success': False},
                                                                      {'name': 'unittest-random-abm-name', 'internal': False, 'success': True}],
                              ServiceType.ARAKOON_CLUSTER_TYPES.NSM: [{'name': 'unittest-cluster-1-nsm_0', 'internal': True, 'success': True}]}
        self.assertEqual(first=sorted(clusters_to_create.keys()),
                         second=sorted(ServiceType.ARAKOON_CLUSTER_TYPES.keys()),
                         msg='An Arakoon cluster type has been removed or added, please update this test accordingly')

        # Create all Arakoon clusters and related services
        failed_clusters = []
        external_clusters = []
        successful_clusters = []
        for cluster_type, cluster_infos in clusters_to_create.iteritems():
            filesystem = cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.CFG
            for cluster_info in cluster_infos:
                internal = cluster_info['internal']
                cluster_name = cluster_info['name']

                base_dir = DalHelper.CLUSTER_DIR.format(cluster_name)
                arakoon_installer = ArakoonInstaller(cluster_name=cluster_name)
                arakoon_installer.create_cluster(cluster_type=cluster_type,
                                                 ip=storagerouter_1.ip,
                                                 base_dir=base_dir,
                                                 internal=internal,
                                                 log_sinks=LogHandler.get_sink_path('arakoon-server_{0}'.format(cluster_name)),
                                                 crash_log_sinks=LogHandler.get_sink_path('arakoon-server-crash_{0}'.format(cluster_name)))
                arakoon_installer.start_cluster()
                arakoon_installer.extend_cluster(new_ip=storagerouter_2.ip,
                                                 base_dir=base_dir,
                                                 log_sinks=LogHandler.get_sink_path('arakoon-server_{0}'.format(cluster_name)),
                                                 crash_log_sinks=LogHandler.get_sink_path('arakoon-server-crash_{0}'.format(cluster_name)))

                service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
                if cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.ABM:
                    service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ALBA_MGR)
                elif cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.NSM:
                    service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.NS_MGR)
                else:
                    service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)

                if internal is True:
                    DalHelper.create_service(service_name=service_name,
                                             service_type=service_type,
                                             storagerouter=storagerouter_1,
                                             ports=arakoon_installer.ports[storagerouter_1.ip])
                    DalHelper.create_service(service_name=service_name,
                                             service_type=service_type,
                                             storagerouter=storagerouter_2,
                                             ports=arakoon_installer.ports[storagerouter_2.ip])
                else:
                    DalHelper.create_service(service_name=service_name,
                                             service_type=service_type)

                    external_clusters.append(cluster_name)
                    continue

                if cluster_info['success'] is True:
                    if filesystem is True:
                        config_path = ArakoonClusterConfig.CONFIG_FILE.format(cluster_name)
                    else:
                        config_path = Configuration.get_configuration_path(ArakoonClusterConfig.CONFIG_KEY.format(cluster_name))
                    MockedSSHClient._run_returns[storagerouter_1.ip]['arakoon --collapse-local 1 2 -config {0}'.format(config_path)] = None
                    MockedSSHClient._run_returns[storagerouter_2.ip]['arakoon --collapse-local 2 2 -config {0}'.format(config_path)] = None
                    successful_clusters.append(cluster_name)
                else:  # For successful False clusters we don't emulate the collapse, thus making it fail
                    failed_clusters.append(cluster_name)

        # Start collapse and make it fail for all clusters on StorageRouter 2
        SSHClient._raise_exceptions[storagerouter_2.ip] = {'users': ['ovs'],
                                                           'exception': UnableToConnectException('No route to host')}
        GenericController.collapse_arakoon()

        # Verify all log messages for each type of cluster
        generic_logs = LogHandler._logs.get('lib_generic tasks', {})
        for cluster_name in successful_clusters + failed_clusters + external_clusters:
            collect_msg = ('debug', 'Collecting info for cluster {0}'.format(cluster_name))
            unreachable_msg = ('error', 'Could not collapse any cluster on {0} (not reachable)'.format(storagerouter_2.name))
            end_collapse_msg = ('debug', 'Collapsing cluster {0} on {1} completed'.format(cluster_name, storagerouter_1.ip))
            start_collapse_msg = ('debug', 'Collapsing cluster {0} on {1}'.format(cluster_name, storagerouter_1.ip))
            failed_collapse_msg = ('exception', 'Collapsing cluster {0} on {1} failed'.format(cluster_name, storagerouter_1.ip))
            messages_to_validate = []
            if cluster_name in successful_clusters:
                assert_function = self.assertIn
                messages_to_validate.append(collect_msg)
                messages_to_validate.append(unreachable_msg)
                messages_to_validate.append(start_collapse_msg)
                messages_to_validate.append(end_collapse_msg)
            elif cluster_name in failed_clusters:
                assert_function = self.assertIn
                messages_to_validate.append(collect_msg)
                messages_to_validate.append(unreachable_msg)
                messages_to_validate.append(start_collapse_msg)
                messages_to_validate.append(failed_collapse_msg)
            else:
                assert_function = self.assertNotIn
                messages_to_validate.append(collect_msg)
                messages_to_validate.append(start_collapse_msg)
                messages_to_validate.append(end_collapse_msg)

            for severity, message in messages_to_validate:
                if assert_function == self.assertIn:
                    assert_message = 'Expected to find log message: {0}'.format(message)
                else:
                    assert_message = 'Did not expect to find log message: {0}'.format(message)
                assert_function(member=message,
                                container=generic_logs,
                                msg=assert_message)
                if assert_function == self.assertIn:
                    self.assertEqual(first=severity,
                                     second=generic_logs[message],
                                     msg='Log message {0} is of severity {1} expected {2}'.format(message, generic_logs[message], severity))

        # Collapse should always have a 'finished' message since each cluster should be attempted to be collapsed
        for general_message in ['Arakoon collapse started', 'Arakoon collapse finished']:
            self.assertIn(member=general_message,
                          container=generic_logs,
                          msg='Expected to find log message: {0}'.format(general_message))

    def test_refresh_package_information(self):
        """
        Test the refresh package information functionality
        """
        def _multi_1(client, information):
            information[client.ip]['component1'] = {'package1': {'candidate': 'version2',
                                                                 'installed': 'version1',
                                                                 'services_to_restart': []}}

        def _multi_2(client, information):
            information[client.ip]['component2'] = {'package2': {'candidate': 'version2',
                                                                 'installed': 'version1',
                                                                 'services_to_restart': []}}
            if client.ip == storagerouter_3.ip:
                information[client.ip]['errors'] = ['Unexpected error occurred for StorageRouter {0}'.format(storagerouter_3.name)]

        def _single_1(information):
            _ = information  # get_package_info_single is used for Alba nodes, so not testing here

        expected_package_info = {'component1': {'package1': {'candidate': 'version2',
                                                             'installed': 'version1',
                                                             'services_to_restart': []}},
                                 'component2': {'package2': {'candidate': 'version2',
                                                             'installed': 'version1',
                                                             'services_to_restart': []}}}

        # StorageRouter 1 successfully updates its package info
        # StorageRouter 2 is inaccessible
        # StorageRouter 3 gets error in 2nd hook --> package_information is reset to {}
        structure = DalHelper.build_dal_structure(structure={'storagerouters': [1, 2, 3]})
        storagerouter_1 = structure['storagerouters'][1]
        storagerouter_2 = structure['storagerouters'][2]
        storagerouter_3 = structure['storagerouters'][3]
        Toolbox._function_pointers['update-get_package_info_multi'] = [_multi_1, _multi_2]
        Toolbox._function_pointers['update-get_package_info_single'] = [_single_1]

        SSHClient._raise_exceptions[storagerouter_2.ip] = {'users': ['root'],
                                                           'exception': UnableToConnectException('No route to host')}

        with self.assertRaises(excClass=Exception) as raise_info:
            GenericController.refresh_package_information()

        storagerouter_1.discard()
        self.assertDictEqual(d1=expected_package_info,
                             d2=storagerouter_1.package_information,
                             msg='Incorrect package information found for StorageRouter 1'.format(storagerouter_1.name))
        self.assertDictEqual(d1={},
                             d2=storagerouter_2.package_information,
                             msg='Incorrect package information found for StorageRouter 2'.format(storagerouter_2.name))
        self.assertDictEqual(d1={},
                             d2=storagerouter_3.package_information,
                             msg='Incorrect package information found for StorageRouter {0}'.format(storagerouter_3.name))
        self.assertIn(member='StorageRouter {0} is inaccessible'.format(storagerouter_2.name),
                      container=raise_info.exception.message,
                      msg='Expected to find log message about StorageRouter {0} being inaccessible'.format(storagerouter_2.name))
        self.assertIn(member='Unexpected error occurred for StorageRouter {0}'.format(storagerouter_3.name),
                      container=raise_info.exception.message,
                      msg='Expected to find log message about unexpected error for StorageRouter {0}'.format(storagerouter_3.name))

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

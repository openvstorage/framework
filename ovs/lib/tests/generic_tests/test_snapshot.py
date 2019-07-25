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
import time
import datetime
import unittest
from ovs_extensions.constants import is_running_on_travis
from ovs.constants.vdisk import SCRUB_VDISK_EXCEPTION_MESSAGE
from ovs.dal.tests.helpers import DalHelper
from ovs.lib.generic import GenericController
from ovs.lib.vdisk import VDiskController
from ovs.extensions.storageserver.tests.mockups import StorageRouterClient


class SnapshotTestCase(unittest.TestCase):
    """
    This test class will validate the various scenarios of the Generic logic
    Actual snapshot logic is tested in the vdisk_tests.test_snapshot
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        StorageRouterClient.delete_snapshot_callbacks = {}
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
        storagedriver_1 = structure['storagedrivers'][1]
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
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))
        self.assertIn(base_snapshot_guid, vdisk_1.snapshot_ids, 'Snapshot was deleted while there are still clones of it')

    def test_snapshot_automatic_consistent(self):
        """
        is_automatic: True, is_consistent: True --> Automatically created consistent snapshots should be deleted
        """
        minute = 60
        hour = minute * 60
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        base = datetime.datetime.now().date()
        vdisk_1 = structure['vdisks'][1]
        storagedriver_1 = structure['storagedrivers'][1]

        label = 'c'
        # Extra time to add to the hourly timestamps
        additional_time = minute * 30
        # Hours to create a snapshot on
        sticky_hours = []
        consistent_hours = [2]
        inconsistent_hours = []
        # Snapshot details
        is_sticky = len(sticky_hours) > 0
        is_consistent = len(consistent_hours) > 0
        is_automatic = True

        for day in xrange(35):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))

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
                                                          'is_automatic': is_automatic,
                                                          'is_consistent': is_consistent})

    def test_snapshot_automatic_not_consistent(self):
        """
        is_automatic: True, is_consistent: False --> Automatically created non-consistent snapshots should be deleted
        """
        minute = 60
        hour = minute * 60
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        base = datetime.datetime.now().date()
        vdisk_1 = structure['vdisks'][1]
        storagedriver_1 = structure['storagedrivers'][1]

        label = 'i'
        # Extra time to add to the hourly timestamps
        additional_time = 0
        # Hours to create a snapshot on
        sticky_hours = []
        consistent_hours = []
        inconsistent_hours = [2]
        # Snapshot details
        is_sticky = len(sticky_hours) > 0
        is_consistent = len(consistent_hours) > 0
        is_automatic = True

        for day in xrange(35):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))

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
                                                          'is_automatic': is_automatic,
                                                          'is_consistent': is_consistent})

    def test_snapshot_non_automatic_consistent(self):
        """
        is_automatic: False, is_consistent: True --> Manually created consistent snapshots should be deleted
        """
        minute = 60
        hour = minute * 60
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        base = datetime.datetime.now().date()
        vdisk_1 = structure['vdisks'][1]
        storagedriver_1 = structure['storagedrivers'][1]

        label = 'c'
        # Extra time to add to the hourly timestamps
        additional_time = minute * 30
        # Hours to create a snapshot on
        sticky_hours = []
        consistent_hours = [2]
        inconsistent_hours = []
        # Snapshot details
        is_sticky = len(sticky_hours) > 0
        is_consistent = len(consistent_hours) > 0
        is_automatic = False

        for day in xrange(35):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))

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
                                                          'is_automatic': is_automatic,
                                                          'is_consistent': is_consistent})

    def test_snapshot_not_automatic_not_consistent(self):
        """
        is_automatic: False, is_consistent: False --> Manually created non-consistent snapshots should be deleted
        """
        minute = 60
        hour = minute * 60
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        base = datetime.datetime.now().date()
        vdisk_1 = structure['vdisks'][1]
        storagedriver_1 = structure['storagedrivers'][1]

        label = 'i'
        # Extra time to add to the hourly timestamps
        additional_time = 0
        # Hours to create a snapshot on
        sticky_hours = []
        consistent_hours = []
        inconsistent_hours = [2]
        # Snapshot details
        is_sticky = len(sticky_hours) > 0
        is_consistent = len(consistent_hours) > 0
        is_automatic = False

        for day in xrange(35):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))

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
                                                          'is_automatic': is_automatic,
                                                          'is_consistent': is_consistent})

    def test_snapshot_sticky(self):
        """
        is_sticky: True --> Sticky snapshots of any kind should never be deleted (Only possible to delete manually)
        """
        minute = 60
        hour = minute * 60
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        base = datetime.datetime.now().date()
        vdisk_1 = structure['vdisks'][1]
        storagedriver_1 = structure['storagedrivers'][1]

        label = 'c'
        # Extra time to add to the hourly timestamps
        additional_time = minute * 30
        # Hours to create a snapshot on
        sticky_hours = [2]
        consistent_hours = [2]
        inconsistent_hours = []
        # Snapshot details
        is_sticky = len(sticky_hours) > 0
        is_consistent = len(consistent_hours) > 0
        is_automatic = False

        for day in xrange(35):
            base_timestamp = self._make_timestamp(base, datetime.timedelta(1) * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))

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
                                                          'is_automatic': is_automatic,
                                                          'is_consistent': is_consistent})

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
        storagedriver_1 = structure['storagedrivers'][1]
        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        # Run the testing scenario
        if is_running_on_travis():
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
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid,
                                                             timestamp=base_timestamp + (minute * 30))

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

    def test_exception_handling(self):
        """
        Test if the scheduled job can handle exceptions
        """
        def raise_an_exception(*args, **kwargs):
            raise RuntimeError('Emulated snapshot delete error')

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )

        vdisk_1, vdisk_2 = structure['vdisks'].values()
        storagedriver_1 = structure['storagedrivers'][1]

        vdisks = [vdisk_1, vdisk_2]

        for vdisk in vdisks:
            [dynamic for dynamic in vdisk._dynamics if dynamic.name == 'snapshots'][0].timeout = 0
            for i in xrange(0, 2):
                metadata = {'label': str(i),
                            'is_consistent': False,
                            'is_sticky': False,
                            'timestamp': str((int(time.time() - datetime.timedelta(2).total_seconds() - i)))}
                snapshot_id = VDiskController.create_snapshot(vdisk.guid, metadata)
                if vdisk == vdisk_1:
                    StorageRouterClient.delete_snapshot_callbacks[vdisk.volume_id] = {snapshot_id: raise_an_exception}
        with self.assertRaises(RuntimeError):
            GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid)
        self.assertEqual(1, len(vdisk_2.snapshot_ids), 'One snapshot should be removed for vdisk 2')
        self.assertEqual(2, len(vdisk_1.snapshot_ids), 'No snapshots should be removed for vdisk 1')

    def test_scrubbing_exception_handling(self):
        """
        Test if the scheduled job can handle scrub related exceptions
        """
        def raise_an_exception(*args, **kwargs):
            raise RuntimeError(SCRUB_VDISK_EXCEPTION_MESSAGE)

        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk_1 = structure['vdisks'][1]
        storagedriver_1 = structure['storagedrivers'][1]

        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        for i in xrange(0, 2):
            metadata = {'label': str(i),
                        'is_consistent': False,
                        'is_sticky': False,
                        'timestamp': str((int(time.time() - datetime.timedelta(2).total_seconds() - i)))}
            snapshot_id = VDiskController.create_snapshot(vdisk_1.guid, metadata)
            StorageRouterClient.delete_snapshot_callbacks[vdisk_1.volume_id] = {snapshot_id: raise_an_exception}

        GenericController.delete_snapshots_storagedriver(storagedriver_guid=storagedriver_1.guid)
        self.assertEqual(2, len(vdisk_1.snapshot_ids), 'No snapshots should be removed for vdisk 1')

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

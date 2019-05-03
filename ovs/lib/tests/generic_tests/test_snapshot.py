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
from ovs.constants.vdisk import SNAPSHOT_POLICY_LOCATION, SCRUB_VDISK_EXCEPTION_MESSAGE
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.tests.mockups import StorageRouterClient
from ovs.lib.generic import GenericController
from ovs.lib.vdisk import VDiskController
from ovs.lib.helpers.generic.snapshots import SnapshotManager, RetentionPolicy

MINUTE = 60
HOUR = MINUTE * 60
DAY = datetime.timedelta(1)


class SnapshotTestCase(unittest.TestCase):
    """
    Test the scheduling of snapshot creation and the enforced retention policy
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
        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        base = datetime.datetime.now().date()
        base_timestamp = self._make_timestamp(base, DAY)
        for h in [6, 12, 18]:
            timestamp = base_timestamp + (HOUR * h)
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
            base_timestamp = self._make_timestamp(base, DAY * day)
            for h in [6, 12, 18]:
                timestamp = base_timestamp + (HOUR * h)
                VDiskController.create_snapshot(vdisk_guid=clone_vdisk.guid,
                                                metadata={'label': 'snapshot_{0}:30'.format(str(h)),
                                                          'is_consistent': True,
                                                          'timestamp': str(timestamp)})
            base_timestamp = self._make_timestamp(base, DAY * 2)
            GenericController.delete_snapshots(timestamp=base_timestamp + (MINUTE * 30))
        self.assertIn(base_snapshot_guid, vdisk_1.snapshot_ids, 'Snapshot was deleted while there are still clones of it')

    @staticmethod
    def _build_vdisk():
        # type: () -> VDisk
        """
        Build the DAL structure and retrieve the vdisk
        :return: VDisk object
        :rtype: VDisk
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        return structure['vdisks'][1]

    def _create_validate_snapshots(self, vdisk, start_time, sticky_hours, consistent_hours, inconsistent_hours,
                                   snapshot_time_offset=0, automatic_snapshots=True, number_of_days=35):
        # type: (VDisk, datetime.date, List[int], List[int], List[int], int, bool, int) -> None
        """
        Create and validate snapshot creation and deletion sequence
        This is suitable to enforce the default policy which is:
        < 1d | 1d bucket | 1 | best of bucket   | 1d
        < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        > 1m | delete
        :param vdisk: VDisk to validate
        :type vdisk: VDisk
        :param start_time: Time when snapshots started to be made
        :type start_time: datetime.datetime
        :param sticky_hours: Hours that the sticky snapshots were made on
        :type sticky_hours: List[int]
        :param consistent_hours: Hours that the consistent snapshots were made on
        :type consistent_hours: List[int]
        :param inconsistent_hours: Hours that the inconsistent snapshots were made on
        :type inconsistent_hours: List[int]
        :param snapshot_time_offset: Offset time to create snapshot. Defaults to creating snapshot on the hour mark
        :type snapshot_time_offset: int
        :param automatic_snapshots: Indicate that the snapshots are made automatically (because of the scheduling)
        :type automatic_snapshots: bool
        """
        # Snapshot details
        is_sticky = len(sticky_hours) > 0
        is_consistent = len(consistent_hours) > 0
        label = 'c' if is_consistent else 'i'

        for day in xrange(number_of_days):
            base_timestamp = self._make_timestamp(start_time, DAY * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            self._print_message('- Deleting snapshots')
            # The absolute timestamp is used when providing one. Going back a day to skip a day similar to the scheduled task
            delete_snapshot_timestamp = base_timestamp + (MINUTE * 30) - DAY.total_seconds()
            GenericController.delete_snapshots(timestamp=delete_snapshot_timestamp)

            self._validate(vdisk=vdisk,
                           current_day=day,
                           start_time=start_time,
                           sticky_hours=sticky_hours,
                           consistent_hours=consistent_hours,
                           inconsistent_hours=inconsistent_hours)

            self._print_message('- Creating snapshots')
            for x in consistent_hours + inconsistent_hours:
                timestamp = base_timestamp + (HOUR * x) + snapshot_time_offset
                VDiskController.create_snapshot(vdisk_guid=vdisk.guid,
                                                metadata={'label': 'ss_{0}_{1}:00'.format(label, x),
                                                          'is_sticky': is_sticky,
                                                          'timestamp': str(timestamp),
                                                          'is_automatic': automatic_snapshots,
                                                          'is_consistent': is_consistent})

    def test_snapshot_automatic_consistent(self):
        """
        is_automatic: True, is_consistent: True --> Automatically created consistent snapshots should be deleted
        """
        self._create_validate_snapshots(vdisk=self._build_vdisk(),
                                        start_time=datetime.datetime.now().date(),
                                        sticky_hours=[],
                                        consistent_hours=[2],
                                        inconsistent_hours=[],
                                        snapshot_time_offset=MINUTE * 30,  # Extra time to add to the hourly timestamps
                                        automatic_snapshots=True)

    def test_snapshot_automatic_not_consistent(self):
        """
        is_automatic: True, is_consistent: False --> Automatically created non-consistent snapshots should be deleted
        """
        self._create_validate_snapshots(vdisk=self._build_vdisk(),
                                        start_time=datetime.datetime.now().date(),
                                        sticky_hours=[],
                                        consistent_hours=[],
                                        inconsistent_hours=[2],
                                        snapshot_time_offset=0,  # Extra time to add to the hourly timestamps
                                        automatic_snapshots=True)

    def test_snapshot_non_automatic_consistent(self):
        """
        is_automatic: False, is_consistent: True --> Manually created consistent snapshots should be deleted
        """
        self._create_validate_snapshots(vdisk=self._build_vdisk(),
                                        start_time=datetime.datetime.now().date(),
                                        sticky_hours=[],
                                        consistent_hours=[2],
                                        inconsistent_hours=[],
                                        snapshot_time_offset=MINUTE * 30,  # Extra time to add to the hourly timestamps
                                        automatic_snapshots=False)

    def test_snapshot_not_automatic_not_consistent(self):
        """
        is_automatic: False, is_consistent: False --> Manually created non-consistent snapshots should be deleted
        """
        self._create_validate_snapshots(vdisk=self._build_vdisk(),
                                        start_time=datetime.datetime.now().date(),
                                        sticky_hours=[],
                                        consistent_hours=[],
                                        inconsistent_hours=[2],
                                        snapshot_time_offset=0,  # Extra time to add to the hourly timestamps
                                        automatic_snapshots=False)

    def test_snapshot_sticky(self):
        """
        is_sticky: True --> Sticky snapshots of any kind should never be deleted (Only possible to delete manually)
        """
        self._create_validate_snapshots(vdisk=self._build_vdisk(),
                                        start_time=datetime.datetime.now().date(),
                                        sticky_hours=[2],
                                        consistent_hours=[2],
                                        inconsistent_hours=[],
                                        snapshot_time_offset=MINUTE * 30,  # Extra time to add to the hourly timestamps
                                        automatic_snapshots=False)

    def test_happy_path(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistent
        every now and then. The delete policy is executed every day
        """
        vdisk_1 = self._build_vdisk()
        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        # Run the testing scenario
        base = datetime.datetime.now().date()
        consistent_hours = [6, 12, 18]
        inconsistent_hours = xrange(2, 23)

        for day in xrange(0, 35):
            base_timestamp = self._make_timestamp(base, DAY * day)
            self._print_message('')
            self._print_message('Day cycle: {0}: {1}'.format(day, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')))

            # At the start of the day, delete snapshot policy runs at 00:30
            self._print_message('- Deleting snapshots')
            GenericController.delete_snapshots(timestamp=base_timestamp + (MINUTE * 30) - DAY.total_seconds())

            # Validate snapshots
            self._print_message('- Validating snapshots')
            self._validate(vdisk=vdisk_1,
                           current_day=day,
                           start_time=base,
                           sticky_hours=[],
                           consistent_hours=consistent_hours,
                           inconsistent_hours=inconsistent_hours)

            # During the day, snapshots are taken
            # - Create non consistent snapshot every hour, between 2:00 and 22:00
            # - Create consistent snapshot at 6:30, 12:30, 18:30
            self._print_message('- Creating snapshots')
            for h in inconsistent_hours:
                timestamp = base_timestamp + (HOUR * h)
                snapshot_id = VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid,
                                                              metadata={'label': 'ss_i_{0}:00'.format(str(h)),
                                                                        'is_consistent': False,
                                                                        'timestamp': str(timestamp)})
                self._print_message('- Created inconsistent snapshot {} for vDisk {} on hour {}'.format(snapshot_id, vdisk_1.guid, h))
                if h in consistent_hours:
                    ts = (timestamp + (MINUTE * 30))
                    snapshot_id = VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid, metadata={'label': 'ss_c_{0}:30'.format(str(h)),
                                                                                                     'is_consistent': True,
                                                                                                     'timestamp': str(ts)})
                    self._print_message('- Created consistent snapshot {} for vDisk {} on hour {}'.format(snapshot_id, vdisk_1.guid, h))

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
            GenericController.delete_snapshots()
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
        [dynamic for dynamic in vdisk_1._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        for i in xrange(0, 2):
            metadata = {'label': str(i),
                        'is_consistent': False,
                        'is_sticky': False,
                        'timestamp': str((int(time.time() - datetime.timedelta(2).total_seconds() - i)))}
            snapshot_id = VDiskController.create_snapshot(vdisk_1.guid, metadata)
            StorageRouterClient.delete_snapshot_callbacks[vdisk_1.volume_id] = {snapshot_id: raise_an_exception}

        GenericController.delete_snapshots()

    ##################
    # HELPER METHODS #
    ##################
    def _print_message(self, message):
        if self.debug is True:
            print message

    def _visualise_snapshots(self, vdisk, current_day, start_time):
        # type: (VDisk, int, datetime.date) -> None
        """
        Visualize the snapshots of the VDisk
        :param vdisk: VDisk object
        :type vdisk: VDisk
        :param current_day: Number of the current day
        :type current_day: int
        :param start_time: Time when snapshots started to be made
        :type start_time: datetime.datetime
        :return:
        """
        snapshots = {}
        for snapshot in vdisk.snapshots:
            snapshots[int(snapshot['timestamp'])] = snapshot
        for day in xrange(0, current_day + 1):
            timestamp = self._make_timestamp(start_time, DAY * day)
            visual = '\t\t- {0} '.format(datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'))
            for t in xrange(timestamp, timestamp + HOUR * 24, MINUTE * 30):
                if t in snapshots:
                    visual += 'S' if snapshots[t]['is_sticky'] else 'C' if snapshots[t]['is_consistent'] else 'R'
                else:
                    visual += '-'
            self._print_message(visual)

    def _validate(self, vdisk, current_day, start_time, sticky_hours, consistent_hours, inconsistent_hours):
        # type: (VDisk, int, datetime.date, List[int], List[int], List[int]) -> None
        """
        This validates assumes the same policy as currently implemented in the policy code
        itself. In case the policy strategy ever changes, this unittest should be adapted as well
        or rewritten to load the implemented policy
        :param vdisk: VDisk to validate
        :type vdisk: VDisk
        :param current_day: Number of the current day
        :type current_day: int
        :param start_time: Time when snapshots started to be made
        :type start_time: datetime.date
        :param sticky_hours: Hours that the sticky snapshots were made on
        :type sticky_hours: List[int]
        :param consistent_hours: Hours that the consistent snapshots were made on
        :type consistent_hours: List[int]
        :param inconsistent_hours: Hours that the inconsistent snapshots were made on
        :type inconsistent_hours: List[int]
        """
        # Implemented policy:
        # < 1d | 1d bucket | 1 | best of bucket   | 1d
        # < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        # < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        # > 1m | delete

        self._print_message('\t- VDisk {0}'.format(vdisk.name))

        # Visualisation
        if self.debug:
            self._visualise_snapshots(vdisk, current_day, start_time)

        sticky = [int(s['timestamp']) for s in vdisk.snapshots if s['is_sticky']]
        consistent = [int(s['timestamp']) for s in vdisk.snapshots if s['is_consistent']]
        inconsistent = [int(s['timestamp']) for s in vdisk.snapshots if not s['is_consistent']]
        self._print_message('\t\t- {0} consistent, {1} inconsistent, {2} sticky'.format(len(consistent), len(inconsistent), len(sticky)))

        # Check for correct amount of snapshots
        amount_sticky = len(sticky_hours) * current_day  # Stickies do not get removed automatically
        amount_consistent = 0
        amount_inconsistent = 0
        processed_days = 0
        # First 24h period which are skipped so all taken snapshots are kept
        if processed_days < current_day:
            amount_consistent += len(consistent_hours)
            amount_inconsistent += len(inconsistent_hours)
            processed_days += 1

        # One consistent snapshot per day
        while processed_days < current_day and processed_days <= 7:
            if len(consistent_hours) > 0:
                amount_consistent += 1
            else:
                amount_inconsistent += 1
            processed_days += 1
        # One consistent snapshot per week
        while processed_days < current_day and processed_days <= 28:
            if len(consistent_hours) > 0:
                amount_consistent += 1
            else:
                amount_inconsistent += 1
            processed_days += 7
        self.assertEqual(first=len(sticky),
                         second=amount_sticky,
                         msg='Wrong amount of sticky snapshots: {0} vs expected {1}'.format(len(sticky), amount_sticky))
        if not sticky:
            self.assertEqual(first=amount_consistent, second=len(consistent),
                             msg='Wrong amount of consistent snapshots')
            self.assertEqual(first=amount_inconsistent, second=len(inconsistent),
                             msg='Wrong amount of inconsistent snapshots')

        # Check of the correctness of the snapshot timestamp
        if consistent_hours:
            sn_type = 'consistent'
            container = consistent
            time_diff = (HOUR * consistent_hours[-1]) + (MINUTE * 30)
        else:
            sn_type = 'inconsistent'
            container = inconsistent
            time_diff = (HOUR * inconsistent_hours[-1])

        for day in xrange(0, current_day):
            for h in sticky_hours:
                timestamp = self._make_timestamp(start_time, DAY * day) + (HOUR * h) + (MINUTE * 30)
                self.assertIn(member=timestamp, container=sticky,
                              msg='Expected sticky snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
            if day == (current_day - 1):
                for h in inconsistent_hours:
                    timestamp = self._make_timestamp(start_time, DAY * day) + (HOUR * h)
                    self.assertIn(member=timestamp, container=inconsistent,
                                  msg='Expected hourly inconsistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
                for h in consistent_hours:
                    timestamp = self._make_timestamp(start_time, DAY * day) + (HOUR * h) + (MINUTE * 30)
                    self.assertIn(member=timestamp, container=consistent,
                                  msg='Expected random consistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
            elif day > (current_day - 7):
                timestamp = self._make_timestamp(start_time, DAY * day) + time_diff
                self.assertIn(member=timestamp, container=container,
                              msg='Expected daily {0} snapshot for {1} at {2}'.format(sn_type, vdisk.name, self._from_timestamp(timestamp)))
            elif day % 7 == 0 and day > 28:
                timestamp = self._make_timestamp(start_time, DAY * day) + time_diff
                self.assertIn(member=timestamp, container=container,
                              msg='Expected weekly {0} snapshot for {1} at {2}'.format(sn_type, vdisk.name, self._from_timestamp(timestamp)))

    def test_retention_policy_configuration_levels(self):
        """
        Test the different retention policy settings
        :return:
        """
        global_config = [{'nr_of_days': 30, 'nr_of_snapshots': 30}]
        vpool_config = [{'nr_of_days': 7, 'nr_of_snapshots': 7}]
        vdisk_config = [{'nr_of_days': 1, 'nr_of_snapshots': 1}]

        Configuration.set(SNAPSHOT_POLICY_LOCATION, global_config)
        vdisk_1 = self._build_vdisk()
        vpool_1 = VPoolList.get_vpools()[0]

        # Global configuration
        snapshot_manager = SnapshotManager()

        policy_check = RetentionPolicy.from_configuration(global_config)[0]
        policy = snapshot_manager.get_policy_to_enforce(vdisk_1)[0]
        self.assertEqual(policy_check, policy)

        # VPool configuration
        vpool_1.snapshot_retention_policy = vpool_config
        vpool_1.save()

        snapshot_manager = SnapshotManager()

        policy_check = RetentionPolicy.from_configuration(vpool_config)[0]
        policy = snapshot_manager.get_policy_to_enforce(vdisk_1)[0]
        self.assertEqual(policy_check, policy)

        # VDisk Configuration
        snapshot_manager = SnapshotManager()

        vdisk_1.snapshot_retention_policy = vdisk_config
        vdisk_1.save()

        policy_check = RetentionPolicy.from_configuration(vdisk_config)[0]
        policy = snapshot_manager.get_policy_to_enforce(vdisk_1)[0]
        self.assertEqual(policy_check, policy)

    def test_retention_policy_overlap(self):
        """
        Test the application of the retention policy settings with overlapping timespans
        """
        global_config = [{'nr_of_days': 1, 'nr_of_snapshots': 1},
                         {'nr_of_days': 2, 'nr_of_snapshots': 1, 'consistency_first': True}]
        # The first theory (invalid one, but good to write down nonetheless:
        # Day 1 will have 1 consistent and 1 inconsistent snapshot. Inconsistent is older
        # Day 2 will have 2 inconsistent snapshots
        # The goal is to have both buckets still retain their snapshots
        # The bucket logic will distribute all snapshots to buckets that can fit them. Both buckets have the same start day
        # Bucket 1 (1 day) will have the consistent and inconsistent one
        # Bucket 2 (2 days) will have all the snapshots
        # Bucket 1 will choose the oldest, discarding the consistent one when removing
        # Bucket 2 will choose the consistent one above all else
        # In the end, bucket 2 won't have a snapshot

        # After reviewing the code: no overlap is possible as it increments the days that are processed.
        # It doesn't review every period by itself, which would be a nightmare on it's own
        Configuration.set(SNAPSHOT_POLICY_LOCATION, global_config)
        vdisk_1 = self._build_vdisk()
        start_time = datetime.datetime.now().date()
        snapshots = []
        for day, snapshot_consistencies in {1: [True, False],
                                            2: [False, False]}.iteritems():
            day_timestamp = self._make_timestamp(start_time, day * DAY * -1)
            for index, consistency in enumerate(snapshot_consistencies):
                snapshot_timestamp = day_timestamp + (HOUR * index)
                snapshots.append(VDiskController.create_snapshot(vdisk_guid=vdisk_1.guid,
                                                                 metadata={'label': 'snapshot_{0}:30'.format(str(index)),
                                                                           'is_consistent': consistency,
                                                                           'timestamp': str(snapshot_timestamp)}))
        GenericController.delete_snapshots()
        self.assertEqual(2, len(vdisk_1._snapshot_ids()))

    @staticmethod
    def _make_timestamp(base, offset):
        return int(time.mktime((base + offset).timetuple()))

    @staticmethod
    def _from_timestamp(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

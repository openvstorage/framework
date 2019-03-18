# Copyright (C) 2019 iNuron NV
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

import time
from datetime import datetime, timedelta
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.lib.vdisk import VDiskController
from ovs.log.log_handler import LogHandler


class RetentionPolicy(object):
    def __init__(self, nr_of_snapshots, nr_of_days):
        # type: (int, int) -> None
        """
        Initialize a retention policy
        :param nr_of_snapshots: Number of snapshots to keep over the configured number of days
        :type nr_of_snapshots: int
        :param nr_of_days: Number of days to account the number of snapshots for
        :type nr_of_days: int
        """
        self.nr_of_snapshots = nr_of_snapshots
        self.nr_of_days = nr_of_days

    @classmethod
    def from_configuration(cls, configuration):
        # type: (List[Dict[str, int]]) -> List[RetentionPolicy]
        """
        A configuration should look like this:
        [{'nr_of_snapshots': 24, 'nr_of_days': 1},
        {'nr_of_snapshots': 6,  'nr_of_days': 6},
        {'nr_of_snapshots': 3,  'nr_of_days': 21}])
        The passed number of snapshots is an absolute number of snapshots and is evenly distributed across the number of days passed in the interval.
        This way, this config will result in storing
        one snapshot per hour the first day
        one snapshot per day the rest of the week
        one snapshot per week the rest of the month
        one older snapshot snapshot will always be stored for an interval older then the longest interval passed in the config
        :param configuration: Configuration to use
        :type configuration: List[Dict[str, int]]
        :return: List[RetentionPolicy]
        """
        return [cls(**c) for c in configuration]


class Snapshot(object):
    def __init__(self, timestamp, snapshot_id, vdisk_guid, is_consistent, is_sticky=False, *args, **kwargs):
        """
        Initialize a snapshot object
        :param timestamp:
        :param snapshot_id:
        :param vdisk_guid:
        :param is_consistent:
        :param is_sticky:
        :param args:
        :param kwargs:
        """
        self.timestamp = int(timestamp)
        self.snapshot_id = snapshot_id
        self.vdisk_guid = vdisk_guid
        self.consistent = is_consistent
        self.is_sticky = is_sticky

    def __str__(self):
        return 'Snapshot for vDisk {0}'.format(self.vdisk_guid)


class Bucket(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.snapshots = []

    def is_snapshot_in_interval(self, snapshot):
        # type: (Snapshot) -> bool
        return self.start >= snapshot.timestamp > self.end

    def try_add_snapshot(self, snapshot):
        if self.is_snapshot_in_interval(snapshot):
            self.snapshots.append(snapshot)

    def get_obsolete_snapshots(self, consistency_first=False):
        # type: (bool) -> List[Snapshot]
        """
        Retrieve all snapshots which are no longer within this interval
        :param consistency_first: Consistency of the snapshot is priortized above the age
        :type consistency_first: bool
        :return: List with Snapshots
        :rtype: List[Snapshot]
        """
        if not self.end:
            # No end date for the interval, every snapshot is obsolete
            return self.snapshots

        if consistency_first:
            best = None
            for snapshot in self.snapshots:
                if best is None:
                    best = snapshot
                # Consistent is better than inconsistent
                elif snapshot.consistent and not best.consistent:
                    best = snapshot
                # Newer (larger timestamp) is better than older snapshots
                elif snapshot.consistent == best.consistent and snapshot.timestamp > best.timestamp:
                    best = snapshot
            return [s for s in self.snapshots if s.timestamp != best.timestamp]
        # First the oldest snapshot and remove all younger ones
        oldest = None
        for snapshot in self.snapshots:
            if oldest is None:
                oldest = snapshot
            # Older (smaller timestamp) is the one we want to keep
            elif snapshot.timestamp < oldest.timestamp:
                oldest = snapshot
        return [s for s in self.snapshots if s.timestamp != oldest.timestamp]

    def __str__(self):
        return 'Bucket (start: {0}, end: {1}) with {2}'.format(self.start, self.end, self.snapshots)


class SnapshotManager(object):
    """
    Manages snapshots of all vdisks
    """
    _logger = LogHandler.get('lib', name='generic tasks')

    def __init__(self):
        self.global_policy = self.get_retention_policy()
        self.vpool_policies = self.get_retention_policies_for_vpools()

    def get_policy_to_enforce(self, vdisk):
        # type: (VDisk) -> List[RetentionPolicy]
        """
        Retrieve the policy to enforce for a VDisk
        :param vdisk: VDisk to retrieve policy for
        :type vdisk: VDisk
        :return: Policy to enforce
        :rtype: List[RetentionPolicy]
        """
        return self.get_retention_policy_vdisk(vdisk) or self.vpool_policies.get(vdisk.vpool) or self.global_policy

    @staticmethod
    def get_retention_policy():
        # type: () -> List[RetentionPolicy]
        """
        Retrieve the globally configured retention policy
        """
        # @todo retrieve the config path
        return RetentionPolicy.from_configuration([{'nr_of_snapshots': 24, 'nr_of_days': 1},  # One per hour
                                                   {'nr_of_snapshots': 6, 'nr_of_days': 6},  # one per day for rest of the week
                                                   {'nr_of_snapshots': 3, 'nr_of_days': 21}])  # One per week for the rest of the week

    @classmethod
    def get_retention_policies_for_vpools(cls):
        # type: () -> Dict[VPool, List[RetentionPolicy]]
        """
        Map VPool with its retention policy (if any)
        :return: Dict with VPool as keys and list of RetentionPolicy as value
        :rtype: Dict[VPool, List[RetentionPolicy]]
        """
        vpool_policies = {}
        for vpool in VPoolList.get_vpools():
            policies_config = cls.get_retention_policy_vpool(vpool)
            if policies_config:
                vpool_policies[vpool] = RetentionPolicy.from_configuration(policies_config)
        return vpool_policies

    @staticmethod
    def get_retention_policy_vpool(vpool):
        # type: (VPool) -> Union[List[RetentionPolicy], None]
        """
        Retrieve the retention policy for the VPool (if any)
        """
        # @todo Retrieve config key
        return None

    @staticmethod
    def get_retention_policy_vdisk(vdisk):
        # type: (VDisk) -> Union[List[RetentionPolicy], None]
        """
        Retrieve the retention policy for the VDisk (if any)
        """
        # @todo retrieve config key
        return None

    @staticmethod
    def make_timestamp(base, offset):
        # type: (datetime, timedelta) -> int
        """
        Create an integer based timestamp based on a datetime and a timedelta
        :param base: Base timestamp
        :type base: datetime
        :param offset: Offset in days
        :type offset: timedelta
        :return: Timestamp
        """
        return int(time.mktime((base - offset).timetuple()))

    @classmethod
    def _get_snapshot_buckets(cls, start_time, policies):
        # type: (datetime, List[RetentionPolicy]) -> List[Bucket]
        """
        Retrieve the bucket distribution based on the policies
        There is always an additional bucket to keep track of older snapshots
        :param start_time: Datetime to start counting from
        :type start_time: datetime
        :param policies
        :type policies: RetentionPolicies
        :return:
        """
        day_delta = timedelta(1)  # Convert to number of seconds in calculations
        buckets = []
        processed_retention_days = 0
        offset = processed_retention_days * day_delta

        for policy in policies:  # type: RetentionPolicy
            number_of_days = policy.nr_of_days
            number_of_snapshots = policy.nr_of_snapshots
            snapshot_timedelta = number_of_days * day_delta / number_of_snapshots
            for i in xrange(0, number_of_snapshots):
                buckets.append(Bucket(start=cls.make_timestamp(start_time, offset + snapshot_timedelta * i),
                                      end=cls.make_timestamp(start_time, offset + snapshot_timedelta * (i + 1))))
            processed_retention_days += number_of_days
            offset = processed_retention_days * day_delta
        # Always add a bucket which falls out of the configured retention
        buckets.append(Bucket(start=cls.make_timestamp(start_time, processed_retention_days * day_delta), end=0))
        return buckets

    @staticmethod
    def is_vdisk_running(vdisk):
        # type: (VDisk) -> bool
        """
        Determine if the VDisk is running
        :return: True if the vdisk is running
        :rtype: bool
        """
        return vdisk.info['object_type'] in ['BASE']

    def delete_snapshots(self, timestamp=None):
        """
        Delete snapshots & scrubbing policy

        Implemented delete snapshot policy:
        < 1d | 1d bucket | 1 | best of bucket   | 1d
        < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        > 1m | delete

        :param timestamp: Timestamp to determine whether snapshots should be kept or not, if none provided, current time will be used
        :type timestamp: float
        :return: None
        """
        if timestamp is None:
            timestamp = time.time()

        # @todo think about backwards compatibility. The previous code would not account for the first day
        start_time = datetime.fromtimestamp(timestamp)

        # Get a list of all snapshots that are used as parents for clones
        parent_snapshots = set([vd.parentsnapshot for vd in VDiskList.get_with_parent_snaphots()])

        # Distribute all snapshots into buckets. These buckets specify an interval and are ordered young to old
        bucket_chains = []
        for vdisk in VDiskList.get_vdisks():
            if not self.is_vdisk_running(vdisk):
                continue
            vdisk.invalidate_dynamics('being_scrubbed')
            if vdisk.being_scrubbed:
                continue

            bucket_chain = self._get_snapshot_buckets(start_time, self.get_policy_to_enforce(vdisk))
            for vdisk_snapshot in vdisk.snapshots:
                snapshot = Snapshot(**vdisk_snapshot)
                if snapshot.is_sticky:
                    continue
                if snapshot.vdisk_guid in parent_snapshots:
                    self._logger.info('Not deleting snapshot {0} because it has clones'.format(snapshot.vdisk_guid))
                    continue
                for bucket in bucket_chain:
                    bucket.try_add_snapshot(snapshot)
            bucket_chains.append(bucket_chain)

        # Delete obsolete snapshots
        for index, bucket_chain in enumerate(bucket_chains):
            # @todo this consistency first behaviour changed with the new implementation
            # There are now buckets based on hourly intervals which means the consistency of the first day is not guaranteed (unless the config is specified that way)
            consistency_first = index == 0
            for bucket in bucket_chain:
                obsolete_snapshots = bucket.get_obsolete_snapshots(consistency_first)
                for snapshot in obsolete_snapshots:
                    VDiskController.delete_snapshot(vdisk_guid=snapshot.vdisk_guid, snapshot_id=snapshot.snapshot_id)

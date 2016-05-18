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
Delete snapshots test module
"""
import os
import time
import datetime
import unittest
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.failuredomain import FailureDomain
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.generic.system import System
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.tests.mockups import MockStorageRouterClient
from ovs.lib.vmachine import VMachineController
from ovs.lib.vdisk import VDiskController
from ovs.lib.scheduledtask import ScheduledTaskController


class DeleteSnapshots(unittest.TestCase):
    """
    This test class will validate the various scenarios of the delete snapshots logic
    """
    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        cls.persistent = PersistentFactory.get_client()
        cls.persistent._keep_in_memory_only = True
        cls.persistent.clean()

        cls.volatile = VolatileFactory.get_client()
        cls.volatile._keep_in_memory_only = True
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
        MockStorageRouterClient.clean()

    def test_happypath(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistent
        every now an then. The delete policy is executed every day
        """
        # Setup
        # There are 2 machines; one with two disks, one with one disk and a stand-alone additional disk
        failure_domain = FailureDomain()
        failure_domain.name = 'Test'
        failure_domain.save()
        backend_type = BackendType()
        backend_type.name = 'BackendType'
        backend_type.code = 'BT'
        backend_type.save()
        vpool = VPool()
        vpool.name = 'vpool'
        vpool.status = 'RUNNING'
        vpool.backend_type = backend_type
        vpool.save()
        pmachine = PMachine()
        pmachine.name = 'PMachine'
        pmachine.username = 'root'
        pmachine.ip = '127.0.0.1'
        pmachine.hvtype = 'VMWARE'
        pmachine.save()
        storage_router = StorageRouter()
        storage_router.name = 'storage_router'
        storage_router.ip = '127.0.0.1'
        storage_router.pmachine = pmachine
        storage_router.machine_id = System.get_my_machine_id()
        storage_router.rdma_capable = False
        storage_router.primary_failure_domain = failure_domain
        storage_router.save()
        disk = Disk()
        disk.name = 'physical_disk_1'
        disk.path = '/dev/non-existent'
        disk.size = 500 * 1024 ** 3
        disk.state = 'OK'
        disk.is_ssd = True
        disk.storagerouter = storage_router
        disk.save()
        disk_partition = DiskPartition()
        disk_partition.id = 'disk_partition_id'
        disk_partition.disk = disk
        disk_partition.path = '/dev/disk/non-existent'
        disk_partition.size = 400 * 1024 ** 3
        disk_partition.state = 'OK'
        disk_partition.offset = 1024
        disk_partition.roles = [DiskPartition.ROLES.SCRUB]
        disk_partition.mountpoint = '/var/tmp'
        disk_partition.save()
        vmachine_1 = VMachine()
        vmachine_1.name = 'vmachine_1'
        vmachine_1.devicename = 'dummy'
        vmachine_1.pmachine = pmachine
        vmachine_1.save()
        vdisk_1_1 = VDisk()
        vdisk_1_1.name = 'vdisk_1_1'
        vdisk_1_1.volume_id = 'vdisk_1_1'
        vdisk_1_1.vmachine = vmachine_1
        vdisk_1_1.vpool = vpool
        vdisk_1_1.devicename = 'dummy'
        vdisk_1_1.size = 0
        vdisk_1_1.save()
        vdisk_1_1.reload_client()
        vdisk_1_2 = VDisk()
        vdisk_1_2.name = 'vdisk_1_2'
        vdisk_1_2.volume_id = 'vdisk_1_2'
        vdisk_1_2.vmachine = vmachine_1
        vdisk_1_2.vpool = vpool
        vdisk_1_2.devicename = 'dummy'
        vdisk_1_2.size = 0
        vdisk_1_2.save()
        vdisk_1_2.reload_client()
        vmachine_2 = VMachine()
        vmachine_2.name = 'vmachine_2'
        vmachine_2.devicename = 'dummy'
        vmachine_2.pmachine = pmachine
        vmachine_2.save()
        vdisk_2_1 = VDisk()
        vdisk_2_1.name = 'vdisk_2_1'
        vdisk_2_1.volume_id = 'vdisk_2_1'
        vdisk_2_1.vmachine = vmachine_2
        vdisk_2_1.vpool = vpool
        vdisk_2_1.devicename = 'dummy'
        vdisk_2_1.size = 0
        vdisk_2_1.save()
        vdisk_2_1.reload_client()
        vdisk_3 = VDisk()
        vdisk_3.name = 'vdisk_3'
        vdisk_3.volume_id = 'vdisk_3'
        vdisk_3.vpool = vpool
        vdisk_3.devicename = 'dummy'
        vdisk_3.size = 0
        vdisk_3.save()
        vdisk_3.reload_client()

        for disk in [vdisk_1_1, vdisk_1_2, vdisk_2_1, vdisk_3]:
            [dynamic for dynamic in disk._dynamics if dynamic.name == 'snapshots'][0].timeout = 0

        # Run the testing scenario
        travis = 'TRAVIS' in os.environ and os.environ['TRAVIS'] == 'true'
        if travis is True:
            print 'Running in Travis, reducing output.'
        debug = not travis
        amount_of_days = 50
        base = datetime.datetime.now().date()
        day = datetime.timedelta(1)
        minute = 60
        hour = minute * 60

        for d in xrange(0, amount_of_days):
            base_timestamp = self._make_timestamp(base, day * d)
            print ''
            print 'Day cycle: {0}: {1}'.format(d, datetime.datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d'))

            # At the start of the day, delete snapshot policy runs at 00:30
            print '- Deleting snapshots'
            ScheduledTaskController.delete_snapshots(timestamp=base_timestamp + (minute * 30))

            # Validate snapshots
            print '- Validating snapshots'
            for vdisk in [vdisk_1_1, vdisk_1_2, vdisk_2_1, vdisk_3]:
                self._validate(vdisk, d, base, amount_of_days, debug)

            # During the day, snapshots are taken
            # - Create non consistent snapshot every hour, between 2:00 and 22:00
            # - Create consistent snapshot at 6:30, 12:30, 18:30
            print '- Creating snapshots'
            for h in xrange(2, 23):
                timestamp = base_timestamp + (hour * h)
                for vm in [vmachine_1, vmachine_2]:
                    VMachineController.snapshot(machineguid=vm.guid,
                                                label='ss_i_{0}:00'.format(str(h)),
                                                is_consistent=False,
                                                timestamp=timestamp)
                    if h in [6, 12, 18]:
                        ts = (timestamp + (minute * 30))
                        VMachineController.snapshot(machineguid=vm.guid,
                                                    label='ss_c_{0}:30'.format(str(h)),
                                                    is_consistent=True,
                                                    timestamp=ts)

                VDiskController.create_snapshot(diskguid=vdisk_3.guid,
                                                metadata={'label': 'ss_i_{0}:00'.format(str(h)),
                                                          'is_consistent': False,
                                                          'timestamp': str(timestamp),
                                                          'machineguid': None})
                if h in [6, 12, 18]:
                    ts = (timestamp + (minute * 30))
                    VDiskController.create_snapshot(diskguid=vdisk_3.guid,
                                                    metadata={'label': 'ss_c_{0}:30'.format(str(h)),
                                                              'is_consistent': True,
                                                              'timestamp': str(ts),
                                                              'machineguid': None})

    def _validate(self, vdisk, current_day, base_date, amount_of_days, debug):
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
        day = datetime.timedelta(1)

        print '  - {0}'.format(vdisk.name)

        # Visualisation
        if debug:
            snapshots = {}
            for snapshot in vdisk.snapshots:
                snapshots[int(snapshot['timestamp'])] = snapshot
            for d in xrange(0, amount_of_days):
                timestamp = self._make_timestamp(base_date, d * day)
                visual = '    - {0} '.format(datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'))
                for t in xrange(timestamp, timestamp + hour * 24, minute * 30):
                    if t in snapshots:
                        visual += 'C' if snapshots[t]['is_consistent'] else 'R'
                    else:
                        visual += '-'
                print visual

        consistent = [int(s['timestamp']) for s in vdisk.snapshots if s['is_consistent'] is True]
        inconsistent = [int(s['timestamp']) for s in vdisk.snapshots if s['is_consistent'] is False]
        print '    - {0} consistent, {1} inconsistent'.format(len(consistent), len(inconsistent))

        # Check for correct amount of snapshots
        amount_consistent = 0
        amount_inconsistent = 0
        pointer = 0
        if pointer < current_day:
            amount_consistent += 3     # First day, there are 3 consistent snapshots
            amount_inconsistent += 21  # First day, there are 20 inconsistent snapshots
            pointer += 1
        while pointer < current_day and pointer <= 7:
            amount_consistent += 1  # One consistent snapshot per day
            pointer += 1
        while pointer < current_day and pointer <= 28:
            amount_consistent += 1  # One consistent snapshot per week
            pointer += 7
        self.assertEqual(first=len(consistent),
                         second=amount_consistent,
                         msg='Wrong amount of consistent snapshots: {0} vs expected {1}'.format(len(consistent), amount_consistent))
        self.assertEqual(first=len(inconsistent),
                         second=amount_inconsistent,
                         msg='Wrong amount of inconsistent snapshots: {0} vs expected {1}'.format(len(inconsistent), amount_inconsistent))

        # Check of the correctness of the snapshot timestamp
        for d in xrange(0, current_day):
            if d == (current_day - 1):
                for h in xrange(2, 23):
                    timestamp = self._make_timestamp(base_date, d * day) + (hour * h)
                    self.assertIn(member=timestamp,
                                  container=inconsistent,
                                  msg='Expected hourly inconsistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
                    if h in [6, 12, 18]:
                        ts = (timestamp + (minute * 30))
                        self.assertIn(member=ts,
                                      container=consistent,
                                      msg='Expected random consistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(ts)))
            elif d > (current_day - 7):
                timestamp = self._make_timestamp(base_date, d * day) + (hour * 18) + (minute * 30)
                self.assertIn(member=timestamp,
                              container=consistent,
                              msg='Expected daily consistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))
            elif d % 7 == 0 and d > 28:
                timestamp = self._make_timestamp(base_date, d * day) + (hour * 18) + (minute * 30)
                self.assertIn(member=timestamp,
                              container=consistent,
                              msg='Expected weekly consistent snapshot for {0} at {1}'.format(vdisk.name, self._from_timestamp(timestamp)))

    @staticmethod
    def _make_timestamp(base, offset):
        return int(time.mktime((base + offset).timetuple()))

    @staticmethod
    def _from_timestamp(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

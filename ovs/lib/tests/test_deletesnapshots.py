# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Delete snapshots test module
"""
import sys
import unittest
from time import mktime
from datetime import datetime, timedelta
from unittest import TestCase
from ovs.lib.tests.mockups import StorageDriverModule
from ovs.extensions.generic.system import System
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore


class DeleteSnapshots(TestCase):
    """
    This test class will validate the various scenarios of the delete snapshots logic
    """

    Disk = None
    VDisk = None
    VPool = None
    VMachine = None
    PMachine = None
    logLevel = None
    BackendType = None
    DiskPartition = None
    StorageRouter = None
    VolatileMutex = None
    VDiskController = None
    VMachineController = None
    ScheduledTaskController = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        # Load dummy stores
        PersistentFactory.store = DummyPersistentStore()
        VolatileFactory.store = DummyVolatileStore()
        # Replace mocked classes
        sys.modules['ovs.extensions.storageserver.storagedriver'] = StorageDriverModule
        # Import required modules/classes after mocking is done
        from ovs.dal.hybrids.backendtype import BackendType
        from ovs.dal.hybrids.disk import Disk
        from ovs.dal.hybrids.diskpartition import DiskPartition
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.hybrids.vdisk import VDisk
        from ovs.dal.hybrids.vmachine import VMachine
        from ovs.dal.hybrids.vpool import VPool
        from ovs.extensions.generic.volatilemutex import VolatileMutex
        from ovs.lib.vmachine import VMachineController
        from ovs.lib.vdisk import VDiskController
        from ovs.lib.scheduledtask import ScheduledTaskController
        # Globalize mocked classes
        global Disk
        global VDisk
        global VMachine
        global PMachine
        global VPool
        global BackendType
        global DiskPartition
        global StorageRouter
        global VolatileMutex
        global VMachineController
        global VDiskController
        global ScheduledTaskController
        _ = VDisk(), VolatileMutex('dummy'), VMachine(), PMachine(), VPool(), BackendType(), \
            VMachineController, VDiskController, ScheduledTaskController, StorageRouter(), Disk(), DiskPartition()

        # Cleaning storage
        VolatileFactory.store.clean()
        PersistentFactory.store.clean()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()

    def test_happypath(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistent
        every now an then. The delete policy is executed every day
        """
        # Setup
        # There are 2 machines; one with two disks, one with one disk and an additional disk
        backend_type = BackendType()
        backend_type.name = 'BackendType'
        backend_type.code = 'BT'
        backend_type.save()
        vpool = VPool()
        vpool.name = 'vpool'
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
        debug = True
        amount_of_days = 50
        base = datetime.now().date()
        day = timedelta(1)
        minute = 60
        hour = minute * 60

        for d in xrange(0, amount_of_days):
            base_timestamp = DeleteSnapshots._make_timestamp(base, day * d)
            print ''
            print 'Day cycle: {0}: {1}'.format(d, datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d'))

            # At the start of the day, delete snapshot policy runs at 00:30
            print '- Deleting snapshots'
            ScheduledTaskController.deletescrubsnapshots(timestamp=base_timestamp + (minute * 30))

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
        day = timedelta(1)

        print '  - {0}'.format(vdisk.name)

        # Visualisation
        if debug:
            snapshots = {}
            for snapshot in vdisk.snapshots:
                snapshots[int(snapshot['timestamp'])] = snapshot
            for d in xrange(0, amount_of_days):
                timestamp = DeleteSnapshots._make_timestamp(base_date, d * day)
                visual = '    - {0} '.format(datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'))
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
        self.assertEqual(
            len(consistent), amount_consistent,
            'Wrong amount of consistent snapshots: {0} vs expected {1}'.format(len(consistent),
                                                                               amount_consistent)
        )
        self.assertEqual(
            len(inconsistent), amount_inconsistent,
            'Wrong amount of inconsistent snapshots: {0} vs expected {1}'.format(len(inconsistent),
                                                                                 amount_inconsistent)
        )

        # Check of the correctness of the snapshot timestamp
        for d in xrange(0, current_day):
            if d == (current_day - 1):
                for h in xrange(2, 23):
                    timestamp = DeleteSnapshots._make_timestamp(base_date, d * day) + (hour * h)
                    self.assertIn(
                        timestamp, inconsistent,
                        'Expected hourly inconsistent snapshot for {0} at {1}'.format(
                            vdisk.name, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
                        )
                    )
                    if h in [6, 12, 18]:
                        ts = (timestamp + (minute * 30))
                        self.assertIn(
                            ts, consistent,
                            'Expected random consistent snapshot for {0} at {1}'.format(
                                vdisk.name, datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
                            )
                        )
            elif d > (current_day - 7):
                timestamp = DeleteSnapshots._make_timestamp(base_date, d * day) + (hour * 18) + (minute * 30)
                self.assertIn(
                    timestamp, consistent,
                    'Expected daily consistent snapshot for {0} at {1}'.format(
                        vdisk.name, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
                    )
                )
            elif d % 7 == 0 and d > 28:
                timestamp = DeleteSnapshots._make_timestamp(base_date, d * day) + (hour * 18) + (minute * 30)
                self.assertIn(
                    timestamp, consistent,
                    'Expected weekly consistent snapshot for {0} at {1}'.format(
                        vdisk.name, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
                    )
                )

    @staticmethod
    def _make_timestamp(base, offset):
        return int(mktime((base + offset).timetuple()))


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DeleteSnapshots)
    unittest.TextTestRunner().run(suite)

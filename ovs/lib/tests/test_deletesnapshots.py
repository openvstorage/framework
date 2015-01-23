# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Delete snapshots test module
"""
import logging
import sys
import unittest
from time import mktime
from datetime import datetime
from unittest import TestCase
from ovs.lib.tests.mockups import StorageDriver
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore


class DeleteSnapshots(TestCase):
    """
    This test class will validate the various scenarios of the delete snapshots logic
    """

    VDisk = None
    VMachine = None
    PMachine = None
    VPool = None
    BackendType = None
    VolatileMutex = None
    VMachineController = None
    VDiskController = None
    ScheduledTaskController = None
    logLevel = None

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
        sys.modules['ovs.extensions.storageserver.storagedriver'] = StorageDriver
        # Import required modules/classes after mocking is done
        from ovs.dal.hybrids.vmachine import VMachine
        from ovs.dal.hybrids.vdisk import VDisk
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.hybrids.vpool import VPool
        from ovs.dal.hybrids.backendtype import BackendType
        from ovs.extensions.generic.volatilemutex import VolatileMutex
        from ovs.lib.vmachine import VMachineController
        from ovs.lib.vdisk import VDiskController
        from ovs.lib.scheduledtask import ScheduledTaskController
        # Globalize mocked classes
        global VDisk
        global VMachine
        global PMachine
        global VPool
        global BackendType
        global VolatileMutex
        global VMachineController
        global VDiskController
        global ScheduledTaskController
        _ = VDisk(), VolatileMutex('dummy'), VMachine(), PMachine(), VPool(), BackendType(), \
            VMachineController, VDiskController, ScheduledTaskController

        # Cleaning storage
        VolatileFactory.store.clean()
        PersistentFactory.store.clean()

        # Logging
        DeleteSnapshots.logLevel = logging.root.manager.disable
        logging.disable('INFO')

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        logging.disable(DeleteSnapshots.logLevel)

    def test_happypath(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistents
        every now an then. The delelete policy is exectued every day
        """
        # Setup
        # There are 2 machines; one with two disks, one with one disk and an additional disk
        vpool = VPool()
        vpool.name = 'vpool'
        vpool.backend_type = BackendType()
        vpool.save()
        vmachine_1 = VMachine()
        vmachine_1.name = 'vmachine_1'
        vmachine_1.devicename = 'dummy'
        vmachine_1.pmachine = PMachine()
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
        vmachine_2.pmachine = PMachine()
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
        now = int(mktime(datetime.now().date().timetuple()))  # Last night
        minute = 60
        hour = minute * 60
        day = hour * 24
        for d in xrange(0, amount_of_days):
            base_timestamp = now + (day * d)
            print ''
            print 'Day cycle: {}: {}'.format(
                d,
                datetime.fromtimestamp(base_timestamp).strftime('%Y-%m-%d')
            )

            # At the start of the day, delete snapshot policy runs at 00:30
            print '- Deleting snapshots'
            ScheduledTaskController.deletescrubsnapshots(timestamp=base_timestamp + (minute * 30))

            # Validate snapshots
            print '- Validating snapshots'
            for vdisk in [vdisk_3]:  # [vdisk_1_1, vdisk_1_2, vdisk_2_1, vdisk_3]:
                self._validate(vdisk, d, now, amount_of_days, debug)

            # During the day, snapshots are taken
            # - Create non consistent snapshot every hour, between 2:00 and 22:00
            # - Create consistent snapshot at 6:30, 12:30, 18:30
            print '- Creating snapshots'
            for h in xrange(2, 23):
                timestamp = base_timestamp + (hour * h)
                for vm in [vmachine_1, vmachine_2]:
                    VMachineController.snapshot(machineguid=vm.guid,
                                                label='ss_i_{}:00'.format(str(h)),
                                                is_consistent=False,
                                                timestamp=timestamp)
                    if h in [6, 12, 18]:
                        ts = (timestamp + (minute * 30))
                        VMachineController.snapshot(machineguid=vm.guid,
                                                    label='ss_c_{}:30'.format(str(h)),
                                                    is_consistent=True,
                                                    timestamp=ts)

                VDiskController.create_snapshot(diskguid=vdisk_3.guid,
                                                metadata={'label': 'ss_i_{}:00'.format(str(h)),
                                                          'is_consistent': False,
                                                          'timestamp': timestamp,
                                                          'machineguid': None})
                if h in [6, 12, 18]:
                    ts = (timestamp + (minute * 30))
                    VDiskController.create_snapshot(diskguid=vdisk_3.guid,
                                                    metadata={'label': 'ss_c_{}:30'.format(str(h)),
                                                              'is_consistent': True,
                                                              'timestamp': ts,
                                                              'machineguid': None})

    def _validate(self, vdisk, current_day, initial_timestamp, amount_of_days, debug):
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
        day = hour * 24

        # Visualisation
        if debug:
            snapshots = {}
            for snapshot in vdisk.snapshots:
                snapshots[snapshot['timestamp']] = snapshot
            for d in xrange(0, amount_of_days):
                timestamp = initial_timestamp + (d * day)
                visual = '  - {} '.format(datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'))
                for t in xrange(timestamp, timestamp + day, minute * 30):
                    if t in snapshots:
                        visual += 'C' if snapshots[t]['is_consistent'] else 'R'
                    else:
                        visual += '-'
                print visual

        consistent = [s['timestamp'] for s in vdisk.snapshots if s['is_consistent'] is True]
        inconsistent = [s['timestamp'] for s in vdisk.snapshots if s['is_consistent'] is False]
        print '  - {} consistent, {} inconsistent'.format(len(consistent), len(inconsistent))

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
            'Wrong amount of consistent snapshots: {} vs expected {}'.format(len(consistent),
                                                                             amount_consistent)
        )
        self.assertEqual(
            len(inconsistent), amount_inconsistent,
            'Wrong amount of inconsistent snapshots: {} vs expected {}'.format(len(inconsistent),
                                                                               amount_inconsistent)
        )

        # Check of the correctness of the snapshot timestamp
        for d in xrange(0, current_day):
            if d == (current_day - 1):
                for h in xrange(2, 23):
                    timestamp = initial_timestamp + (d * day) + (hour * h)
                    self.assertIn(
                        timestamp, inconsistent,
                        'Expected hourly inconsistent snapshot at {}'.format(
                            datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
                        )
                    )
                    if h in [6, 12, 18]:
                        ts = (timestamp + (minute * 30))
                        self.assertIn(
                            ts, consistent,
                            'Expected random consistent snapshot at {}'.format(
                                datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
                            )
                        )
            elif d > (current_day - 7):
                timestamp = initial_timestamp + (day * d) + (hour * 18) + (minute * 30)
                self.assertIn(
                    timestamp, consistent,
                    'Expected daily consistent snapshot at {}'.format(
                        datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
                    )
                )
            elif d % 7 == 0 and d > 28:
                timestamp = initial_timestamp + (day * d) + (hour * 18) + (minute * 30)
                self.assertIn(
                    timestamp, consistent,
                    'Expected weekly consistent snapshot at {}'.format(
                        datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
                    )
                )


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DeleteSnapshots)
    unittest.TextTestRunner().run(suite)

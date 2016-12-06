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
Generic scheduled tasks test module
"""
import json
import unittest
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.generic.threadhelpers import Waiter
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.tests.mockups import LockedClient, StorageRouterClient
from ovs.lib.generic import GenericController
from ovs.lib.vdisk import VDiskController
from ovs.lib.tests.helpers import Helper


class ScheduledTaskTest(unittest.TestCase):
    """
    This test class will validate the various scenarios of the delete snapshots logic
    """
    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        cls.volatile = VolatileFactory.get_client()
        cls.persistent = PersistentFactory.get_client()
        cls.volatile.clean()
        cls.persistent.clean()
        StorageRouterClient.clean()

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.volatile.clean()
        self.persistent.clean()
        StorageRouterClient.clean()

    def tearDown(self):
        """
        Clean up the unittest
        """
        self.volatile.clean()
        self.persistent.clean()
        StorageRouterClient.clean()

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
        """
        _ = self
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
        with self.assertRaises(Exception) as raise_info:
            VDiskController.scrub_single_vdisk(vdisk.guid, storagerouter.guid)
        self.assertIn(vdisk.name, raise_info.exception.message)
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
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vdisks = structure['vdisks']
        storagerouter = structure['storagerouters'][1]
        System._machine_id = {storagerouter.ip: '1'}
        Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps({}, indent=4), raw=True)
        LockedClient.scrub_controller = {'possible_threads': ['scrub_{0}_{1}'.format(vpool.guid, storagerouter.guid)],
                                         'volumes': {},
                                         'waiter': Waiter(1)}
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
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 1, 1, 1), (7, 1, 1, 1), (8, 1, 1, 1), (9, 1, 1, 1), (10, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vdisks = structure['vdisks']
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

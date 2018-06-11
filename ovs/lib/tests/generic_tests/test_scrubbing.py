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
Generic test module
"""
import re
import unittest
from threading import Event
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs_extensions.generic.threadhelpers import Waiter
from ovs_extensions.services.mockups.systemd import SystemdMock
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.tests.mockups import LockedClient
from ovs.lib.generic import GenericController
from ovs.lib.helpers.generic.scrubber import ScrubShared, StackWorker, Scrubber
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
        ScrubShared._test_hooks = {}
        ScrubShared._unittest_data = {'setup': False}

    def test_prerequisites(self):
        """
        Test pre-requisite checking before running a scrubbing job
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisk = structure['vdisks'][1]
        storagerouter = structure['storagerouters'][1]
        vpool = structure['vpools'][1]
        LockedClient.scrub_controller = {'possible_threads': None,
                                         'volumes': {},
                                         'waiter': Waiter(1)}
        LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': False,
                                                                     'scrub_work': [0]}

        # Remove SCRUB partition from StorageRouter and try to scrub on it
        expected_log = 'Scrubber unittest - Assuming StorageRouter {0} is dead. Not scrubbing there'.format(
            storagerouter.ip)
        storagerouter.disks[0].partitions[0].roles = []
        storagerouter.disks[0].partitions[0].save()
        with self.assertRaises(ValueError) as raise_info:
            GenericController.execute_scrub(vdisk_guids=[vdisk.guid], manual=True)
        self.assertIn(member='No scrub locations found',
                      container=raise_info.exception.message)
        logs = LogHandler._logs.get('lib_generic tasks scrub', [])  # No logging should have happened yet
        self.assertNotIn(member=expected_log,
                         container=logs)

        # Restore SCRUB partition and make sure StorageRouter is unreachable
        storagerouter.disks[0].partitions[0].roles = [DiskPartition.ROLES.SCRUB]
        storagerouter.disks[0].partitions[0].save()
        storagerouter.invalidate_dynamics('partition_config')
        SSHClient._raise_exceptions[storagerouter.ip] = {'users': ['root'],
                                                         'exception': UnableToConnectException('No route to host')}
        with self.assertRaises(ValueError):
            GenericController.execute_scrub(vdisk_guids=[vdisk.guid], manual=True)
        logs = LogHandler._logs['lib_generic tasks scrub']
        self.assertIn(member=expected_log,
                      container=logs)
        self.assertEqual(first=logs[expected_log],
                         second='error')

        # Now actually attempt to scrub
        # The scrub should fail as no result has been set for the vDisks
        SSHClient._raise_exceptions = {}
        with self.assertRaises(Exception) as raise_info:
            GenericController.execute_scrub(vdisk_guids=[vdisk.guid], storagerouter_guid=storagerouter.guid,
                                            manual=True)
        # Only one stack would be deployed (one scrub location) and only one thread (one vDisk)
        expected_log = 'Scrubber unittest - vPool {0} - StorageRouter {1} - Stack 0 - Scrubbing thread 0 - vDisk {2} with volume id {3} - Scrubbing failed'.format(
            vpool.name, storagerouter.name, vdisk.name, vdisk.volume_id)
        self.assertIn(member=expected_log,
                      container=raise_info.exception.message)

        # Make sure scrubbing succeeds now
        LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                     'scrub_work': [0]}
        GenericController.execute_scrub(vdisk_guids=[vdisk.guid], storagerouter_guid=storagerouter.guid, manual=True)
        with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
            self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                             second=0,
                             msg='Scrubbed vDisk {0} does not have the expected amount of scrubbing items: {1}'.format(
                                 vdisk.name, 0))

    def test_scrub_disk_failures(self):
        """
        1 vPool, 10 vDisks, 1 scrub role
        Scrubbing fails for 5 vDisks, check if scrubbing completed for all other vDisks
        Run scrubbing a 2nd time and verify scrubbing now works for failed vDisks
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 1, 1, 1), (7, 1, 1, 1), (8, 1, 1, 1), (9, 1, 1, 1), (10, 1, 1, 1)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpools = structure['vpools']
        vdisks = structure['vdisks']
        storagerouter_1 = structure['storagerouters'][1]
        storagerouter_2 = structure['storagerouters'][2]
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
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
        vdisk_namespaces = {}  # Validate all registered items are gone
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

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
                                 msg='Scrubbed vDisk {0} does not have the expected amount of scrubbing items: {1}'.format(
                                     vdisk.name, int(vdisk.name)))

        # Execute scrubbing again
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id]['success'] = True
        GenericController.execute_scrub()
        vdisk_namespaces = {}  # Validate all registered items are gone
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

        for vdisk in vdisks.values():
            with vdisk.storagedriver_client.make_locked_client(vdisk.volume_id) as locked_client:
                self.assertEqual(first=len(locked_client.get_scrubbing_workunits()),
                                 second=0,
                                 msg='Scrubbed vDisk {0} does still have scrubbing work left after scrubbing a 2nd time'.format(
                                     vdisk.name))

    def test_thread_spawning_limitation(self):
        """
        3 vPools, 15 vDisks, 5 scrub roles
        Validate 12 threads will be spawned and used out of a potential of 30 (5 scrub roles * 3 vPools * 2 threads per StorageRouter)
        We limit max amount of threads spawned per vPool to 2 in case 3 to 5 vPools are present
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2, 3],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 2, 2), (7, 2, 2, 2), (8, 2, 2, 2), (9, 2, 2, 2), (10, 2, 2, 2),
                        (11, 3, 3, 3), (12, 3, 3, 3), (13, 3, 3, 3), (14, 3, 3, 3), (15, 3, 3, 3)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
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
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(12)}
        LockedClient.thread_names = thread_names[:]
        for vdisk_id in sorted(vdisks):
            vdisk = vdisks[vdisk_id]
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        vdisk_namespaces = {}  # Validate all registered items are gone
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

        self.assertEqual(first=len(LockedClient.thread_names),
                         second=18,  # 30 possible thread_names - 12 which should be created and consumed
                         msg='Not all threads have been used in the process')

        # Of the 18 threads which have been created and consumed, 6 should have been created for each vPool
        for vpool in vpools.values():
            threads_left = [thread_name for thread_name in LockedClient.thread_names if vpool.guid in thread_name]
            self.assertEqual(first=len(threads_left),
                             second=6,
                             msg='Unexpected amount of threads left for vPool {0}'.format(vpool.name))

    def test_thread_division_skip_vtemplate(self):
        """
        1 vPool, 11 vDisks, 5 scrub roles (4 StorageRouters, one of then has 2 scrub roles)
        Check template vDisk is NOT scrubbed
        Check if vDisks are divided among all threads
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],  # vPool 2 has no vDisks attached to it
             'vdisks': [(i, 1, 1, 1) for i in xrange(1, 12)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vpool = structure['vpools'][1]
        vpools = structure['vpools']
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
        vdisk_namespaces = {}  # Validate all registered items are gone
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

        # Verify all threads have been 'consumed'
        self.assertEqual(first=len(LockedClient.thread_names),
                         second=0)
        self.assertIn(
            member='Scrubber unittest - vPool {0} - vDisk {1} with guid {2} is a template, not scrubbing'.format(
                vdisk_t.vpool.name, vdisk_t.name, vdisk_t.guid),
            container=LogHandler._logs['lib_generic tasks scrub'])

    def test_location_specification(self):
        """
        2 vPools, 8 vDisks, 2 scrub roles
        Validate correct vDisks are scrubbed on expected location when specifying vpool_guids and/or vdisk_guids
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 2, 1, 2), (3, 3, 2, 3), (4, 4, 2, 4),
                        (5, 1, 1, 1), (6, 2, 1, 2), (7, 3, 2, 3), (8, 4, 2, 4)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
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
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        GenericController.execute_scrub()
        vdisk_namespaces = {}  # Validate all registered items are gone
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

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
                                 list2=expected_work,
                                 msg='Not all expected work items were applied for vDisk {0}'.format(vdisk.name))

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
                                 list2=[],
                                 msg='Not all scrub work was applied for vDisk {0}'.format(vdisk.name))
        logs = LogHandler._logs['lib_generic tasks scrub']
        for log in logs:
            self.assertNotRegexpMatches(text=log,
                                        unexpected_regexp='.*Scrubber unittest - vPool [{0}|{1}] - StorageRouter {2} - .*'.format(
                                            vpools[1].name, vpools[2].name, storagerouters[1].name))

    def test_thread_spawning_configuration(self):
        """
        Configure amount of threads per StorageRouter to 5 for SR1
        2 vPools, 20 vDisks, 2 scrub roles
        Validate expected amount of threads is spawned
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
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
        Configuration.set(
            key='/ovs/framework/hosts/{0}/config|scrub_stack_threads'.format(storagerouters[1].machine_id),
            value=sr_1_threads)
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
        vdisk_namespaces = {}  # Validate all registered items are gone
        for vpool in vpools.values():
            vdisk_namespaces[ScrubShared._SCRUB_VDISK_KEY.format(vpool.name)] = []
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)
        counter = 0
        for log in LogHandler._logs['lib_generic tasks scrub']:
            if 'threads for proxy service' in log:
                match = re.match(
                    '^Scrubber unittest - vPool ([1|2]) - StorageRouter ([1|2]) - Stack ([0|1]) - .*ovs-albaproxy.*_scrub',
                    log)
                self.assertIsNotNone(match)
                if match.groups()[1] == storagerouters[1].name:
                    expected_threads = 5
                else:
                    expected_threads = 2
                self.assertIn(member='Spawning {0} threads for proxy'.format(expected_threads),
                              container=log)
                counter += 1
        self.assertEqual(first=4,  # Log entry for each combination of 2 vPools and 2 StorageRouters
                         second=counter)

    def test_proxy_deployment_removal_failures(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate special conditions that can occur
        - Proxy deployment fail (No thread should should and all should fail)
        - Proxy removal fail after scrub (All threads will try to clean up the proxy)
        """
        def _raise_exception(message):
            raise RuntimeError(message)

        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        # Insert some hooks to create some failure cases
        hooks = {'post_proxy_deployment': lambda x: _raise_exception('Simulated proxy deployment failure')}
        ScrubShared._test_hooks.update(hooks)
        # No scrubbing should have taken place
        with self.assertRaises(Exception):
            GenericController.execute_scrub()
        # Check if service has been removed properly
        service_manager = ServiceFactory.get_manager()
        for storagerouter in storagerouters.values():
            client = SSHClient(storagerouter, 'root')
            for proxy_name in proxy_names:
                self.assertFalse(service_manager.has_service(proxy_name, client), 'StorageRouter {0} still has proxy {1}'.format(storagerouter.name, proxy_name))
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

    def test_multiple_proxy_deployment_removal(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate special conditions that can occur
        - Proxy deployment fail (No thread should should and all should fail)
        - Proxy removal fail after scrub (All threads will try to clean up the proxy)
        """
        deployed_proxies = {}
        deployed_scrubber_configs = {}

        def _capture_deployed_proxies(self):
            service_manager = ServiceFactory.get_manager()
            client = SSHClient(self.storagerouter, 'root')
            for alba_proxy_service in self.alba_proxy_services:
                if service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                    if self not in deployed_proxies:
                        deployed_proxies[self] = []
                    deployed_proxies[self].append(alba_proxy_service)
            deployed_scrubber_configs[self] = Configuration.get(self.backend_config_key)

        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']
        vpools = structure['vpools']
        for vpool in vpools.values():
            # Simulate a vpool with multiple proxies
            backend_connection_manager = {'backend_type': 'MULTI',
                                          'backend_interface_retries_on_error': 5,
                                          'backend_interface_retry_interval_secs': 1,
                                          'backend_interface_retry_backoff_multiplier': 2.0}
            storagedriver_config = {'backend_connection_manager': backend_connection_manager}
            start_port = 100
            proxy_config = {'alba_connection_host': '',
                            'alba_connection_port': start_port,
                            'alba_connection_preset': '',
                            'alba_connection_timeout': 30,
                            'alba_connection_use_rora': True,
                            'alba_connection_transport': 'TCP',
                            'alba_connection_rora_manifest_cache_capacity': 25000,
                            'alba_connection_asd_connection_pool_capacity': 10,
                            'alba_connection_rora_timeout_msecs': 50,
                            'backend_type': 'ALBA'}
            for i in xrange(0, 4):
                port = start_port + i
                proxy_config_copy = proxy_config.copy()
                proxy_config_copy['alba_connection_port'] = port
                backend_connection_manager[str(i)] = proxy_config_copy
            DalHelper.set_vpool_storage_driver_configuration(vpool, config=storagedriver_config)
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure, 4)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        # Set the # proxies to be deployed to 4 on the first sr
        sr1_proxy_amount = 3
        sr1 = storagerouters[1]
        Configuration.set(key='/ovs/framework/hosts/{0}/config|scrub_proxy_amount'.format(sr1.machine_id),
                          value=3)
        # Insert some hooks to create some failure cases
        hooks = {'post_proxy_deployment': _capture_deployed_proxies}
        ScrubShared._test_hooks.update(hooks)
        # No scrubbing should have taken place
        GenericController.execute_scrub()

        # Check if the right number of proxies were deployed and check if the right amount of proxies are referenced in the scrubber config
        for stack_worker, running_proxy_services in deployed_proxies.iteritems():
            proxy_amount = 1
            if stack_worker.storagerouter == sr1:
                proxy_amount = sr1_proxy_amount
            self.assertEquals(len(running_proxy_services), proxy_amount, 'Not the right amount of proxies were deployed')
            proxy_config_keys = sorted([int(key) for key in deployed_scrubber_configs[stack_worker]['backend_connection_manager'].iterkeys() if key.isdigit()])
            self.assertListEqual(proxy_config_keys, sorted(xrange(0, proxy_amount)))

        # Check if service has been removed properly
        service_manager = ServiceFactory.get_manager()
        for storagerouter in storagerouters.values():
            client = SSHClient(storagerouter, 'root')
            # Only set to 4 for sr1
            for proxy_name in proxy_names:
                self.assertFalse(service_manager.has_service(proxy_name, client), 'StorageRouter {0} still has proxy {1}'.format(storagerouter.name, proxy_name))
                if storagerouter != sr1:  # Only one proxy would be deployed. Generated names generate it for everything
                    break
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():  # All registered items of this job should be cleaned up
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

    def test_multiple_proxy_deployment_removal_failures(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate special conditions that can occur
        - Proxy deployment fail (No thread should should and all should fail)
        - Proxy removal fail after scrub (All threads will try to clean up the proxy)
        """
        deployed_proxies = {}
        deployed_scrubber_configs = {}

        def _capture_deployed_proxies(self):
            service_manager = ServiceFactory.get_manager()
            client = SSHClient(self.storagerouter, 'root')
            for alba_proxy_service in self.alba_proxy_services:
                if service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                    if self not in deployed_proxies:
                        deployed_proxies[self] = []
                    deployed_proxies[self].append(alba_proxy_service)
            deployed_scrubber_configs[self] = Configuration.get(self.backend_config_key)

        def _raise_exception_second_proxy(self, alba_proxy_service):
            if alba_proxy_service.rsplit('_')[-1] == '1':
                raise RuntimeError('Simulated second proxy failure')

        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        storagerouters = structure['storagerouters']
        vpools = structure['vpools']
        for vpool in vpools.values():
            # Simulate a vpool with multiple proxies
            backend_connection_manager = {'backend_type': 'MULTI',
                                          'backend_interface_retries_on_error': 5,
                                          'backend_interface_retry_interval_secs': 1,
                                          'backend_interface_retry_backoff_multiplier': 2.0}
            storagedriver_config = {'backend_connection_manager': backend_connection_manager}
            start_port = 100
            proxy_config = {'alba_connection_host': '',
                            'alba_connection_port': start_port,
                            'alba_connection_preset': '',
                            'alba_connection_timeout': 30,
                            'alba_connection_use_rora': True,
                            'alba_connection_transport': 'TCP',
                            'alba_connection_rora_manifest_cache_capacity': 25000,
                            'alba_connection_asd_connection_pool_capacity': 10,
                            'alba_connection_rora_timeout_msecs': 50,
                            'backend_type': 'ALBA'}
            for i in xrange(0, 4):
                port = start_port + i
                proxy_config_copy = proxy_config.copy()
                proxy_config_copy['alba_connection_port'] = port
                backend_connection_manager[str(i)] = proxy_config_copy
            DalHelper.set_vpool_storage_driver_configuration(vpool, config=storagedriver_config)
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        sr1 = storagerouters[1]
        # No scrubbing can occur for this SR since the proxies can't be deployed due to a failure after proxy 2
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure, 4, skip_threads_for=sr1)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        # Set the # proxies to be deployed to 4 on the first sr
        sr1_proxy_amount = 3
        Configuration.set(key='/ovs/framework/hosts/{0}/config|scrub_proxy_amount'.format(sr1.machine_id),
                          value=3)
        # Insert some hooks to create some failure cases
        hooks = {'post_proxy_deployment': _capture_deployed_proxies,
                 'post_single_proxy_deployment': _raise_exception_second_proxy}
        ScrubShared._test_hooks.update(hooks)
        # No scrubbing should have taken place for storagerouter 1
        with self.assertRaises(Exception) as ex:
            GenericController.execute_scrub()
        for vpool in vpools.values():
            potential_logs = []
            for possible_stack_number in xrange(0, 2):  # Guessing the stacknumber. The list retrieved by the scrubber might take a different storagerouter as the first one
                potential_logs.append('- Scrubber unittest - vPool {0} - StorageRouter {1} - Stack {2} - An error occurred deploying ALBA proxies'.format(vpool.name, sr1.name, possible_stack_number))
            self.assertTrue(any(l in ex.exception.message for l in potential_logs), 'ALBA Proxies deployment error message not raised')

        # Check if the right number of proxies were deployed and check if the right amount of proxies are referenced in the scrubber config
        for stack_worker, running_proxy_services in deployed_proxies.iteritems():
            proxy_amount = 1
            if stack_worker.storagerouter == sr1:
                proxy_amount = sr1_proxy_amount
            self.assertEquals(len(running_proxy_services), proxy_amount, 'Not the right amount of proxies were deployed')
            proxy_config_keys = sorted([int(key) for key in deployed_scrubber_configs[stack_worker]['backend_connection_manager'].iterkeys() if key.isdigit()])
            self.assertListEqual(proxy_config_keys, sorted(xrange(0, proxy_amount)))

        # Check if service has been removed properly
        service_manager = ServiceFactory.get_manager()
        for storagerouter in storagerouters.values():
            client = SSHClient(storagerouter, 'root')
            # Only set to 4 for sr1
            for proxy_name in proxy_names:
                self.assertFalse(service_manager.has_service(proxy_name, client), 'StorageRouter {0} still has proxy {1}'.format(storagerouter.name, proxy_name))
                if storagerouter != sr1:  # Only one proxy would be deployed. Generated names generate it for everything
                    break
        # All registered items of this job should be cleaned up - Every vpool spawns two stacks: one for each storagerouter
        # so all items are scrubbed by storagerouter[0]
        for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():
            self.assertEquals(self.persistent.get(vdisk_namespace), namespace_value)

    def test_skipping_already_registered(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate race condition handling
        - Validate that already queued items won't be scrubbed again
        """
        print SystemdMock.services
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        # Test already queued items
        Scrubber.setup_for_unittests()  # Normally scrubber does the preparation but the information is now required beforehand
        local_sr = System.get_my_storagerouter()  # Executor of the scrubbing
        pid = None
        start_time = None
        other_sr = None
        print SystemdMock.services
        for ip, service_info in SystemdMock.services.iteritems():
            if ip == local_sr.ip:
                continue
            other_sr = StorageRouterList.get_by_ip(ip)
            worker_info = service_info.get('ovs-workers', {})
            pid = worker_info.get('pid')
            start_time = worker_info.get('start_time')
            break
        self.assertNotEqual(pid, None, 'A PID should be found for a different StorageRouter\'s ovs-worker process')
        self.assertNotEqual(start_time, None, 'A start time should be found for a different StorageRouter\'s ovs-worker process')
        # Register vdisks beforehand
        for index, vdisk in enumerate(vdisks.values()):
            if index == 2:
                break
            for vdisk_namespace, namespace_value in vdisk_namespaces.iteritems():
                namespace_value.append({'vdisk_guid': vdisk.guid, 'worker_pid': pid, 'worker_start': start_time,
                                        'storagerouter_guid': other_sr.guid})
                self.persistent.set(vdisk_namespace, namespace_value)
        GenericController.execute_scrub()
        logs = LogHandler._logs['lib_generic tasks scrub']
        for registered_items in vdisk_namespaces.values():
            for registered_item in registered_items:
                vdisk = VDisk(registered_item['vdisk_guid'])
                log = 'Scrubber unittest - vPool {0} - vDisk {1} with guid {2} has already been registered to get scrubbed, not queueing again'.format(
                    vdisk.vpool.name, vdisk.name, vdisk.guid)
                self.assertIn(log, logs)

    def test_race_conditions(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate race condition handling
        - Validate that proxies will be re-used
        - Validate that proxies won't be removed when still in use
        """
        stack_workers = []  # Copied stack workers by hooking

        def _wait_for_event(event):
            """
            Wait forever until the event is set
            :param event: Event object
            :type event: Threading.Event
            :return: None
            :rtype: NoneType
            """
            event.wait()

        def _inject_race(self):
            """
            Injects a race condition. The stack worker threads will wait for another the event to clear.
            A stackworker is taken to duplicate it and register its proxy usage
            :param self: Scrubber instance
            :return: None
            :rtype: NoneType
            """
            for model_stack_worker in self.stack_workers:
                stack_worker = StackWorker(queue=model_stack_worker.queue,
                                           vpool=model_stack_worker.vpool,
                                           scrub_info=model_stack_worker.scrub_info,
                                           error_messages=self.error_messages,
                                           worker_contexts=self.worker_contexts,
                                           stack_work_handler=model_stack_worker.stack_work_handler,
                                           job_id='Another job',
                                           stacks_to_spawn=100,
                                           stack_number=100)
                stack_workers.append(stack_worker)
                stack_worker._test_hooks = {}  # Clear hooks
                stack_worker._deploy_proxies()
            proxy_event.set()  # Allow others to deploy proxies now

        # Test proxy re-use
        # Test is use proxies won't be removed
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
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
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        proxy_event = Event()
        hooks = {'pre_proxy_deployment': lambda x: _wait_for_event(proxy_event),
                 # This will make the scrubber stacks to wait forever before deploying proxies
                 'post_stack_worker_deployment': lambda x: _inject_race(x)}
        ScrubShared._test_hooks.update(hooks)
        GenericController.execute_scrub()
        logs = LogHandler._logs['lib_generic tasks scrub']
        # Still working with only 1 proxy service
        re_use_logs = ['Re-using existing proxy services {0}'.format(proxy_name) for proxy_name in proxy_names]
        remove_logs = ['Cannot remove services {0} as it is still in use by others'.format(proxy_name) for proxy_name in
                       proxy_names]
        stack_amount = len(vpools) * len(
            storagerouters)  # A stack is spawned for every vpool/storagerouter (if permitted by max stack constraint)
        self.assertEquals(len([log for log in logs if log.endswith(tuple(re_use_logs))]), stack_amount,
                          'All deployed stacks should be re-using the proxies')
        self.assertEquals(len([log for log in logs if log.endswith(tuple(remove_logs))]), stack_amount,
                          'All deployed stacks should be not be touching the proxies')
        service_manager = ServiceFactory.get_manager()
        for stack_worker in stack_workers:
            client = SSHClient(stack_worker.storagerouter, 'root')
            for alba_proxy_service in stack_worker.alba_proxy_services:
                self.assertTrue(service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active',
                                'The proxy should be still running')
        for stack_worker in stack_workers:
            stack_worker._remove_proxies()
        for stack_worker in stack_workers:
            client = SSHClient(stack_worker.storagerouter, 'root')
            for alba_proxy_service in stack_worker.alba_proxy_services:
                self.assertFalse(stack_worker._service_manager.has_service(name=alba_proxy_service, client=client),
                                 'The proxy should be removed')

    def test_scrubbing_registration(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate if a vdisk is properly registered for scrubbing
        - Validate that proxies will be re-used
        - Validate that proxies won't be removed when still in use
        """
        vdisks_scrub_status = []
        vdisk_scrub_status_unregistration = []

        def _check_vdisk_for_scrub_registration(self, vdisk_guid):
            """
            Hooking which will register if the vdisk is marked for being scrubbed
            :param vdisk_guid: Guid of the VDisk being processed (given by the hook)
            """
            vdisk = VDisk(vdisk_guid)
            vdisks_scrub_status.append((vdisk, vdisk.being_scrubbed))

        def _check_vdisk_for_scrub_unregistration(self, vdisk_guid):
            """
            Hooking which will register if the vdisk is marked for being scrubbed
            :param vdisk_guid: Guid of the VDisk being processed (given by the hook)
            """
            vdisk = VDisk(vdisk_guid)
            vdisk.invalidate_dynamics('being_scrubbed')
            vdisk_scrub_status_unregistration.append((vdisk, vdisk.being_scrubbed))

        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        hooks = {'post_vdisk_scrub_registration': _check_vdisk_for_scrub_registration,
                 # This will make the scrubber stacks to wait forever before deploying proxies
                 'post_vdisk_scrub_unregistration': _check_vdisk_for_scrub_unregistration}
        ScrubShared._test_hooks.update(hooks)
        GenericController.execute_scrub()

        for vdisk, scrub_status in vdisks_scrub_status:
            self.assertTrue(scrub_status, 'VDisk should have been marked that it is being scrubbed')
        for vdisk, scrub_status in vdisk_scrub_status_unregistration:
            self.assertFalse(scrub_status, 'VDisk should have been marked that it is not being scrubbed')

    def test_scrubbing_registration(self):
        """
        2 vPools, 20 vDisks, 2 scrub roles
        Validate if a vdisk is properly registered for scrubbing
        - Validate that proxies will be re-used
        - Validate that proxies won't be removed when still in use
        """
        vdisks_scrub_status = []
        vdisk_scrub_status_unregistration = []

        def _check_vdisk_for_scrub_registration(self, vdisk_guid):
            """
            Hooking which will register if the vdisk is marked for being scrubbed
            :param vdisk_guid: Guid of the VDisk being processed (given by the hook)
            """
            vdisk = VDisk(vdisk_guid)
            vdisks_scrub_status.append((vdisk, vdisk._being_scrubbed()))

        def _check_vdisk_for_scrub_unregistration(self, vdisk_guid):
            """
            Hooking which will register if the vdisk is marked for being scrubbed
            :param vdisk_guid: Guid of the VDisk being processed (given by the hook)
            """
            vdisk = VDisk(vdisk_guid)
            vdisk.invalidate_dynamics('being_scrubbed')
            vdisk_scrub_status_unregistration.append((vdisk, vdisk._being_scrubbed()))

        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1), (3, 1, 1, 1), (4, 1, 1, 1), (5, 1, 1, 1),
                        (6, 2, 1, 2), (7, 2, 1, 2), (8, 2, 1, 2), (9, 2, 1, 2), (10, 2, 1, 2),
                        (11, 3, 2, 3), (12, 3, 2, 3), (13, 3, 2, 3), (14, 3, 2, 3), (15, 3, 2, 3),
                        (16, 4, 2, 4), (17, 4, 2, 4), (18, 4, 2, 4), (19, 4, 2, 4), (20, 4, 2, 4)],
             'mds_services': [(1, 1), (2, 2), (3, 3), (4, 4)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        vdisks = structure['vdisks']
        # Amount of actual threads calculation:
        #   - Threads per VPool * vPools * 2 threads per StorageRouter
        #   - Threads per vPool is 2 when 2 vPools and 2 StorageRouters
        #   - Amount of threads that will be created: 2 * 2 * 2 = 8
        # Amount of possible threads calculation:
        #   - vPools * StorageRouters * 2 threads per StorageRouter
        #   - Amount of possible threads to be created: 2 * 2 * 2 = 8
        proxy_names, thread_names, vdisk_namespaces = self.generate_scrub_related_info(structure)
        LockedClient.scrub_controller = {'possible_threads': thread_names,
                                         'volumes': {},
                                         'waiter': Waiter(len(thread_names))}

        # Scrub all volumes
        for vdisk_id, vdisk in vdisks.iteritems():
            LockedClient.scrub_controller['volumes'][vdisk.volume_id] = {'success': True,
                                                                         'scrub_work': range(vdisk_id)}
        hooks = {'post_vdisk_scrub_registration': _check_vdisk_for_scrub_registration,
                 # This will make the scrubber stacks to wait forever before deploying proxies
                 'post_vdisk_scrub_unregistration': _check_vdisk_for_scrub_unregistration}
        ScrubShared._test_hooks.update(hooks)
        GenericController.execute_scrub()

        for vdisk, scrub_status in vdisks_scrub_status:
            self.assertTrue(scrub_status, 'VDisk should have been marked that it is being scrubbed')
        for vdisk, scrub_status in vdisk_scrub_status_unregistration:
            self.assertFalse(scrub_status, 'VDisk should have been marked that it is not being scrubbed')

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

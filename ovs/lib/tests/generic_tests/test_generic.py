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
import time
import datetime
import unittest
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.log.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs_extensions.generic.tests.sshclient_mock import MockedSSHClient
from ovs.lib.generic import GenericController
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.vdisk import VDiskController


class Generic(unittest.TestCase):
    """
    This test class will validate the various scenarios of the Generic logic
    """
    _logger = Logger('unittest')

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
                                                 internal=internal)
                arakoon_installer.start_cluster()
                arakoon_installer.extend_cluster(new_ip=storagerouter_2.ip,
                                                 base_dir=base_dir)

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
        generic_logs = Logger._logs.get('lib', {})
        for cluster_name in successful_clusters + failed_clusters + external_clusters:
            collect_msg = ('DEBUG', 'Collecting info for cluster {0}'.format(cluster_name))
            unreachable_msg = ('ERROR', 'Could not collapse any cluster on {0} (not reachable)'.format(storagerouter_2.name))
            end_collapse_msg = ('DEBUG', 'Collapsing cluster {0} on {1} completed'.format(cluster_name, storagerouter_1.ip))
            start_collapse_msg = ('DEBUG', 'Collapsing cluster {0} on {1}'.format(cluster_name, storagerouter_1.ip))
            failed_collapse_msg = ('ERROR', 'Collapsing cluster {0} on {1} failed'.format(cluster_name, storagerouter_1.ip))
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
        def _update_info_cluster_1(client, update_info, package_info):
            _ = package_info
            update_info[client.ip]['framework'] = {'packages': {'package1': {'candidate': 'version2',
                                                                              'installed': 'version1'}},
                                                   'prerequisites': []}

        def _update_info_cluster_2(client, update_info, package_info):
            _ = package_info
            update_info[client.ip]['component2'] = {'packages': {'package2': {'candidate': 'version2',
                                                                              'installed': 'version1'}},
                                                    'prerequisites': []}
            if client.ip == storagerouter_3.ip:
                update_info[client.ip]['errors'] = ['Unexpected error occurred for StorageRouter {0}'.format(storagerouter_3.name)]

        def _update_info_plugin_1(error_information):
            _ = error_information  # get_update_info_plugin is used for Alba nodes, so not testing here

        expected_package_info = {'framework': {'packages': {'package1': {'candidate': 'version2', 'installed': 'version1'}},
                                               'prerequisites': [['node_down', '2']]},
                                 'component2': {'packages': {'package2': {'candidate': 'version2', 'installed': 'version1'}},
                                                'prerequisites': []}}


        # StorageRouter 1 successfully updates its package info
        # StorageRouter 2 is inaccessible
        # StorageRouter 3 gets error in 2nd hook --> package_information is reset to {}
        structure = DalHelper.build_dal_structure(structure={'storagerouters': [1, 2, 3]})
        storagerouter_1 = structure['storagerouters'][1]
        storagerouter_2 = structure['storagerouters'][2]
        storagerouter_3 = structure['storagerouters'][3]
        Toolbox._function_pointers['update-get_update_info_cluster'] = [_update_info_cluster_1, _update_info_cluster_2]
        Toolbox._function_pointers['update-get_update_info_plugin'] = [_update_info_plugin_1]

        SSHClient._raise_exceptions[storagerouter_2.ip] = {'users': ['root'],
                                                           'exception': UnableToConnectException('No route to host')}

        with self.assertRaises(excClass=Exception) as raise_info:
            GenericController.refresh_package_information()

        storagerouter_1.discard()
        storagerouter_2.discard()
        storagerouter_3.discard()
        self.assertDictEqual(d1=expected_package_info,
                             d2=storagerouter_1.package_information,
                             msg='Incorrect package information found for StorageRouter 1'.format(storagerouter_1.name))
        self.assertDictEqual(d1={},
                             d2=storagerouter_2.package_information,
                             msg='Incorrect package information found for StorageRouter 2'.format(storagerouter_2.name))
        self.assertDictEqual(d1={},
                             d2=storagerouter_3.package_information,
                             msg='Incorrect package information found for StorageRouter {0}'.format(storagerouter_3.name))
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

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
import logging
import datetime
import unittest
from ovs.constants.logging import UNITTEST_LOGGER
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


class Generic(unittest.TestCase):
    """
    This test class will validate the various scenarios of the Generic logic
    """
    _logger = logging.getLogger(UNITTEST_LOGGER)

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

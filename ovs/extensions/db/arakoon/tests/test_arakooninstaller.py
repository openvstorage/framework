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
Module for testing the ArakoonInstaller
"""
import os
import json
import shutil
import unittest
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.tests.helpers import ExtensionsHelper


class ArakoonInstallerTester(unittest.TestCase):
    """
    This test class will validate the various scenarios of the MDSService logic
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        DalHelper.setup()

    def tearDown(self):
        """
        Clean up test suite
        """
        DalHelper.teardown()

    def test_cluster_maintenance(self):
        """
        Validates whether a cluster can be correctly created
        """
        structure = DalHelper.build_dal_structure(structure={'storagerouters': [1, 2]})
        storagerouters = structure['storagerouters']

        # Create new cluster
        mountpoint = storagerouters[1].disks[0].partitions[0].mountpoint
        if os.path.exists(mountpoint) and mountpoint != '/':
            shutil.rmtree(mountpoint)
        base_dir = mountpoint + '/test_create_cluster'
        info = ArakoonInstaller.create_cluster(cluster_name='test',
                                               cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK,
                                               ip=storagerouters[1].ip,
                                               base_dir=base_dir)

        reality = ExtensionsHelper.extract_dir_structure(base_dir)
        expected = {'dirs': {'arakoon': {'dirs': {'test': {'dirs': {'tlogs': {'dirs': {},
                                                                              'files': []},
                                                                    'db': {'dirs': {},
                                                                           'files': []}},
                                                           'files': []}},
                                         'files': []}},
                    'files': []}
        self.assertDictEqual(reality, expected)
        expected = '{0}\n\n{1}\n\n'.format(ArakoonInstallerTester.EXPECTED_CLUSTER_CONFIG.format('1', 'test', ''),
                                           ArakoonInstallerTester.EXPECTED_NODE_CONFIG.format(
                                               '1', storagerouters[1].ip, 20000, base_dir, '1', 20001
                                           ))
        self.assertEqual(Configuration.get(ArakoonClusterConfig.CONFIG_KEY.format('test'), raw=True), expected)
        # @TODO: assert service availability here. It should be stopped

        ArakoonInstaller.start_cluster(metadata=info['metadata'])
        # @TODO: assert the service is running

        config = ArakoonClusterConfig(cluster_id='test')
        client = ArakoonInstaller.build_client(config)
        reality = client.get(ArakoonInstaller.INTERNAL_CONFIG_KEY)
        self.assertEqual(reality, expected)
        self.assertFalse(client.exists(ArakoonInstaller.METADATA_KEY))

        ArakoonInstaller.claim_cluster(cluster_name='test')

        reality = json.loads(client.get(ArakoonInstaller.METADATA_KEY))
        expected = {'cluster_name': 'test',
                    'cluster_type': 'FWK',
                    'in_use': True,
                    'internal': True}
        self.assertDictEqual(reality, expected)

        # Extending cluster
        mountpoint = storagerouters[2].disks[0].partitions[0].mountpoint
        if os.path.exists(mountpoint) and mountpoint != '/':
            shutil.rmtree(mountpoint)
        base_dir2 = mountpoint + '/test_extend_cluster'
        ArakoonInstaller.extend_cluster(cluster_name='test',
                                        new_ip=storagerouters[2].ip,
                                        base_dir=base_dir2)
        reality = ExtensionsHelper.extract_dir_structure(base_dir)
        expected = {'dirs': {'arakoon': {'dirs': {'test': {'dirs': {'tlogs': {'dirs': {},
                                                                              'files': []},
                                                                    'db': {'dirs': {},
                                                                           'files': []}},
                                                           'files': []}},
                                         'files': []}},
                    'files': []}
        self.assertDictEqual(reality, expected)
        expected = '{0}\n\n{1}\n\n{2}\n\n'.format(ArakoonInstallerTester.EXPECTED_CLUSTER_CONFIG.format('1,2', 'test', ''),
                                                  ArakoonInstallerTester.EXPECTED_NODE_CONFIG.format(
                                                      '1', storagerouters[1].ip, 20000, base_dir, '1', 20001
                                                  ),
                                                  ArakoonInstallerTester.EXPECTED_NODE_CONFIG.format(
                                                      '2', storagerouters[2].ip, 20100, base_dir2, '2', 20101
                                                  ))
        self.assertEqual(Configuration.get(ArakoonClusterConfig.CONFIG_KEY.format('test'), raw=True), expected)
        # @TODO: assert service availability here. It should be stopped

        catchup_command = 'arakoon --node 2 -config file://opt/OpenvStorage/config/framework.json?key=/ovs/arakoon/test/config -catchup-only'
        SSHClient._run_returns[catchup_command] = None
        SSHClient._run_recordings = []
        ArakoonInstaller.restart_cluster_add(cluster_name='test',
                                             current_ips=[storagerouters[1].ip],
                                             new_ip=storagerouters[2].ip)
        self.assertIn(catchup_command, SSHClient._run_recordings)
        # @TODO: assert the service is running

        config = ArakoonClusterConfig(cluster_id='test')
        client = ArakoonInstaller.build_client(config)
        reality = client.get(ArakoonInstaller.INTERNAL_CONFIG_KEY)
        self.assertEqual(reality, expected)

        reality = json.loads(client.get(ArakoonInstaller.METADATA_KEY))
        expected = {'cluster_name': 'test',
                    'cluster_type': 'FWK',
                    'in_use': True,
                    'internal': True}
        self.assertDictEqual(reality, expected)

        # Shrinking cluster
        ArakoonInstaller.shrink_cluster(cluster_name='test',
                                        ip=storagerouters[1].ip)
        reality = ExtensionsHelper.extract_dir_structure(base_dir)
        expected = {'dirs': {'arakoon': {'dirs': {'test': {'dirs': {}, 'files': []}},
                                         'files': []}},
                    'files': []}
        self.assertDictEqual(reality, expected)
        expected = '{0}\n\n{1}\n\n'.format(ArakoonInstallerTester.EXPECTED_CLUSTER_CONFIG.format('2', 'test', ''),
                                           ArakoonInstallerTester.EXPECTED_NODE_CONFIG.format(
                                               '2', storagerouters[2].ip, 20100, base_dir2, '2', 20101
                                           ))
        self.assertEqual(Configuration.get(ArakoonClusterConfig.CONFIG_KEY.format('test'), raw=True), expected)
        # @TODO: assert service availability here. It should have been stopped and started again

        config = ArakoonClusterConfig(cluster_id='test')
        client = ArakoonInstaller.build_client(config)
        reality = client.get(ArakoonInstaller.INTERNAL_CONFIG_KEY)
        self.assertEqual(reality, expected)

        reality = json.loads(client.get(ArakoonInstaller.METADATA_KEY))
        expected = {'cluster_name': 'test',
                    'cluster_type': 'FWK',
                    'in_use': True,
                    'internal': True}
        self.assertDictEqual(reality, expected)

    EXPECTED_CLUSTER_CONFIG = """[global]
cluster = {0}
cluster_id = {1}
plugins = {2}
tlog_max_entries = 5000"""

    EXPECTED_NODE_CONFIG = """[{0}]
client_port = {2}
crash_log_sinks = console:
fsync = true
home = {3}/arakoon/test/db
ip = {1}
log_level = info
log_sinks = console:
messaging_port = {5}
name = {4}
tlog_compression = snappy
tlog_dir = {3}/arakoon/test/tlogs"""

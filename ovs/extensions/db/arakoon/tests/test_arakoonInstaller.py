# Copyright 2015 iNuron NV
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

import sys
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
from ovs.extensions.generic.system import System
from ovs.extensions.generic.sshclient import SSHClient
from unittest import TestCase


class TestArakoonInstaller(TestCase):

    nodes = None
    expected_global = None
    expected_base = None
    cluster_name = None
    cluster_config_file = None
    cluster_config_path = None

    @classmethod
    def setUpClass(cls):
        TestArakoonInstaller.cluster_name = 'unit_test_cluster'
        TestArakoonInstaller.cluster_config_path = '/opt/OpenvStorage/config/arakoon/{0}/'.format(TestArakoonInstaller.cluster_name)
        TestArakoonInstaller.cluster_config_file = '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'.format(TestArakoonInstaller.cluster_name)
        TestArakoonInstaller.expected_global = '[global]\ncluster = {0}\ncluster_id = {1}\nplugins =\n\n'
        TestArakoonInstaller.expected_base = '[{0}]\ntlog_compression = snappy\nclient_port = {1}\nmessaging_port = {2}\nname = {0}\nfsync = true\nhome = /tmp/db/arakoon/{3}/db\nip = {4}\nlog_level = info\ntlog_dir = /tmp/db/arakoon/{3}/tlogs\nlog_dir = /var/log/arakoon/{3}\n\n'

        def _get_my_machine_id(_client):
            return TestArakoonInstaller.nodes[_client.ip]

        System.get_my_machine_id = staticmethod(_get_my_machine_id)

    @classmethod
    def setUp(cls):
        for node in TestArakoonInstaller.nodes:
            client = SSHClient(node)
            root_client = SSHClient(node, username='root')
            root_client.dir_delete('/tmp/db')
            root_client.dir_create('/tmp/db')
            client.dir_delete(TestArakoonInstaller.cluster_config_path)
            client.dir_create(TestArakoonInstaller.cluster_config_path)

    def test_single_node(self):
        node = sorted(TestArakoonInstaller.nodes.keys())[0]
        result = ArakoonInstaller.create_cluster(TestArakoonInstaller.cluster_name, node, [], '/tmp/db')
        contents = SSHClient(node).file_read(TestArakoonInstaller.cluster_config_file)
        expected = TestArakoonInstaller.expected_global.format(TestArakoonInstaller.nodes[node], TestArakoonInstaller.cluster_name)
        expected += TestArakoonInstaller.expected_base.format(TestArakoonInstaller.nodes[node], result['client_port'], result['messaging_port'], TestArakoonInstaller.cluster_name, node)
        self.assertEqual(contents.strip(), expected.strip())

    def test_multi_node(self):
        nodes = sorted(TestArakoonInstaller.nodes.keys())
        nodes = dict((node, SSHClient(node)) for node in nodes)
        first_node = nodes.keys()[0]
        result = ArakoonInstaller.create_cluster(TestArakoonInstaller.cluster_name, first_node, [], '/tmp/db')
        for node in nodes.keys()[1:]:
            ArakoonInstaller.extend_cluster(first_node, node, TestArakoonInstaller.cluster_name, [], '/tmp/db')
        expected = TestArakoonInstaller.expected_global.format(','.join(TestArakoonInstaller.nodes[node] for node in nodes), TestArakoonInstaller.cluster_name)
        for node in nodes:
            expected += TestArakoonInstaller.expected_base.format(TestArakoonInstaller.nodes[node], result['client_port'], result['messaging_port'], TestArakoonInstaller.cluster_name, node)
        expected = expected.strip()
        for node, client in nodes.iteritems():
            contents = client.file_read(TestArakoonInstaller.cluster_config_file)
            self.assertEqual(contents.strip(), expected.strip())
        ArakoonInstaller.shrink_cluster(nodes.keys()[1], first_node, TestArakoonInstaller.cluster_name)
        expected = TestArakoonInstaller.expected_global.format(','.join(TestArakoonInstaller.nodes[node] for node in nodes[1:]), TestArakoonInstaller.cluster_name)
        for node in nodes.keys()[1:]:
            expected += TestArakoonInstaller.expected_base.format(TestArakoonInstaller.nodes[node], result['client_port'], result['messaging_port'], TestArakoonInstaller.cluster_name, node)
        expected = expected.strip()
        for node, client in nodes.iteritems():
            if node == first_node:
                continue
            contents = client.file_read(TestArakoonInstaller.cluster_config_file)
            self.assertEqual(contents.strip(), expected.strip())


if __name__ == '__main__':
    import unittest
    TestArakoonInstaller.nodes = {}
    if len(sys.argv) == 1:
        print 'Please specify the IP addresses of the OVS nodes to run these tests on.'
        print 'Make sure you have all your code-to-be-tested copied to these nodes'
    for arg in sys.argv[1:]:
        TestArakoonInstaller.nodes[arg] = arg.replace('.', '')
    suite = unittest.TestLoader().loadTestsFromTestCase(TestArakoonInstaller)
    unittest.TextTestRunner(verbosity=2).run(suite)

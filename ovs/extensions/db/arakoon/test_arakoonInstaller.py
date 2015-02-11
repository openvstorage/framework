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

from ArakoonInstaller import ArakoonInstaller
from unittest import TestCase

import os
import pprint


class TestArakoonInstaller(TestCase):

    def setUp(self):
        self.ARAKOON_CONFIG_DIR = '/opt/OpenvStorage/config/arakoon'
        self.ai = ArakoonInstaller()
        self.base_dir = '/mnt/db'
        self.cluster_name = 'abm_0'
        self.ip = '10.100.131.62'
        self.ip2 = '10.100.131.61'
        self.ip3 = '10.100.131.63'
        self.name2 = '005056a3f5a7'
        self.name3 = '005056a33fa8'
        self.node_id = 'abm_0'
        self.client_port = 8870
        self.messaging_port = 8871
        self._create_config()

    def tearDown(self):
        # remove generated files
        for config in ['abm_0', 'new_abm_0']:
            for suffix in ['.cfg', '_client.cfg', '_local_nodes.cfg']:
                filename = "{0}/{1}/{1}{2}".format(self.ARAKOON_CONFIG_DIR, config, suffix)
                if os.path.exists(filename):
                    os.remove(filename)
            filedir = '{0}/{1}'.format(self.ARAKOON_CONFIG_DIR, config)
            if os.path.exists(filedir):
                os.removedirs(filedir)

    def _print(self, value):
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(vars(value))

    def _print_config(self):
        print "\n ###################### \n"
        self._print(self.ai.config)
        for node in self.ai.config.nodes:
            print 'Node:'
            self._print(node)

    def _create_config(self):
        self.ai.clear_config()
        self.ai.create_config('/mnt/db', 'abm_0', '10.100.131.62', '8870', '8871',
                              ArakoonInstaller.ABM_PLUGIN)

    def _print_config_files_for(self, cluster_name):
        config_dir = '/'.join([self.ARAKOON_CONFIG_DIR, cluster_name])
        for suffix in '_local_nodes.cfg', '_client.cfg', '.cfg':
            filename = '/'.join([config_dir, cluster_name + suffix])
            print filename + ':'
            with open(filename, 'r') as f:
                print f.read()

    def test_create_config(self):
        self._create_config()
        self._print_config()

    def test_generate_config(self):
        self.ai.generate_config()

    def test_generate_client_config(self):
        self.ai.generate_client_config()

    def test_generate_local_nodes_config(self):
        self.ai.generate_local_nodes_config()

    def test_load_master_config_from(self):
        self.ai.clear_config()
        self._create_config()
        self.ai.generate_config()
        self.ai.load_config_from(self.base_dir, self.cluster_name, self.ip)

    def test_1_node_install(self):
        self.ai.clear_config()
        self._create_config()
        self.ai.generate_configs()

        self.ai.load_config_from(self.base_dir, self.cluster_name, self.ip)
        self.ai.generate_config()
        self.ai.generate_client_config()
        self.ai.generate_local_nodes_config()
        print "### Single node configuration files:"
        self._print_config_files_for(self.cluster_name)

    def test_3_node_install(self):
        client_port = self.client_port
        messaging_port = self.messaging_port
        self.ai.clear_config()
        self._create_config()

        self.ai.add_node_to_config(self.name2, self.ip2, client_port, messaging_port)
        self.ai.add_node_to_config(self.name3, self.ip3, client_port, messaging_port)

        self.ai.generate_config()
        self.ai.generate_client_config()
        self.ai.generate_local_nodes_config()
        print "### Three node configuration files:"
        self._print_config_files_for(self.cluster_name)

    def test_duplicate_cluster(self):
        src_cluster = 'abm_0'
        tgt_cluster = 'abm_2'
        self.ai.clear_config()
        self._create_config()
        self.ai.generate_configs()

        self.ai.clone_cluster('10.100.131.62', src_cluster, tgt_cluster)
        self._print_config_files_for(tgt_cluster)

    def test_exclude_ports(self):
        # @todo: to implement
        pass

    def test_get_client_config(self):
        self.ai.clear_config()
        self._print(self.ai.get_client_config_from('10.100.131.62', 'abm_2'))

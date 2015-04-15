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

from unittest import TestCase
from ovs.extensions.generic.system import System
from ovs.extensions.generic.sshclient import SSHClient


class TestSystem(TestCase):
    multiple_port_range = '5000-6000,7000-7500,8000-9000'
    single_port = '1500'
    single_port_range = '8870-8880'
    ip = '127.0.0.1'

    def setup(self):
        pass

    def test_no_free_port_can_be_found_within_system_range(self):
        self.assertRaises(ValueError, System.get_free_ports, xrange(0, 1024))

    def test_first_free_port_after_system_range_succeeds(self):
        result = System.get_free_ports(1025)
        self.assertTrue(result == [1025], 'Expected 1025 got: {0}'.format(result))

    def test_get_1_free_port(self):
        result = System.get_free_ports(self.single_port_range, range(8870, 8874))
        self.assertTrue(result == [8874], 'Expected 8874 got: {0}'.format(result))

    def test_get_2_free_ports(self):
        result = System.get_free_ports(self.single_port_range, range(8870, 8874), 2)
        self.assertTrue(result == [8874, 8875], 'Expected 8874 got: {0}'.format(result))

    def test_support_for_multiple_port_ranges(self):
        result = System.get_free_ports(self.multiple_port_range, range(3000, 8873), 2)
        self.assertTrue(result == [8874, 8875], 'Expected: {0} got: {1}'.format(self.single_port_range, result))

    def test_check_if_single_port_is_free(self):
        result = System.get_free_ports(self.single_port)
        self.assertTrue(result == [1500], 'Expected: {0}, got: {1}'.format(self.single_port, result))

    def test_local_remote_check(self):
        local_result = System.get_free_ports(self.single_port_range)
        client = SSHClient.load(self.ip)
        remote_result = System.get_free_ports(self.single_port_range, client=client)
        self.assertEqual(local_result, remote_result)

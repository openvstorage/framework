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
Test module for the System class
"""
import unittest
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System


class TestSystem(unittest.TestCase):
    multiple_port_range = [[5000, 6000], [7000, 7500], [8000, 9000]]
    single_port = [1500]
    single_port_range = [[8870, 8880]]
    ip = '127.0.0.1'

    def test_no_free_port_can_be_found_within_system_range(self):
        self.assertRaises(ValueError, System.get_free_ports, [range(0, 1024)])

    def test_first_free_port_after_system_range_succeeds(self):
        result = System.get_free_ports([1025])
        expected = [1025]
        self.assertTrue(result == expected, 'Expected {0} got: {1}'.format(expected, result))

    def test_get_1_free_port(self):
        result = System.get_free_ports(self.single_port_range, range(8870, 8874))
        expected = [8874]
        self.assertTrue(result == expected, 'Expected {0} got: {1}'.format(expected, result))

    def test_get_2_free_ports(self):
        result = System.get_free_ports(self.single_port_range, range(8870, 8874), 2)
        expected = [8874, 8875]
        self.assertTrue(result == expected, 'Expected {0} got: {1}'.format(expected, result))

    def test_support_for_multiple_port_ranges(self):
        result = System.get_free_ports(self.multiple_port_range, range(3000, 8874), 2)
        expected = [8874, 8875]
        self.assertTrue(result == expected, 'Expected: {0} got: {1}'.format(expected, result))

    def test_check_if_single_port_is_free(self):
        result = System.get_free_ports(self.single_port)
        expected = [1500]
        self.assertTrue(result == expected, 'Expected: {0}, got: {1}'.format(expected, result))

    def test_local_remote_check(self):
        local_result = System.get_free_ports(self.single_port_range)
        client = SSHClient(self.ip)
        remote_result = System.get_free_ports(self.single_port_range, client=client)
        self.assertEqual(local_result, remote_result)

# Copyright 2014 Open vStorage NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Unit test to validate different stdout / stderr behaviours of specific commands
- alba cli command - redirects it's debug output to stderr
-
"""

import logging
import unittest
from ovs.extensions.generic.system import System
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.plugins.albacli import AlbaCLI
from subprocess import CalledProcessError

import pprint


class TestSshClient(unittest.TestCase):

    def _get_my_ip(self):
        try:
            return System.get_my_storagerouter().ip
        except:
            return '127.0.0.1'

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """

    def _get_SSHClient(self, ip, user, password=None):
        return SSHClient(ip, user, password)

    def execute(self, command, ip='127.0.0.1', user='root', debug=False):
        client = self._get_SSHClient(ip, user, debug)
        output = client.run(command, debug=debug)
        pprint.pprint(output)
        return output

    def test_os_error(self):
        """
        Assert OSError is handled properly
        """
        with self.assertRaises(CalledProcessError):
            self.execute('lsd -la /var/tmp', user='ovs', debug=True)

    def test_debug_flag(self):
        with_debug_output = AlbaCLI.run('list-namespaces',
                                        config='/opt/OpenvStorage/config/arakoon/alba-abm/alba-abm.cfg',
                                        as_json=True, debug=True, client=self._get_SSHClient(self._get_my_ip(), 'root'))
        with_debug_length = len(with_debug_output[0]) + len(with_debug_output[1])
        without_debug_output = AlbaCLI.run('list-namespaces',
                                           config='/opt/OpenvStorage/config/arakoon/alba-abm/alba-abm.cfg',
                                           as_json=True, debug=False,
                                           client=self._get_SSHClient(self._get_my_ip(), 'root'))
        without_debug_length = len(without_debug_output)

        assert with_debug_length > without_debug_length,\
            "additional logging expected with debug=True:\n {0}\n{1}".format(with_debug_output, without_debug_output)

    def test_remote_path(self):
        output = self.execute('ls -la /var/tmp', ip=self._get_my_ip(), user='ovs', debug=True)
        assert len(output) > 0, "Output expected but nothing received"

    def test_alba_cli(self):
        output = AlbaCLI.run('list-namespaces', config='/opt/OpenvStorage/config/arakoon/alba-abm/alba-abm.cfg',
                             as_json=True, debug=True, client=self._get_SSHClient(self._get_my_ip(), 'root'))
        logging.log(1, "alba_cli output: {0}".format(output))


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSshClient)
    unittest.TextTestRunner(verbosity=2).run(suite)

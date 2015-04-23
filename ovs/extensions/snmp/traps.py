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

from ovs.extensions.snmp.trapsender import SNMPTrapSender

"""
OVS defined traps
"""
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.exceptions import KeyNotFoundException


STORAGE_PREFIX = "ovs_snmp"

class OVSTraps():
    """
    OVS TRAPS
    """
    def __init__(self):
        """
        Init
        """
        self.persistent = PersistentFactory.get_client()
        trap_host_port = "{}_config_trap_target".format(STORAGE_PREFIX)
        self.sender = None

        try:
            target_host, port = self.persistent.get(trap_host_port)
        except KeyNotFoundException:
            print('OVS SNMP Target not configured, cannot send TRAP')
        else:
            self.sender = SNMPTrapSender(target_host, port)
            # security from model
            self.sender.security_public()

    def set_trap_target(self, host, port):
        """
        Add a TRAP target to model
        """
        trap_host_port = "{}_config_trap_target"
        self.persistent.set(trap_host_port, (host, port))

        self.sender = SNMPTrapSender(target_host, port)
        # security from model
        self.sender.security_public()

    def _get_oid(self, trap):
        """
        Return oid for TRAP type + default message
        """
        # Read from arakoon/json cfg
        TRAPS = {'KEEPALIVE': ("1.3.6.1.4.1.1.0.0.0", 'OVS keepalive')}
        if not trap in TRAPS:
            raise ValueError('Undefined trap type {}'.format(trap))
        oid, default = TRAPS[trap]
        return oid, default

    def send(self, trap = 'KEEPALIVE', message = None):
        """
        Send a predefined trap
        """
        if not self.sender:
            raise RuntimeError('OVS SNMP Target not configured, cannot send TRAP')
        oid, default = self._get_oid(trap)
        if message:
            default = message
        self.sender.send(oid, default)

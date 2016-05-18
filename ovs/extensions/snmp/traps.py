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
OVS defined traps
"""
from ovs.extensions.snmp.trapsender import SNMPTrapSender
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

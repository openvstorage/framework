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
SNMP TRAP Sender
"""
from pysnmp.entity.rfc3413.oneliner import ntforg
from pysnmp.proto import rfc1902
from pysnmp.entity import engine


class SNMPTrapSender():
    """
    Send SNMP TRAP Message
    """
    def __init__(self, host, port = 162):
        self.host = host
        self.port = port
        self.authData = None
        #TODO: engine id customizable
        # engine id makes sense in v3 context, using USM
        #  SNMPv3 with the User-Based Security Model (USM)
        #  makes use of an EngineID identifier for the SNMPv3 application
        #  that is authoritative (meaning the one who controls the flow of information).
        snmpEngineId = rfc1902.OctetString(hexValue='0000000000000000')
        self.ntfOrg = ntforg.NotificationOriginator(engine.SnmpEngine(snmpEngineId))

    def send(self, mib, value, value_type='OctetString'):
        """
        v1 snmp, public
        """
        if not self.authData:
            raise ValueError('Credentials not set, use .security_XXX() methods')
        obj_class = getattr(rfc1902, value_type)
        errorIndication = self.ntfOrg.sendNotification(self.authData,
                                                       ntforg.UdpTransportTarget((self.host, self.port)), #transportTarget
                                                       'trap', #notifyType
                                                       ntforg.MibVariable('SNMPv2-MIB', 'snmpOutTraps'), #notificationType
                                                       ((rfc1902.ObjectName(mib),
                                                         obj_class(value))))

        if errorIndication:
            raise RuntimeError('Notification not sent: %s' % errorIndication)
        print('Sent SNMP TRAP {} "{}" to {} {}'.format(mib, value, self.host, self.port))

    def security_public(self, community_string = 'public'):
        """
        v1 snmp, insecure
        """
        self.authData, = ntforg.CommunityData(community_string, mpModel=0), # authData

    def security_aes128(self, user, authkey, privkey):
        """
        v3 snmp, secure
        """
        self.authData, = ntforg.UsmUserData(user, authkey, privkey,
                                            authProtocol=ntforg.usmHMACSHAAuthProtocol,
                                            privProtocol=ntforg.usmAesCfb128Protocol)

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

from pysnmp.entity.rfc3413.oneliner import ntforg
from pysnmp.proto import rfc1902
from pysnmp.entity import engine

"""
SNMP TRAP Sender
"""


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

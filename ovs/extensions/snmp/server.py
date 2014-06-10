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

"""
SNMP Server module
"""

from pysnmp.entity import engine, config
from pysnmp.entity.rfc3413 import cmdrsp, context
from pysnmp.carrier.asynsock.dgram import udp
from pysnmp.proto.api import v2c
from pysnmp.smi.error import SmiError



class SNMPServer():
    """
    SNMP Server - command responder
    responds to snmp queries
    standard oids implemented by pysnmp
    custom oids implemented either in custom class or by browsing dal/model
    """
    def __init__(self, host, port, users, naming_scheme):
        """
        host = public ip to listen on
        port = port to listen on (usually 161)
        users = list of ('username', 'password', 'privatekey', 'authPriv') #authentication method for snmp v3
        if users is None, authentication will be snmp v1 public community string, read only
        """
        self.naming_scheme = naming_scheme

        self.run = True
        self.users = users
        # Create SNMP engine
        self.snmpEngine = engine.SnmpEngine()
        # Get default SNMP context this SNMP engine serves
        self.snmpContext = context.SnmpContext(self.snmpEngine)
        # MIB builder
        self.mibBuilder = self.snmpContext.getMibInstrum().getMibBuilder()
        self.MibScalar, self.MibScalarInstance = self.mibBuilder.importSymbols('SNMPv2-SMI', 'MibScalar', 'MibScalarInstance')

        # Transport setup
        # UDP over IPv4
        config.addSocketTransport(self.snmpEngine,
                                  udp.domainName,
                                  udp.UdpTransport().openServerMode((host, port)))

        # SNMPv3/USM setup
        # user: usr-md5-des, auth: MD5, priv DES
        if users:
            for user in users:
                self._add_v3_md5_des_user(user)
                # Allow full MIB access for each user at VACM
        else:
            # SNMPv1 public community string setup
            config.addV1System(self.snmpEngine, 'my-read-area', 'public')

        self._add_user_permission("1.3.6.1.2.1") #full walk permission, without this snmpwalk returns None

        # Overwrite default strings with custom name
        sysDescr, = self.snmpEngine.msgAndPduDsp.mibInstrumController.mibBuilder.importSymbols('SNMPv2-MIB', 'sysDescr')
        sysDescr = self.MibScalarInstance(sysDescr.name,
                                          (0,),
                                          sysDescr.syntax.clone("PySNMP engine - OVS 1.2.0 SNMP Agent")) # Get from config?
        self.snmpEngine.msgAndPduDsp.mibInstrumController.mibBuilder.exportSymbols('SNMPv2-MIB', sysDescr)
        self._add_user_permission(self.naming_scheme.replace('.%s', ''))

    def start(self):
        """
        Start SNMP Server main loop
        """
        # Register SNMP Applications at the SNMP engine for particular SNMP context
        #read only
        cmdrsp.GetCommandResponder(self.snmpEngine, self.snmpContext)
        cmdrsp.NextCommandResponder(self.snmpEngine, self.snmpContext)
        cmdrsp.BulkCommandResponder(self.snmpEngine, self.snmpContext)

        self.snmpEngine.transportDispatcher.jobStarted(1)

        # Run I/O dispatcher which would receive queries and send responses
        while self.run:
            try:
                self.snmpEngine.transportDispatcher.runDispatcher()
            except KeyboardInterrupt:
                self.snmpEngine.transportDispatcher.closeDispatcher()
                raise
            except Exception as ex:
                print(ex)

    def stop(self):
        """
        Stop the underlying transport
        """
        print('Got stop request, will now exit')
        self.run = False
        self.snmpEngine.transportDispatcher._AbstractTransportDispatcher__jobs = {} # The only way to stop the loop clean
        try:
            self.snmpEngine.transportDispatcher.closeDispatcher()
        except Exception as ex:
            print('Failed to close SNMP TransportDispatcher: {}'.format(str(ex)))

    def _add_v3_md5_des_user(self, user):
        """
        Setup v3 user with md5, des
        """
        config.addV3User(self.snmpEngine, user[0],
                         config.usmHMACMD5AuthProtocol, user[1],
                         config.usmDESPrivProtocol, user[2])


    def _add_user_permission(self, OID):
        """
        Add user permission to OID - readOnly
        """
        OID = tuple(int(x) for x in OID.split('.'))
        if self.users:
            for user in self.users:
                print('add user permission %s %s ' % (str(user), str(OID)))
                config.addVacmUser(self.snmpEngine, 3, str(user[0]), str(user[3]), OID)
        else:
             #Allow full MIB access for this user / securityModels at VACM
             config.addVacmUser(self.snmpEngine, 1, 'my-read-area',
                   'noAuthNoPriv', OID)


    def register_custom_oid(self, class_oid, instance_oid, attribute_oid, get_function, atype = str):
        """
        Register a custom oid - agnostic
        """
        return_types = {str: v2c.OctetString,
                        int: v2c.Counter32}

        oid = self.naming_scheme % (class_oid, instance_oid, attribute_oid)
        return_type = return_types.get(atype, v2c.OctetString)
        OID = tuple(int(x) for x in oid.split('.'))

        print('Registering OID %s for %s return type %s ' % (str(OID), get_function, return_type))
        def _class():
            class CustomScalar(self.MibScalarInstance):
                def getValue(class_, name, idx): #@NoSelf
                    _, _ = name, idx
                    try:
                        value = get_function()
                    except Exception as ex:
                        value = str(ex)
                    print('MibScalar getValue %s %s, return %s' % (name, idx, value))
                    return class_.getSyntax().clone(value)

            return CustomScalar
        #export mib
        self.mibBuilder.exportSymbols(oid, self.MibScalar(OID[:-1], return_type()),
                                      _class()(OID[:-1], (OID[-1],), return_type()))
        return oid

    def unregister_custom_oid(self, oid):
        """
        Unexport an OID symbol from snmp
        """
        try:
            print('Unexporting symbol %s ' % (oid))
            self.mibBuilder.unexportSymbols(oid)
        except SmiError as smie:
            print('[EXCEPTION] Failed to unexport symbol: %s' % str(smie))

    def register_polling_function(self, function, interval_sec):
        """
        Register a custom polling function
        e.g. Periodically check for model changes
        """
        self.snmpEngine.transportDispatcher.registerTimerCbFun(function, interval_sec)

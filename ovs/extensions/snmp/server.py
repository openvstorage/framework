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

DAL_NAMING_SCHEME = "1.3.6.1.4.1.0.%s.%s.%s"

class SNMPServer():
    """
    SNMP Server - command responder
    responds to snmp queries
    standard oids implemented by pysnmp
    custom oids implemented either in custom class or by browsing dal/model
    """
    def __init__(self, host, port, users, assigned_oids):
        """
        host = public ip to listen on
        port = port to listen on (usually 161)
        users = list of ('username', 'password', 'privatekey', 'authPriv') #authentication method for snmp v3
        if users is None, authentication will be snmp v1 public community string, read only
        """
        self.instance_oid = 0
        self.attrb_oid = 0
        self.ASSIGNED = assigned_oids

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


    def register_custom_oid_for_model(self, class_oid, model_object, attributes):
        """
        Register a model object's attributes as oids
        """
        if not class_oid in self.ASSIGNED:
            self.ASSIGNED[class_oid] = {}
            self.instance_oid = 0

        if not self.instance_oid in self.ASSIGNED[class_oid]:
            self.ASSIGNED[class_oid][self.instance_oid] = {}

        while True:
            existing = self.ASSIGNED[class_oid][self.instance_oid].get(self.attrb_oid, None)
            if existing:
                if existing == (model_object, attribute):
                    #  Already modeled correctly
                    return
                else:
                    #  Something is present here but not the expected model_object
                    self.instance_oid += 1
                    self.attrb_oid = 0
            else:
                # Nothing exists, so we add here
                for attribute in attributes:
                    oid = DAL_NAMING_SCHEME % (class_oid, self.instance_oid, self.attrb_oid)
                    self.ASSIGNED[class_oid][self.instance_oid][self.attrb_oid] = (model_object, attribute)
                    self.attrb_oid += 1

                    self._add_user_permission(DAL_NAMING_SCHEME.replace('.%s', ''))
                    return_type = v2c.OctetString
                    OID = tuple(int(x) for x in oid.split('.'))
                    print('Registering OID %s for %s %s' % (str(OID), type(model_object), attribute))
                    def _class():
                        class DALScalar(self.MibScalarInstance):
                            def getValue(class_, name, idx): #@NoSelf
                                try:
                                    c_oid = name[-3]
                                    i_oid = name[-2]
                                    a_oid = name[-1]
                                    mo, attr = self.ASSIGNED[c_oid][i_oid][a_oid]
                                    if callable(attr): #lambda:
                                        value = attr(mo)
                                    else:
                                        value = getattr(mo, attr)
                                except KeyError:
                                    value = "KEY NOT ASSIGNED"
                                except Exception as ex:
                                    value = str(ex)
                                print('MibScalar getValue %s %s, return %s' % (name, idx, value))
                                return class_.getSyntax().clone(value)

                        return DALScalar
                    #export mib
                    self.mibBuilder.exportSymbols(oid, self.MibScalar(OID[:-1], return_type()),
                                                  _class()(OID[:-1], (OID[-1],), return_type()))

                self.attrb_oid = 0
                self.instance_oid += 1
                return

    def register_custom_oid(self, oid):
        """
        Register a custom class as OID, must implemented static method get
        """
        self._add_user_permission(oid.OID)
        return_type = getattr(v2c, oid.RETURN)
        OID = tuple(int(x) for x in oid.OID.split('.'))
        print('Registering OID %s' % str(OID))
        if oid.TYPE == 'MibScalar':
            print('creating class %s' % oid.__name__)
            def _class():
                class MyStaticMibScalarInstance(self.MibScalarInstance):
                    def getValue(self, name, idx):
                        _, _ = name, idx
                        value = oid.get()
                        print('MibScalar getValue %s %s, return %s' % (name, idx, value))
                        return self.getSyntax().clone(value)
                return MyStaticMibScalarInstance
            #export mib
            self.mibBuilder.exportSymbols(oid.NAME, self.MibScalar(OID[:-1], return_type()),
                                          _class()(OID[:-1], (OID[-1],), return_type()))

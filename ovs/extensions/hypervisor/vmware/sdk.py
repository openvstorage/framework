from suds.client import Client, ObjectCache
from suds.sudsobject import Property
from suds.plugin import MessagePlugin

from time import sleep

import datetime
import re


class SdkConnection(object):

    def connect(self, host, login, passwd):
        return Sdk(host, login, passwd)


def validate_session(function):
    def new_function(self, *args, **kwargs):
        self._validateSession()
        return function(self, *args, **kwargs)
    return new_function


class ValueExtender(MessagePlugin):
    def addAttributeForValue(self, node):
        if node.name == 'value':
            node.set('xsi:type', 'xsd:string')

    def marshalled(self, context):
        context.envelope.walk(self.addAttributeForValue)


class Sdk(object):
    def __init__(self, host, login, passwd):
        self._host      = host
        self._username  = login
        self._password  = passwd
        self._sessionID = None

        self._cache = ObjectCache()
        self._cache.setduration(weeks=1)

        self._client = Client('https://%s/sdk/vimService?wsdl' % host,
                              cache         = self._cache,
                              cachingpolicy = 1)
        self._client.set_options(location = 'https://%s/sdk' % host,
                                 plugins  = [ValueExtender()])

        serviceReference = self._buildProperty('ServiceInstance')
        self._serviceContent = self._client.service.RetrieveServiceContent(serviceReference)

        self._isVCenter = self._serviceContent.about.apiType == "VirtualCenter"  # In case of an ESXi host, this would be "HostAgent"
        if not self._isVCenter:
            self._validateSession()
            self._esxHost = self._getObject(self._serviceContent.rootFolder,
                                            propType  = 'HostSystem',
                                            traversal = {'name'     : 'FolderTraversalSpec',
                                                         'type'     : 'Folder',
                                                         'path'     : 'childEntity',
                                                         'traversal': {'name'      : 'DatacenterTraversalSpec',
                                                                       'type'      : 'Datacenter',
                                                                       'path'      : 'hostFolder',
                                                                       'traversal' : {'name'      : 'DFolderTraversalSpec',
                                                                                      'type'      : 'Folder',
                                                                                      'path'      : 'childEntity',
                                                                                      'traversal' : {'name': 'ComputeResourceTravelSpec',
                                                                                                     'type': 'ComputeResource',
                                                                                                     'path': 'host'}}}},
                                            properties=['name']).obj_identifier
        else:
            self._esxHost = None

    def validateResult(self, result, message=None):
        if hasattr(result, '_type') and result._type == "Task":
            return self.validateResult(self.getTaskInfo(result), message)
        elif hasattr(result, 'info'):
            if result.info.state == 'success':
                return True
            else:
                error = result.info.error.localizedMessage
                raise Exception(("%s: %s" % (message, error)) if message else error)
        raise Exception(("%s: %s" % (message, "Unexpected result")) if message else "Unexpected result")

    @validate_session
    def getTaskInfo(self, task):
        return self._getObject(task)

    @validate_session
    def getVMIPInformation(self, esxHost=None):
        esxHost = self._validateHost(esxHost)
        configuration = []
        for vm in self._getObject(esxHost,
                                  propType   = 'VirtualMachine',
                                  traversal  = {'name': 'HostSystemTraversalSpec',
                                               'type': 'HostSystem',
                                               'path': 'vm'},
                                  properties = ['name', 'guest.net', 'config.files']):
            vmi = {'id'     : str(vm.obj_identifier.value),
                   'vmxpath': str(vm.config.files.vmPathName),
                   'name'   : str(vm.name),
                   'net'    : []}
            if vm.guest.net:
                for net in vm.guest.net[0]:
                    vmi['net'].append({'mac'        : str(net.macAddress),
                                       'ipaddresses': [str(i.ipAddress) for i in net.ipConfig.ipAddress]})
            configuration.append(vmi)
        return configuration

    @validate_session
    def exists(self, esxHost=None, name=None, key=None):
        esxHost = self._validateHost(esxHost)
        if name is not None or key is not None:
            try:
                if name is not None:
                    vms = [vm for vm in self._getObject(esxHost,
                                                        propType   = 'VirtualMachine',
                                                        traversal  = {'name': 'HostSystemTraversalSpec',
                                                                      'type': 'HostSystem',
                                                                      'path': 'vm'},
                                                        properties = ['name']) if vm.name == name]
                    if len(vms) == 0:
                        return None
                    else:
                        return vms[0].obj_identifier
                if key is not None:
                    return self._getObject(self._buildProperty('VirtualMachine', key), properties=['name']).obj_identifier
            except:
                return None
        else:
            raise Exception("A name or key should be passed.")

    @validate_session
    def addPhysicalDisk(self, vmname, deviceName, diskLabel, filename, esxHost=None, wait=False):
        def findUnitGap(unitList):
            freeUnitNumber = 0
            while freeUnitNumber in unitList:
                freeUnitNumber += 1
                if freeUnitNumber == 7:  # We're not allowed to use slot 7
                    freeUnitNumber = 8
                if freeUnitNumber > 15:  # There are only 15 slots
                    return None
            return freeUnitNumber

        virtualLsiLogicSASControllerType = type(self._client.factory.create('ns0:VirtualLsiLogicSASController'))
        virtualDiskType                  = type(self._client.factory.create('ns0:VirtualDisk'))

        esxHost = self._validateHost(esxHost)
        vms = self._getObject(esxHost,
                              propType   = 'VirtualMachine',
                              traversal  = {'name': 'HostSystemTraversalSpec',
                                            'type': 'HostSystem',
                                            'path': 'vm'},
                              properties = ['name'])
        if len(vms) > 0:
            for vm in vms:
                if vm.name == vmname:
                    # Finding out the LSILogicSAS controller
                    devices = self._getObject(vm.obj_identifier, properties=['config.hardware']).config.hardware.device
                    controllers = []
                    controllerMapping = {}
                    for device in devices:
                        if type(device) == virtualLsiLogicSASControllerType:
                            controllers.append(device.key)
                        elif type(device) == virtualDiskType:
                            if not device.controllerKey in controllerMapping:
                                controllerMapping[device.controllerKey] = []
                            controllerMapping[device.controllerKey].append(device.unitNumber)
                    if len(controllers) == 0:
                        raise Exception("Could not find LsiLogicSASController for %s" % vm.obj_identifier.value)

                    freeUnitNumber = None
                    controllerKey = None
                    for controller in controllers:
                        freeUnitNumber = findUnitGap(controllerMapping[controller])
                        if freeUnitNumber is not None:
                            controllerKey = controller
                            break
                    if freeUnitNumber is None or controllerKey is None:
                        raise Exception(
                            "Could not find and empty LsiLogicSASController for %s" % vm.obj_identifier.value)

                    config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
                    config.deviceChange = []

                    deviceInfo = self._client.factory.create('ns0:Description')
                    deviceInfo.label   = diskLabel
                    deviceInfo.summary = diskLabel
                    backing = self._client.factory.create('ns0:VirtualDiskRawDiskMappingVer1BackingInfo')
                    backing.deviceName        = deviceName
                    backing.compatibilityMode = 'physicalMode'
                    backing.diskMode          = 'independent_persistent'
                    backing.fileName          = filename
                    device = self._client.factory.create('ns0:VirtualDisk')
                    device.controllerKey = controllerKey
                    device.key           = -100
                    device.unitNumber    = freeUnitNumber
                    device.deviceInfo    = deviceInfo
                    device.backing = backing
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation     = 'add'
                    diskSpec.fileOperation = 'create'
                    diskSpec.device        = device

                    config.deviceChange.append(diskSpec)

                    task = self._client.service.ReconfigVM_Task(vm.obj_identifier, config)

                    if wait:
                        self.waitForTask(task)
                    return task

        raise Exception("Could not find a virtual machine with name %s" % vmname)

    @validate_session
    def setDiskMode(self, vmid, disks, mode, esxHost=None, wait=True):
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.deviceChange = []

        esxHost = self._validateHost(esxHost)
        vmid = self.exists(key=vmid)
        # Set disk mode for disks
        esxHost    = self._validateHost(esxHost)
        vm = self._getObject(vmid)
        for device in vm.config.hardware.devices:
            if hasattr(device, 'backing') and device.__class__.__name__ == 'VirtualDisk' and device.backing.fileName in disks:
                backing = factory.create('ns0:VirtualDiskFlatVer2BackingInfo')
                backing.diskMode = mode
                device = factory.create('ns0:VirtualDisk')
                device.backing       = backing
                diskSpec = factory.create('ns0:VirtualDeviceConfigSpec')
                diskSpec.operation     = 'edit'
                diskSpec.fileOperation = None
                diskSpec.device        = device
                config.deviceChange.append(diskSpec)

        task = self._client.service.ReconfigVM_Task(vm.obj_identifier, config)

        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def removeDisk(self, vm, disk, esxHost=None, wait=True):
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.deviceChange = []

        # Map disk to uuid
        esxHost    = self._validateHost(esxHost)
        iqnMapping = self._getHostIQNMapping(esxHost)
        diskLun    = None
        if disk.iqn in iqnMapping:
            diskLun = iqnMapping[disk.iqn]['uuid']

        if diskLun is None:
            raise Exception("Disk not found for iqn %s." % disk.iqn)

        # Renove the disk
        virtualDiskType = type(self._client.factory.create('ns0:VirtualDisk'))
        devices = self._getObject(vm, properties=['config.hardware.device']).config.hardware.device[0]
        for device in devices:
            if type(device) == virtualDiskType:
                if device.backing.lunUuid == diskLun:
                    # Found a matching device
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation     = 'remove'
                    diskSpec.fileOperation = 'destroy'
                    diskSpec.device        = device
                    config.deviceChange.append(diskSpec)

        task = self._client.service.ReconfigVM_Task(vm, config)

        if wait:
            self.waitForTask(task)
        return task

    def _createDisk(self, factory, key, disk, unit, datastore):
        """
        disk = {'name': diskname, 'backingdevice': 'disk-flat.vmdk'}
        """
        deviceInfo = factory.create('ns0:Description')
        deviceInfo.label   = disk['name']
        deviceInfo.summary = 'Disk %s' % disk['name']
        backing = factory.create('ns0:VirtualDiskFlatVer2BackingInfo')
        backing.diskMode = 'persistent'
        #backing.datastore = datastore
        backing.fileName = '[%(datastore)s] %(devicepath)s' % {'datastore': datastore.name, 'devicepath': disk['backingdevice']}
        #backing.fileName = disk['backingdevice']
        device = factory.create('ns0:VirtualDisk')
        device.controllerKey = key
        device.key           = -200 - unit
        device.unitNumber    = unit
        device.deviceInfo    = deviceInfo
        device.backing       = backing
        diskSpec = factory.create('ns0:VirtualDeviceConfigSpec')
        diskSpec.operation     = 'add'
        diskSpec.fileOperation = None
        diskSpec.device        = device
        return diskSpec

    def _createFileInfo(self, factory, datastore):
        fileInfo = factory.create('ns0:VirtualMachineFileInfo')
        fileInfo.vmPathName = '[%s]' % datastore
        return fileInfo

    def _createNic(self, factory, deviceType, deviceLabel, deviceSummary, network, unit):
        deviceInfo = factory.create('ns0:Description')
        deviceInfo.label   = deviceLabel
        deviceInfo.summary = deviceSummary
        backing = factory.create('ns0:VirtualEthernetCardNetworkBackingInfo')
        backing.deviceName = network
        device = factory.create('ns0:%s'%deviceType)
        device.addressType      = 'Generated'
        device.wakeOnLanEnabled = True
        device.controllerKey    = 100  # PCI Controller
        device.key              = -300 - unit
        device.unitNumber       = unit
        device.backing          = backing
        device.deviceInfo       = deviceInfo
        nicSpec = factory.create('ns0:VirtualDeviceConfigSpec')
        nicSpec.operation     = 'add'
        nicSpec.fileOperation = None
        nicSpec.device        = device
        return nicSpec

    def _createDiskController(self, factory, key):
        deviceInfo = self._client.factory.create('ns0:Description')
        deviceInfo.label   = 'SCSI controller 0'
        deviceInfo.summary = 'LSI Logic SAS'
        controller = factory.create('ns0:VirtualLsiLogicSASController')
        controller.busNumber  = 0
        controller.key        = key
        controller.sharedBus  = 'noSharing'
        controller.deviceInfo = deviceInfo
        controllerSpec = factory.create('ns0:VirtualDeviceConfigSpec')
        controllerSpec.operation     = 'add'
        controllerSpec.fileOperation = None
        controllerSpec.device        = controller
        return controllerSpec

    def _createOptionValue(self, factory, key, value):
        option = factory.create('ns0:OptionValue')
        option.key   = key
        option.value = value
        return option

    @validate_session
    def copyFile(self, source, destination, wait=True):
        task = self._client.service.CopyDatastoreFile_Task(_this = self._serviceContent.fileManager, sourceName = source, destinationName = destination)

        if wait:
            self.waitForTask(task)
        return task
        

    @validate_session
    def updateVM(self, vm, name, os, disks, kvmport, esxHost=None, wait=True):
        # The info we get passed in will overwrite whatever is currently on the machine
        self._createDisk(factory, key,disk, unit)

        def createOptionValue(factory, key, value):
            option = factory.create('ns0:OptionValue')
            option.key   = key
            option.value = value
            return option

        virtualLsiLogicSASControllerType = type(self._client.factory.create('ns0:VirtualLsiLogicSASController'))
        virtualDiskType                  = type(self._client.factory.create('ns0:VirtualDisk'))

        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.name         = name
        config.guestId      = os
        config.deviceChange = []
        config.extraConfig  = []

        # Add disk devices
        esxHost    = self._validateHost(esxHost)
        iqnMapping = self._getHostIQNMapping(esxHost, rescan=True)
        diskMap = {}
        for disk in disks:
            if disk['iqn'] in iqnMapping:
                disk['index'] = disks.index(disk)
                disk['eui']   = iqnMapping[disk['iqn']]['eui']
                disk['lun']   = iqnMapping[disk['iqn']]['lun']
                diskMap[iqnMapping[disk['iqn']]['uuid']] = disk

        # Cleaning/reconfiguring disks
        preferredController = None
        controllers = []
        self._client.service.Reload(vm)
        devices = self._getObject(vm, properties=['config.hardware.device']).config.hardware.device[0]
        for device in devices:
            if type(device) == virtualDiskType:
                if device.backing.lunUuid not in diskMap:
                    # We found a disk that's not in our disk list, so we should remove this disk.
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation     = 'remove'
                    diskSpec.fileOperation = 'destroy'
                    diskSpec.device        = device
                    config.deviceChange.append(diskSpec)
                else:
                    # The disk still needs to be attached to the VM. We'll reconfigure it anyway with its new location etc
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation     = 'edit'
                    diskSpec.fileOperation = None
                    diskSpec.device        = device
                    if diskSpec.device.unitNumber != diskMap[device.backing.lunUuid]['index']:
                        diskSpec.device.unitNumber = diskMap[device.backing.lunUuid]['index']
                        config.deviceChange.append(diskSpec)
                    del diskMap[device.backing.lunUuid]
                    preferredController = device.controllerKey
            elif type(device) == virtualLsiLogicSASControllerType:
                controllers.append(device.key)
        disksToAdd = diskMap.values()
        if disksToAdd:
            if preferredController is None:
                preferredController = controllers[0]
            for disk in disksToAdd:
                # The remaining disks were not found, so we should add them
                config.deviceChange.append(self._createDisk(self._client.factory, preferredController, disk, disk['index']))

        # Change additional properties
        extraConfigs = [
            ('RemoteDisplay.vnc.enabled', 'true'),
            ('RemoteDisplay.vnc.port', str(kvmport))
        ]
        for item in extraConfigs:
            config.extraConfig.append(self._createOptionValue(self._client.factory, item[0], item[1]))

        task = self._client.service.ReconfigVM_Task(vm, config)

        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def createVM(self, name, cpus, memory, os, disks, nics, kvmport, datastore, esxHost=None, wait=False):
        esxHost = self._validateHost(esxHost)
        hostData = self._getHostData(esxHost)

        # Build basic config information
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.name         = name
        config.numCPUs      = cpus
        config.memoryMB     = memory
        config.guestId      = os
        config.deviceChange = []
        config.extraConfig  = []
        config.files        = self._createFileInfo(self._client.factory, datastore)

        disk_controller_key = -101
        config.deviceChange.append(self._createDiskController(self._client.factory, disk_controller_key))

        # Add disk devices
        iqnMapping = self._getHostIQNMapping(esxHost, rescan=True)
        for disk in disks:
            if disk['iqn'] in iqnMapping:
                disk['eui'] = iqnMapping[disk['iqn']]['eui']
                disk['lun'] = iqnMapping[disk['iqn']]['lun']
                config.deviceChange.append(self._createDisk(self._client.factory, disk_controller_key, disk, disks.index(disk)))

        # Add network
        #networks = [self._getObject(network) for network in hostData['network']]
        for nic in nics:
            #network = [network.summary.network for network in networks if network.name == nic['bridge']][0]
            unit = nics.index(nic)
            config.deviceChange.append(self._createNic(self._client.factory, 'VirtualE1000', 'Interface %s'%unit, '%s interface'%nic['bridge'], nic['bridge'], unit))

        # Change additional properties
        extraConfigs = [
            ('RemoteDisplay.vnc.enabled',  'true'),
            ('RemoteDisplay.vnc.port',     str(kvmport)),
            ('RemoteDisplay.vnc.password', 'vmconnect'),
            ('pciBridge0.present',         'true'),
            ('pciBridge4.present',         'true'),
            ('pciBridge4.virtualDev',      'pcieRootPort'),
            ('pciBridge4.functions',       '8'),
            ('pciBridge5.present',         'true'),
            ('pciBridge5.virtualDev',      'pcieRootPort'),
            ('pciBridge5.functions',       '8'),
            ('pciBridge6.present',         'true'),
            ('pciBridge6.virtualDev',      'pcieRootPort'),
            ('pciBridge6.functions',       '8'),
            ('pciBridge7.present',         'true'),
            ('pciBridge7.virtualDev',      'pcieRootPort'),
            ('pciBridge7.functions',       '8')
        ]
        for item in extraConfigs:
            config.extraConfig.append(self._createOptionValue(self._client.factory, item[0], item[1]))

        task = self._client.service.CreateVM_Task(hostData['folder'],
                                                  config = config,
                                                  pool   = hostData['resourcePool'],
                                                  host   = hostData['host'])
        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def cloneVM(self, vmid, name, disks, esxHost=None, wait=True):
        """
        Clone a existing VM configuration
        
        @param vmid: unique id of the vm
        @param name: name of the clone vm
        @param disks: list of disks to use in vm configuration
        @param kvmport: kvm port for the clone vm
        @param esxHost: esx host identifier on which to clone the vm
        @param wait: wait for task to complete or not (True/False) 
        """
        
        esxHost = self._validateHost(esxHost)
        hostData = self._getHostData(esxHost)
        
        sourceVMObject = self.exists(key=vmid)
        if not sourceVMObject:
            raise Exception("VM with key reference %s not found" % vmid)
        sourceVM = self._getObject(sourceVMObject)
        datastore = self._getObject(sourceVM.datastore[0][0])

        # Build basic config information
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.name         = name
        config.numCPUs      = sourceVM.config.hardware.numCPU
        config.memoryMB     = sourceVM.config.hardware.memoryMB
        config.guestId      = sourceVM.config.guestId
        config.deviceChange = []
        config.extraConfig  = []
        config.files        = self._createFileInfo(self._client.factory, datastore.name)

        disk_controller_key = -101
        config.deviceChange.append(self._createDiskController(self._client.factory, disk_controller_key))

        # Add disk devices
        for disk in disks:
            config.deviceChange.append(self._createDisk(self._client.factory, disk_controller_key, disk, disks.index(disk), datastore))
            self.copyFile('[{0}] {1}'.format(datastore.name, '%s.vmdk'%disk['name'].split('_')[-1].replace('-clone','')), '[{0}] {1}'.format(datastore.name, disk['backingdevice']))

        # Add network
        for device in sourceVM.config.hardware.device:
            if hasattr(device, 'backing') and device.backing.__class__.__name__ == 'VirtualEthernetCardNetworkBackingInfo':
                config.deviceChange.append(self._createNic(self._client.factory, device.__class__.__name__, device.deviceInfo.label, device.deviceInfo.summary, device.backing.deviceName, device.unitNumber))

        # Copy additional properties
        extraConfigsToSkip = ['nvram']
        for item in sourceVM.config.extraConfig:
            if not item.key in extraConfigsToSkip:
                config.extraConfig.append(self._createOptionValue(self._client.factory, item.key, item.value))

        task = self._client.service.CreateVM_Task(hostData['folder'],
                                                  config = config,
                                                  pool   = hostData['resourcePool'],
                                                  host   = hostData['host'])
        if wait:
            self.waitForTask(task)
        return task


    @validate_session
    def registerVM(self, vmxpath, esxHost=None, wait=False):
        esxHost = self._validateHost(esxHost)
        hostData = self._getHostData(esxHost)
        task = self._client.service.RegisterVM_Task(hostData['folder'],
                                                    path       = vmxpath,
                                                    asTemplate = False,
                                                    pool       = hostData['resourcePool'],
                                                    host       = esxHost)
        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def getVMGuestInfo(self, vmID):
        info = self._getObject(self._buildProperty('VirtualMachine', vmID),
                               properties = ['guest', 'guestHeartbeatStatus'])
        setattr(info.guest, 'guestHeartbeatStatus', info.guestHeartbeatStatus)
        return info.guest

    @validate_session
    def deleteVM(self, vmID, wait=False):
        machine = self._buildProperty('VirtualMachine', vmID)
        task = self._client.service.Destroy_Task(machine)

        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def unregisterVM(self, vmID):
        machine = self._buildProperty('VirtualMachine', vmID)
        self._client.service.UnregisterVM(machine)

    @validate_session
    def powerOn(self, vmID, wait=False):
        machine = self._buildProperty('VirtualMachine', vmID)
        task = self._client.service.PowerOnVM_Task(machine)

        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def powerOff(self, vmID, wait=False):
        machine = self._buildProperty('VirtualMachine', vmID)
        task = self._client.service.PowerOffVM_Task(machine)

        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def shutdown(self, vmID):
        machine = self._buildProperty('VirtualMachine', vmID)
        self._client.service.ShutdownGuest(machine)

    @validate_session
    def suspend(self, vmID, wait=False):
        machine = self._buildProperty('VirtualMachine', vmID)
        task = self._client.service.SuspendVM_Task(machine)

        if wait:
            self.waitForTask(task)
        return task

    @validate_session
    def getPowerState(self, vmID):
        return self._getObject(self._buildProperty('VirtualMachine', vmID), properties=['runtime.powerState']).runtime.powerState

    @validate_session
    def registerExtension(self, description, xmlurl, company, companyEmail, key, version):
        extension = self.findExtension(key)
        if extension:
            extension.description.label = description
            extension.description.summary = description
            if len(extension.server) == 1 and len(extension.client) == 1:
                extension.server[0].url = xmlurl
                extension.server[0].company = company
                extension.server[0].adminEmail = companyEmail
                extension.server[0].description.label = description
                extension.server[0].description.summary = description

                extension.client[0].version = version
                extension.client[0].company = company
                extension.client[0].description.label = description
                extension.client[0].description.summary = description
            else:
                raise Exception("Register extension expects only 1 server and 1 client, currently updating extension with key %s" % key)
            extension.version = version

            return self._client.service.UpdateExtension(self._serviceContent.extensionManager, extension)
        else:
            sdkdescription = self._client.factory.create('ns0:Description')
            sdkdescription.label = description
            sdkdescription.summary = description

            serverInfo = self._client.factory.create('ns0:ExtensionServerInfo')
            serverInfo.url = xmlurl
            serverInfo.description = sdkdescription
            serverInfo.company = company
            serverInfo.type = 'com.vmware.vim.viClientScripts'
            serverInfo.adminEmail = companyEmail

            clientInfo = self._client.factory.create('ns0:ExtensionClientInfo')
            clientInfo.version = version
            clientInfo.description = sdkdescription
            clientInfo.company = company
            clientInfo.type = 'com.vmware.vim.viClientScripts'
            clientInfo.url = xmlurl

            extension = self._client.factory.create('ns0:Extension')
            extension.description = sdkdescription
            extension.key = key
            extension.version = version
            extension.subjectName = 'blank'
            extension.server = [serverInfo]
            extension.client = [clientInfo]
            extension.lastHeartbeatTime = datetime.datetime.now()

            return self._client.service.RegisterExtension(self._serviceContent.extensionManager, extension)

    @validate_session
    def findExtension(self, key):
        return self._client.service.FindExtension(self._serviceContent.extensionManager, key)

    @validate_session
    def waitForTask(self, task):
        state = self.getTaskInfo(task).info.state
        while state in ['running', 'queued']:
            sleep(1)
            state = self.getTaskInfo(task).info.state

    def _getHostData(self, esxHost):
        hostObject      = self._getObject(esxHost,                    properties=['parent', 'datastore', 'network'])
        datastore       = self._getObject(hostObject.datastore[0][0], properties=['info']).info
        computeResource = self._getObject(hostObject.parent,          properties=['resourcePool', 'parent'])
        datacenter      = self._getObject(computeResource.parent,     properties=['parent']).parent
        vmFolder        = self._getObject(datacenter,                 properties=['vmFolder']).vmFolder

        return {'host'           : esxHost,
                'computeResource': computeResource,
                'resourcePool'   : computeResource.resourcePool,
                'datacenter'     : datacenter,
                'folder'         : vmFolder,
                'datastore'      : datastore,
                'network'        : hostObject.network[0]}

    def _getHostIQNMapping(self, esxHost, rescan=False):
        regex   = re.compile('^key-vim.host.PlugStoreTopology.Path-iqn.+?,(?P<iqn>iqn.*?),t,1-(?P<eui>eui.+)$')

        hostObject    = self._getObject(esxHost, properties=['configManager.storageSystem'])
        storageSystem = self._getObject(hostObject.configManager.storageSystem, properties=['storageDeviceInfo',
                                                                                            'storageDeviceInfo.plugStoreTopology.device'])
        if rescan:
            self._client.service.RescanVmfs(storageSystem.obj_identifier)  # Force a rescan of the vmfs
            storageSystem = self._getObject(hostObject.configManager.storageSystem, properties=['storageDeviceInfo',
                                                                                                'storageDeviceInfo.plugStoreTopology.device'])

        deviceInfoMapping = {}
        for disk in storageSystem.storageDeviceInfo.scsiLun:
            deviceInfoMapping[disk.key] = disk.uuid

        iqnMapping = {}
        for device in storageSystem.storageDeviceInfo.plugStoreTopology.device.HostPlugStoreTopologyDevice:
            for path in device.path:
                match = regex.search(path)
                if match:
                    groups = match.groupdict()
                    iqnMapping[groups['iqn']] = {'eui' : groups['eui'],
                                                 'lun' : device.lun,
                                                 'uuid': deviceInfoMapping[device.lun]}

        return iqnMapping

    def _getObject(self, keyObject, propType=None, traversal=None, properties=None):
        objectSpec = self._client.factory.create('ns0:ObjectSpec')
        objectSpec.obj = keyObject

        propertySpec = self._client.factory.create('ns0:PropertySpec')
        propertySpec.type = keyObject._type if propType is None else propType
        if properties is None:
            propertySpec.all = True
        else:
            propertySpec.all = False
            propertySpec.pathSet = properties

        if traversal is not None:
            selectSetPtr = objectSpec
            while True:
                selectSetPtr.selectSet = self._client.factory.create('ns0:TraversalSpec')
                selectSetPtr.selectSet.name = traversal['name']
                selectSetPtr.selectSet.type = traversal['type']
                selectSetPtr.selectSet.path = traversal['path']
                if 'traversal' in traversal:
                    traversal = traversal['traversal']
                    selectSetPtr = selectSetPtr.selectSet
                else:
                    break

        propertyFilterSpec = self._client.factory.create('ns0:PropertyFilterSpec')
        propertyFilterSpec.objectSet = [objectSpec]
        propertyFilterSpec.propSet = [propertySpec]

        foundObjects = self._client.service.RetrieveProperties(self._serviceContent.propertyCollector,
                                                               [propertyFilterSpec])

        if len(foundObjects) > 0:
            for item in foundObjects:
                item.obj_identifier = item.obj
                del item.obj
                for propSet in item.propSet:
                    if '.' in propSet.name:
                        workingItem = item
                        path = str(propSet.name).split('.')
                        partCounter = 0
                        for part in path:
                            partCounter += 1
                            if partCounter < len(path):
                                if not part in workingItem.__dict__:
                                    setattr(workingItem, part, self._createClass(part)())
                                workingItem = workingItem.__dict__[part]
                            else:
                                setattr(workingItem, part, propSet.val)
                    else:
                        setattr(item, propSet.name, propSet.val)
                del item.propSet
            if len(foundObjects) == 1:
                return foundObjects[0]
            else:
                return foundObjects

        return None

    def _buildProperty(self, propertyName, value=None):
        newProperty = Property(propertyName)
        newProperty._type = propertyName
        if value is not None:
            newProperty.value = value
        return newProperty

    def _validateHost(self, host):
        if host is None:
            if self._isVCenter:
                raise Exception("A HostSystem reference is mandatory in case the SDK is executed against a vCenter Server")
            else:
                return self._esxHost
        else:
            if hasattr(host, '_type') and host._type == "HostSystem":
                return self._getObject(host, properties=['name']).obj_identifier
            else:
                return self._getObject(self._buildProperty('HostSystem', host), properties=['name']).obj_identifier

    def _validateSession(self):
        if self._sessionID is None:
            self._logout()
            self._sessionID = self._client.service.Login(self._serviceContent.sessionManager, self._username, self._password, None).key
        else:
            active = False
            try:
                active = self._client.service.SessionIsActive(self._serviceContent.sessionManager,
                                                              sessionID = self._sessionID,
                                                              userName  = self._username)
            except:
                pass
            if not active:
                self._logout()
                self._sessionID = self._client.service.Login(self._serviceContent.sessionManager, self._username, self._password, None).key

    def _createClass(self, name):
        class Dummy():
            pass
        Dummy.__name__ = name
        return Dummy

    def _logout(self):
        try:
            self._client.service.Logout(self._serviceContent.sessionManager)
        except:
            pass
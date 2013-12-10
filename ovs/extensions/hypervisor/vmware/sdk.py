# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains all code for using the VMware SOAP API/SDK
"""

from time import sleep
import datetime
import re

from suds.client import Client
from suds.cache import ObjectCache
from suds.sudsobject import Property
from suds.plugin import MessagePlugin


def validate_session(function):
    """
    Decorator to make sure the SDK session is validated on each request
    """

    def new_function(self, *args, **kwargs):
        """
        Executes the functions
        """
        self._validate_session()
        return function(self, *args, **kwargs)

    return new_function


class ValueExtender(MessagePlugin):
    """
    Plugin for SUDS for compatibility with VMware SDK
    """

    def addAttributeForValue(self, node):
        """
        Adds an attribute to a given node
        """
        if node.name == 'value':
            node.set('xsi:type', 'xsd:string')

    def marshalled(self, context):
        """
        Hook up the plugin
        """
        context.envelope.walk(self.addAttributeForValue)


class Sdk(object):
    """
    This class contains all SDK related methods
    """

    def __init__(self, host, login, passwd):
        """
        Initializes the SDK
        """
        self._host = host
        self._username = login
        self._password = passwd
        self._sessionID = None

        self._cache = ObjectCache()
        self._cache.setduration(weeks=1)

        self._client = Client('https://%s/sdk/vimService?wsdl' % host,
                              cache=self._cache,
                              cachingpolicy=1)
        self._client.set_options(location='https://%s/sdk' % host,
                                 plugins=[ValueExtender()])

        service_reference = self._build_property('ServiceInstance')
        self._serviceContent = self._client.service.RetrieveServiceContent(service_reference)

        # In case of an ESXi host, this would be 'HostAgent'
        self._isVCenter = self._serviceContent.about.apiType == 'VirtualCenter'
        if not self._isVCenter:
            self._validate_session()
            # pylint: disable=line-too-long
            self._esxHost = self._get_object(self._serviceContent.rootFolder,
                                            prop_type='HostSystem',
                                            traversal={'name': 'FolderTraversalSpec',
                                                       'type': 'Folder',
                                                       'path': 'childEntity',
                                                       'traversal': {'name': 'DatacenterTraversalSpec',  # noqa
                                                                     'type': 'Datacenter',
                                                                     'path': 'hostFolder',
                                                                     'traversal': {'name': 'DFolderTraversalSpec',  # noqa
                                                                                   'type': 'Folder',
                                                                                   'path': 'childEntity',  # noqa
                                                                                   'traversal': {
                                                                                   'name': 'ComputeResourceTravelSpec',  # noqa
                                                                                   'type': 'ComputeResource',  # noqa
                                                                                   'path': 'host'}}}},  # noqa
                                            properties=['name']).obj_identifier
            # pylint: enable=line-too-long
        else:
            self._esxHost = None

    def validate_result(self, result, message=None):
        """
        Validates a given result. Returning True if the task succeeded, raising an error if not
        """
        if hasattr(result, '_type') and result._type == 'Task':
            return self.validate_result(self.get_task_info(result), message)
        elif hasattr(result, 'info'):
            if result.info.state == 'success':
                return True
            else:
                error = result.info.error.localizedMessage
                raise Exception(('%s: %s' % (message, error)) if message else error)
        raise Exception(('%s: %s' % (message, 'Unexpected result'))
                        if message else 'Unexpected result')

    @validate_session
    def get_task_info(self, task):
        """
        Loads the task details
        """
        return self._get_object(task)

    @validate_session
    def get_vm_ip_information(self, esxhost=None):
        """
        Get the IP information for all vms on a given esxi host
        """
        esxhost = self._validate_host(esxhost)
        configuration = []
        for vm in self._get_object(esxhost,
                                  prop_type='VirtualMachine',
                                  traversal={'name': 'HostSystemTraversalSpec',
                                             'type': 'HostSystem',
                                             'path': 'vm'},
                                  properties=['name', 'guest.net', 'config.files']):
            vmi = {'id': str(vm.obj_identifier.value),
                   'vmxpath': str(vm.config.files.vmPathName),
                   'name': str(vm.name),
                   'net': []}
            if vm.guest.net:
                for net in vm.guest.net[0]:
                    vmi['net'].append({'mac': str(net.macAddress),
                                       'ipaddresses': [str(i.ipAddress)
                                                       for i in net.ipConfig.ipAddress]})
            configuration.append(vmi)
        return configuration

    @validate_session
    def exists(self, esxhost=None, name=None, key=None):
        """
        Checks whether a vm with a given name or key exists on a given esxi host
        """
        esxhost = self._validate_host(esxhost)
        if name is not None or key is not None:
            try:
                if name is not None:
                    vms = [vm for vm in
                           self._get_object(esxhost,
                                           prop_type='VirtualMachine',
                                           traversal={'name': 'HostSystemTraversalSpec',
                                                      'type': 'HostSystem',
                                                      'path': 'vm'},
                                           properties=['name']) if vm.name == name]
                    if len(vms) == 0:
                        return None
                    else:
                        return vms[0].obj_identifier
                if key is not None:
                    return self._get_object(self._build_property('VirtualMachine', key),
                                           properties=['name']).obj_identifier
            except:
                return None
        else:
            raise Exception('A name or key should be passed.')

    @validate_session
    def add_physical_disk(self, vmname, devicename, disklabel, filename, esxhost=None, wait=False):
        """
        Adds a physical disk to a vm on a given esxi host. It tries to place the disk in the
        first free slot
        """
        def find_unit_gap(unitlist):
            """
            Searches a list of unit numbers for the first available gap
            """
            unit_number = 0
            while unit_number in unitlist:
                unit_number += 1
                if unit_number == 7:  # We're not allowed to use slot 7
                    unit_number = 8
                if unit_number > 15:  # There are only 15 slots
                    return None
            return unit_number

        controller_type = type(self._client.factory.create('ns0:VirtualLsiLogicSASController'))
        disk_type = type(self._client.factory.create('ns0:VirtualDisk'))

        esxhost = self._validate_host(esxhost)
        vms = self._get_object(esxhost,
                              prop_type='VirtualMachine',
                              traversal={'name': 'HostSystemTraversalSpec',
                                         'type': 'HostSystem',
                                         'path': 'vm'},
                              properties=['name'])
        if len(vms) > 0:
            for vm in vms:
                if vm.name == vmname:
                    # Finding out the LSILogicSAS controller
                    devices = self._get_object(vm.obj_identifier, properties=['config.hardware'])\
                        .config.hardware.device
                    controllers = []
                    controller_mapping = {}
                    for device in devices:
                        if type(device) == controller_type:
                            controllers.append(device.key)
                        elif type(device) == disk_type:
                            if not device.controllerKey in controller_mapping:
                                controller_mapping[device.controllerKey] = []
                            controller_mapping[device.controllerKey].append(device.unitNumber)
                    if len(controllers) == 0:
                        raise Exception('Could not find LsiLogicSASController for %s'
                                        % vm.obj_identifier.value)

                    free_unit_number = None
                    controller_key = None
                    for controller in controllers:
                        free_unit_number = find_unit_gap(controller_mapping[controller])
                        if free_unit_number is not None:
                            controller_key = controller
                            break
                    if free_unit_number is None or controller_key is None:
                        raise Exception(
                            'Could not find and empty LsiLogicSASController for %s'
                            % vm.obj_identifier.value)

                    config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
                    config.deviceChange = []

                    # pylint: disable=line-too-long
                    deviceInfo = self._client.factory.create('ns0:Description')
                    deviceInfo.label = disklabel
                    deviceInfo.summary = disklabel
                    backing = self._client.factory.create('ns0:VirtualDiskRawDiskMappingVer1BackingInfo')  # noqa
                    backing.deviceName = devicename
                    backing.compatibilityMode = 'physicalMode'
                    backing.diskMode = 'independent_persistent'
                    backing.fileName = filename
                    device = self._client.factory.create('ns0:VirtualDisk')
                    device.controllerKey = controller_key
                    device.key = -100
                    device.unitNumber = free_unit_number
                    device.deviceInfo = deviceInfo
                    device.backing = backing
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation = 'add'
                    diskSpec.fileOperation = 'create'
                    diskSpec.device = device
                    # pylint: enable=line-too-long

                    config.deviceChange.append(diskSpec)

                    task = self._client.service.ReconfigVM_Task(vm.obj_identifier, config)

                    if wait:
                        self.wait_for_task(task)
                    return task

        raise Exception('Could not find a virtual machine with name %s' % vmname)

    @validate_session
    def set_disk_mode(self, vmid, disks, mode, wait=True):
        """
        Sets the disk mode for a set of disks
        """
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.deviceChange = []

        disk_type = type(self._client.factory.create('ns0:VirtualDisk'))

        vmid = self.exists(key=vmid)
        vm = self._get_object(vmid)
        for device in vm.config.hardware.devices:
            if type(device) == disk_type and hasattr(device, 'backing') \
                    and device.backing.fileName in disks:
                backing = self._client.factory.create('ns0:VirtualDiskFlatVer2BackingInfo')
                backing.diskMode = mode
                device = self._client.factory.create('ns0:VirtualDisk')
                device.backing = backing
                diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                diskSpec.operation = 'edit'
                diskSpec.fileOperation = None
                diskSpec.device = device
                config.deviceChange.append(diskSpec)

        task = self._client.service.ReconfigVM_Task(vm.obj_identifier, config)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def remove_disk(self, vm, disk, esxhost=None, wait=True):
        """
        Removes a disk from a given vm
        """
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.deviceChange = []

        # Map disk to uuid
        esxhost = self._validate_host(esxhost)
        iqn_mapping = self._get_host_iqn_mapping(esxhost)
        disk_lun = None
        if disk.iqn in iqn_mapping:
            disk_lun = iqn_mapping[disk.iqn]['uuid']

        if disk_lun is None:
            raise Exception('Disk not found for iqn %s.' % disk.iqn)

        # Renove the disk
        disk_type = type(self._client.factory.create('ns0:VirtualDisk'))
        devices = self._get_object(vm, properties=['config.hardware.device'])\
            .config.hardware.device[0]
        for device in devices:
            if type(device) == disk_type:
                if device.backing.lunUuid == disk_lun:
                    # Found a matching device
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation = 'remove'
                    diskSpec.fileOperation = 'destroy'
                    diskSpec.device = device
                    config.deviceChange.append(diskSpec)

        task = self._client.service.ReconfigVM_Task(vm, config)

        if wait:
            self.wait_for_task(task)
        return task

    def _create_disk(self, factory, key, disk, unit, datastore):
        """
        Creates a disk spec for a given backing device
        Example for paramter disk: {'name': diskname, 'backingdevice': 'disk-flat.vmdk'}
        """
        deviceInfo = factory.create('ns0:Description')
        deviceInfo.label = disk['name']
        deviceInfo.summary = 'Disk %s' % disk['name']
        backing = factory.create('ns0:VirtualDiskFlatVer2BackingInfo')
        backing.diskMode = 'persistent'
        backing.fileName = '[%s] %s' % (datastore.name, disk['backingdevice'])
        device = factory.create('ns0:VirtualDisk')
        device.controllerKey = key
        device.key = -200 - unit
        device.unitNumber = unit
        device.deviceInfo = deviceInfo
        device.backing = backing
        diskSpec = factory.create('ns0:VirtualDeviceConfigSpec')
        diskSpec.operation = 'add'
        diskSpec.fileOperation = None
        diskSpec.device = device
        return diskSpec

    def _create_file_info(self, factory, datastore):
        """
        Creates a file info object
        """
        fileInfo = factory.create('ns0:VirtualMachineFileInfo')
        fileInfo.vmPathName = '[%s]' % datastore
        return fileInfo

    def _create_nic(self, factory, device_type, device_label, device_summary, network, unit):
        """
        Creates a NIC spec
        """
        deviceInfo = factory.create('ns0:Description')
        deviceInfo.label = device_label
        deviceInfo.summary = device_summary
        backing = factory.create('ns0:VirtualEthernetCardNetworkBackingInfo')
        backing.deviceName = network
        device = factory.create('ns0:%s' % device_type)
        device.addressType = 'Generated'
        device.wakeOnLanEnabled = True
        device.controllerKey = 100  # PCI Controller
        device.key = -300 - unit
        device.unitNumber = unit
        device.backing = backing
        device.deviceInfo = deviceInfo
        nicSpec = factory.create('ns0:VirtualDeviceConfigSpec')
        nicSpec.operation = 'add'
        nicSpec.fileOperation = None
        nicSpec.device = device
        return nicSpec

    def _create_disk_controller(self, factory, key):
        """
        Create a disk controller
        """
        deviceInfo = self._client.factory.create('ns0:Description')
        deviceInfo.label = 'SCSI controller 0'
        deviceInfo.summary = 'LSI Logic SAS'
        controller = factory.create('ns0:VirtualLsiLogicSASController')
        controller.busNumber = 0
        controller.key = key
        controller.sharedBus = 'noSharing'
        controller.deviceInfo = deviceInfo
        controllerSpec = factory.create('ns0:VirtualDeviceConfigSpec')
        controllerSpec.operation = 'add'
        controllerSpec.fileOperation = None
        controllerSpec.device = controller
        return controllerSpec

    def _create_option_value(self, factory, key, value):
        """
        Create option values
        """
        option = factory.create('ns0:OptionValue')
        option.key = key
        option.value = value
        return option

    @validate_session
    def copy_file(self, source, destination, wait=True):
        """
        Copies a file on the datastore
        """
        task = self._client.service.CopyDatastoreFile_Task(_this=self._serviceContent.fileManager,
                                                           sourceName=source,
                                                           destinationName=destination)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def update_vm(self, vm, name, os, disks, kvmport, esxhost=None, wait=True):
        """
        Update a existing vm
        """
        # The info we get passed in will overwrite whatever is currently on the machine
        controller_type = type(self._client.factory.create('ns0:VirtualLsiLogicSASController'))
        disk_type = type(self._client.factory.create('ns0:VirtualDisk'))

        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.name = name
        config.guestId = os
        config.deviceChange = []
        config.extraConfig = []

        # Add disk devices
        esxhost = self._validate_host(esxhost)
        iqn_mapping = self._get_host_iqn_mapping(esxhost, rescan=True)
        disk_map = {}
        for disk in disks:
            if disk['iqn'] in iqn_mapping:
                disk['index'] = disks.index(disk)
                disk['eui'] = iqn_mapping[disk['iqn']]['eui']
                disk['lun'] = iqn_mapping[disk['iqn']]['lun']
                disk_map[iqn_mapping[disk['iqn']]['uuid']] = disk

        # Cleaning/reconfiguring disks
        preferred_controller = None
        controllers = []
        self._client.service.Reload(vm)
        devices = self._get_object(vm, properties=['config.hardware.device'])\
            .config.hardware.device[0]
        for device in devices:
            if type(device) == disk_type:
                if device.backing.lunUuid not in disk_map:
                    # We found a disk that's not in our disk list, so we should remove this disk.
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation = 'remove'
                    diskSpec.fileOperation = 'destroy'
                    diskSpec.device = device
                    config.deviceChange.append(diskSpec)
                else:
                    # The disk still needs to be attached to the VM.
                    # We'll reconfigure it anyway with its new location etc
                    diskSpec = self._client.factory.create('ns0:VirtualDeviceConfigSpec')
                    diskSpec.operation = 'edit'
                    diskSpec.fileOperation = None
                    diskSpec.device = device
                    if diskSpec.device.unitNumber != disk_map[device.backing.lunUuid]['index']:
                        diskSpec.device.unitNumber = disk_map[device.backing.lunUuid]['index']
                        config.deviceChange.append(diskSpec)
                    del disk_map[device.backing.lunUuid]
                    preferred_controller = device.controllerKey
            elif type(device) == controller_type:
                controllers.append(device.key)
        disks_to_add = disk_map.values()
        if disks_to_add:
            if preferred_controller is None:
                preferred_controller = controllers[0]
            for disk in disks_to_add:
                # The remaining disks were not found, so we should add them
                config.deviceChange.append(self._create_disk(self._client.factory,
                                                             preferred_controller,
                                                             disk,
                                                             disk['index']))

        # Change additional properties
        extra_configs = [
            ('RemoteDisplay.vnc.enabled', 'true'),
            ('RemoteDisplay.vnc.port', str(kvmport))
        ]
        for item in extra_configs:
            config.extraConfig.append(self._create_option_value(self._client.factory,
                                                                item[0],
                                                                item[1]))

        task = self._client.service.ReconfigVM_Task(vm, config)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def create_vm(self, name, cpus, memory, os, disks, nics,
                  kvmport, datastore, esxhost=None, wait=False):
        """
        Create a vm with a given set of settings
        """
        esxhost = self._validate_host(esxhost)
        hostdata = self._get_host_data(esxhost)

        # Build basic config information
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.name = name
        config.numCPUs = cpus
        config.memoryMB = memory
        config.guestId = os
        config.deviceChange = []
        config.extraConfig = []
        config.files = self._create_file_info(self._client.factory, datastore)

        disk_controller_key = -101
        config.deviceChange.append(self._create_disk_controller(self._client.factory,
                                                                disk_controller_key))

        # Add disk devices
        iqn_mapping = self._get_host_iqn_mapping(esxhost, rescan=True)
        for disk in disks:
            if disk['iqn'] in iqn_mapping:
                disk['eui'] = iqn_mapping[disk['iqn']]['eui']
                disk['lun'] = iqn_mapping[disk['iqn']]['lun']
                config.deviceChange.append(
                    self._create_disk(self._client.factory,
                                      disk_controller_key,
                                      disk,
                                      disks.index(disk)))

        # Add network
        for nic in nics:
            unit = nics.index(nic)
            config.deviceChange.append(self._create_nic(self._client.factory,
                                                        'VirtualE1000',
                                                        'Interface %s' % unit,
                                                        '%s interface' % nic['bridge'],
                                                        nic['bridge'],
                                                        unit))

        # Change additional properties
        extra_configs = [
            ('RemoteDisplay.vnc.enabled', 'true'),
            ('RemoteDisplay.vnc.port', str(kvmport)),
            ('RemoteDisplay.vnc.password', 'vmconnect'),
            ('pciBridge0.present', 'true'),
            ('pciBridge4.present', 'true'),
            ('pciBridge4.virtualDev', 'pcieRootPort'),
            ('pciBridge4.functions', '8'),
            ('pciBridge5.present', 'true'),
            ('pciBridge5.virtualDev', 'pcieRootPort'),
            ('pciBridge5.functions', '8'),
            ('pciBridge6.present', 'true'),
            ('pciBridge6.virtualDev', 'pcieRootPort'),
            ('pciBridge6.functions', '8'),
            ('pciBridge7.present', 'true'),
            ('pciBridge7.virtualDev', 'pcieRootPort'),
            ('pciBridge7.functions', '8')
        ]
        for item in extra_configs:
            config.extraConfig.append(self._create_option_value(self._client.factory,
                                                                item[0],
                                                                item[1]))

        task = self._client.service.CreateVM_Task(hostdata['folder'],
                                                  config=config,
                                                  pool=hostdata['resourcePool'],
                                                  host=hostdata['host'])
        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def clone_vm(self, vmid, name, disks, esxhost=None, wait=True):
        """
        Clone a existing VM configuration

        @param vmid: unique id of the vm
        @param name: name of the clone vm
        @param disks: list of disks to use in vm configuration
        @param kvmport: kvm port for the clone vm
        @param esxhost: esx host identifier on which to clone the vm
        @param wait: wait for task to complete or not (True/False)
        """

        esxhost = self._validate_host(esxhost)
        host_data = self._get_host_data(esxhost)

        source_vm_object = self.exists(key=vmid)
        if not source_vm_object:
            raise Exception('VM with key reference %s not found' % vmid)
        source_vm = self._get_object(source_vm_object)
        datastore = self._get_object(source_vm.datastore[0][0])

        # Build basic config information
        config = self._client.factory.create('ns0:VirtualMachineConfigSpec')
        config.name = name
        config.numCPUs = source_vm.config.hardware.numCPU
        config.memoryMB = source_vm.config.hardware.memoryMB
        config.guestId = source_vm.config.guestId
        config.deviceChange = []
        config.extraConfig = []
        config.files = self._create_file_info(self._client.factory, datastore.name)

        disk_controller_key = -101
        config.deviceChange.append(self._create_disk_controller(self._client.factory,
                                                                disk_controller_key))

        # Add disk devices
        for disk in disks:
            config.deviceChange.append(
                self._create_disk(self._client.factory, disk_controller_key,
                                  disk, disks.index(disk), datastore))
            self.copy_file(
                '[{0}] {1}'.format(datastore.name, '%s.vmdk'
                                   % disk['name'].split('_')[-1].replace('-clone', '')),
                '[{0}] {1}'.format(datastore.name, disk['backingdevice']))

        # Add network
        nw_type = type(self._client.factory.create('ns0:VirtualEthernetCardNetworkBackingInfo'))
        for device in source_vm.config.hardware.device:
            if hasattr(device, 'backing') and type(device.backing) == nw_type:
                config.deviceChange.append(self._create_nic(self._client.factory,
                                                            device.__class__.__name__,
                                                            device.deviceInfo.label,
                                                            device.deviceInfo.summary,
                                                            device.backing.deviceName,
                                                            device.unitNumber))

        # Copy additional properties
        extraconfigstoskip = ['nvram']
        for item in source_vm.config.extraConfig:
            if not item.key in extraconfigstoskip:
                config.extraConfig.append(self._create_option_value(self._client.factory,
                                                                    item.key,
                                                                    item.value))

        task = self._client.service.CreateVM_Task(host_data['folder'],
                                                  config=config,
                                                  pool=host_data['resourcePool'],
                                                  host=host_data['host'])
        if wait:
            self.wait_for_task(task)
        return task


    @validate_session
    def register_vm(self, vmxpath, esxhost=None, wait=False):
        """
        Register a vm with a given esxhost
        """
        esxhost = self._validate_host(esxhost)
        hostdata = self._get_host_data(esxhost)
        task = self._client.service.RegisterVM_Task(hostdata['folder'],
                                                    path=vmxpath,
                                                    asTemplate=False,
                                                    pool=hostdata['resourcePool'],
                                                    host=esxhost)
        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def get_vm_guest_info(self, vmid):
        """
        Get guest information about a given vm
        """
        info = self._get_object(self._build_property('VirtualMachine', vmid),
                               properties=['guest', 'guestHeartbeatStatus'])
        setattr(info.guest, 'guestHeartbeatStatus', info.guestHeartbeatStatus)
        return info.guest

    @validate_session
    def delete_vm(self, vmid, wait=False):
        """
        Delete a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        task = self._client.service.Destroy_Task(machine)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def unregister_vm(self, vmid):
        """
        Unregister a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        self._client.service.UnregisterVM(machine)

    @validate_session
    def power_on(self, vmid, wait=False):
        """
        Power on a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        task = self._client.service.PowerOnVM_Task(machine)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def power_off(self, vmid, wait=False):
        """
        Power off a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        task = self._client.service.PowerOffVM_Task(machine)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def shutdown(self, vmid):
        """
        Shut down a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        self._client.service.ShutdownGuest(machine)

    @validate_session
    def suspend(self, vmid, wait=False):
        """
        Suspend a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        task = self._client.service.SuspendVM_Task(machine)

        if wait:
            self.wait_for_task(task)
        return task

    @validate_session
    def get_power_state(self, vmid):
        """
        Get the power state of a given vm
        """
        return self._get_object(self._build_property('VirtualMachine', vmid),
                               properties=['runtime.powerState']).runtime.powerState

    @validate_session
    def register_extension(self, description, xmlurl, company, company_email, key, version):
        """
        Register an extension to the vcenter host we're talking to. In case the extension
        already exists, it will be updated with the given information
        """
        if not self._isVCenter:
            raise Exception('An extension can only be registered to a vCenter Server')

        extension = self.find_extension(key)
        if extension:
            extension.description.label = description
            extension.description.summary = description
            if len(extension.server) == 1 and len(extension.client) == 1:
                extension.server[0].url = xmlurl
                extension.server[0].company = company
                extension.server[0].adminEmail = company_email
                extension.server[0].description.label = description
                extension.server[0].description.summary = description

                extension.client[0].version = version
                extension.client[0].company = company
                extension.client[0].description.label = description
                extension.client[0].description.summary = description
            else:
                raise Exception('Register extension expects only 1 server and 1 client')
            extension.version = version

            return self._client.service.UpdateExtension(self._serviceContent.extensionManager,
                                                        extension)
        else:
            sdkdescription = self._client.factory.create('ns0:Description')
            sdkdescription.label = description
            sdkdescription.summary = description

            serverInfo = self._client.factory.create('ns0:ExtensionServerInfo')
            serverInfo.url = xmlurl
            serverInfo.description = sdkdescription
            serverInfo.company = company
            serverInfo.type = 'com.vmware.vim.viClientScripts'
            serverInfo.adminEmail = company_email

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

            return self._client.service.RegisterExtension(self._serviceContent.extensionManager,
                                                          extension)

    @validate_session
    def find_extension(self, key):
        """
        Finds/checks for a extension with a given key
        """
        return self._client.service.FindExtension(self._serviceContent.extensionManager, key)

    @validate_session
    def wait_for_task(self, task):
        """
        Wait for a task to be completed
        """
        state = self.get_task_info(task).info.state
        while state in ['running', 'queued']:
            sleep(1)
            state = self.get_task_info(task).info.state

    @validate_session
    def get_nfs_datastore_object(self, ip, mountpoint, filename):
        """
        ip : "10.130.12.200", string
        mountpoint: "/srv/volumefs", string
        filename: "cfovs001/vhd0.vmdk"
        identify nfs datastore on this esx host based on ip and mount
        check if filename is present on datastore
        if file is .vmdk return VirtualDisk object for corresponding virtual disk
        if file is .vmx return VirtualMachineConfigInfo for corresponding vm
        """
        directory, file = filename.split('/') #what about vmdks in datastore?
        extension = file.split('.')[1]
        esxhost = self._esxHost
        datastore_object = None
        host_object = self._get_object(esxhost, properties=['datastore'])
        for datastore in host_object.datastore[0]:
            ds_object = self._get_object(datastore)
            if hasattr(ds_object.info, 'nas'):
                if ds_object.info.nas.remoteHost == ip and\
                ds_object.info.nas.remotePath == mountpoint:
                    datastore_object = ds_object
        if not datastore_object:
            raise ValueError('Could not identify NFS datastore ({0}, {1}) on host {2}'.format(ip, mountpoint, self._host))

        browser = self._get_object(datastore_object.browser).obj_identifier
        ds_path = "[%s]" % datastore_object.info.name
        file_query = self._client.factory.create('ns0:VmDiskFileQuery')
        search_spec = self._client.factory.create('ns0:HostDatastoreBrowserSearchSpec')
        search_spec.query = file_query
        search_spec.matchPattern = ["*.{0}".format(extension)]
        tid = self._client.service.SearchDatastoreSubFolders_Task(browser, ds_path, search_spec)
        self.wait_for_task(tid)
        task = self._get_object(tid)
        result = {}
        for hdsbsr in task.info.result.HostDatastoreBrowserSearchResults:
            vm_folder = hdsbsr.folderPath.replace(ds_path, '').strip()
            files = []
            if hasattr(hdsbsr, 'file'):
                files = [f.path for f in hdsbsr.file]
            result[vm_folder] = files
        file_found = file in result.get(directory, [])
        if not file_found:
            raise ValueError('Could not find file {0} on NFS datastore ({1}, {2}) on host {3}'\
                             .format(filename, ip, mountpoint, self._host))
        vm_name = directory
        vms = self._get_object(esxhost,
                               prop_type='VirtualMachine',
                               traversal={'name': 'HostSystemTraversalSpec',
                                          'type': 'HostSystem',
                                          'path': 'vm'},
                               properties=['name', 'config'])
        if isinstance(vms, list):
            vms = [vm for vm in vms if hasattr(vm, 'name') and vm.name == vm_name]
        else:
            if vms.name == vm_name:
                vms = [vms]
            else:
                vms = []
        if not vms:
            raise ValueError('Could not find vmachine {0} on NFS datastore ({1}, {2}) on host {3}'\
                             .format(vm_name, ip, mountpoint, self._host))
        vm = vms[0]
        if file.endswith('.vmx'):
            return vm.config
        elif file.endswith('.vmdk'):
            type_ = self._client.factory.create('ns0:VirtualDisk')
            virtual_disks = [dev for dev in vm.config.hardware.device if isinstance(dev, type(type_))]
            virtual_disks = [vd for vd in virtual_disks if vd.backing.fileName.replace(ds_path, '').strip() == filename]
            if not virtual_disks:
                raise ValueError('Could not find virtual disk {0} on vmachine {1} on NFS datastore({2}, {3}) on host {4}'\
                                 .format(filename, vm_name, ip, mountpoint, self._host))
            return virtual_disks[0]
        else:
            raise ValueError('Unexpected file type {0}'.format(extension))

    def _get_host_data(self, esxhost):
        """
        Get host data for a given esxhost
        """
        hostobject = self._get_object(esxhost, properties=['parent', 'datastore', 'network'])
        datastore = self._get_object(hostobject.datastore[0][0], properties=['info']).info
        computeresource = self._get_object(hostobject.parent, properties=['resourcePool', 'parent'])
        datacenter = self._get_object(computeresource.parent, properties=['parent']).parent
        vm_folder = self._get_object(datacenter, properties=['vmFolder']).vmFolder

        return {'host': esxhost,
                'computeResource': computeresource,
                'resourcePool': computeresource.resourcePool,
                'datacenter': datacenter,
                'folder': vm_folder,
                'datastore': datastore,
                'network': hostobject.network[0]}

    def _get_host_iqn_mapping(self, esxhost, rescan=False):
        """
        Get the IQN mapping for a given esx host, optionally rescanning the host
        """
        # pylint: disable=line-too-long
        regex = re.compile('^key-vim.host.PlugStoreTopology.Path-iqn.+?,(?P<iqn>iqn.*?),t,1-(?P<eui>eui.+)$')  # noqa
        # pylint: enable=line-too-long

        hostobject = self._get_object(esxhost, properties=['configManager.storageSystem'])
        stg_ssystem = self._get_object(hostobject.configManager.storageSystem,
                                      properties=['storageDeviceInfo',
                                                  'storageDeviceInfo.plugStoreTopology.device'])
        if rescan:
            # Force a rescan of the vmfs
            self._client.service.RescanVmfs(stg_ssystem.obj_identifier)
            stg_ssystem = self._get_object(hostobject.configManager.storageSystem,
                                          properties=['storageDeviceInfo',
                                                      'storageDeviceInfo.plugStoreTopology.device'])

        device_info_mapping = {}
        for disk in stg_ssystem.storageDeviceInfo.scsiLun:
            device_info_mapping[disk.key] = disk.uuid

        iqn_mapping = {}
        for device in stg_ssystem.storageDeviceInfo.plugStoreTopology\
                                 .device.HostPlugStoreTopologyDevice:
            for path in device.path:
                match = regex.search(path)
                if match:
                    groups = match.groupdict()
                    iqn_mapping[groups['iqn']] = {'eui': groups['eui'],
                                                 'lun': device.lun,
                                                 'uuid': device_info_mapping[device.lun]}

        return iqn_mapping

    def _get_object(self, key_object, prop_type=None, traversal=None, properties=None):
        """
        Gets an object based on a given set of query parameters. Only the requested properties
        will be loaded. If no properties are specified, all will be loaded
        """
        object_spec = self._client.factory.create('ns0:ObjectSpec')
        object_spec.obj = key_object

        property_spec = self._client.factory.create('ns0:PropertySpec')
        property_spec.type = key_object._type if prop_type is None else prop_type
        if properties is None:
            property_spec.all = True
        else:
            property_spec.all = False
            property_spec.pathSet = properties

        if traversal is not None:
            select_set_ptr = object_spec
            while True:
                select_set_ptr.selectSet = self._client.factory.create('ns0:TraversalSpec')
                select_set_ptr.selectSet.name = traversal['name']
                select_set_ptr.selectSet.type = traversal['type']
                select_set_ptr.selectSet.path = traversal['path']
                if 'traversal' in traversal:
                    traversal = traversal['traversal']
                    select_set_ptr = select_set_ptr.selectSet
                else:
                    break

        property_filter_spec = self._client.factory.create('ns0:PropertyFilterSpec')
        property_filter_spec.objectSet = [object_spec]
        property_filter_spec.propSet = [property_spec]

        found_objects = self._client.service.RetrieveProperties(
            self._serviceContent.propertyCollector,
            [property_filter_spec]
        )

        if len(found_objects) > 0:
            for item in found_objects:
                item.obj_identifier = item.obj
                del item.obj
                for propSet in item.propSet:
                    if '.' in propSet.name:
                        working_item = item
                        path = str(propSet.name).split('.')
                        part_counter = 0
                        for part in path:
                            part_counter += 1
                            if part_counter < len(path):
                                if not part in working_item.__dict__:
                                    setattr(working_item, part, type(part, (), {})())
                                working_item = working_item.__dict__[part]
                            else:
                                setattr(working_item, part, propSet.val)
                    else:
                        setattr(item, propSet.name, propSet.val)
                del item.propSet
            if len(found_objects) == 1:
                return found_objects[0]
            else:
                return found_objects

        return None

    def _build_property(self, property_name, value=None):
        """
        Create a property object with given name and value
        """
        new_property = Property(property_name)
        new_property._type = property_name
        if value is not None:
            new_property.value = value
        return new_property

    def _validate_host(self, host):
        """
        Validates wheteher a given host is valid
        """
        if host is None:
            if self._isVCenter:
                raise Exception('A HostSystem reference is mandatory for a vCenter Server')
            else:
                return self._esxHost
        else:
            if hasattr(host, '_type') and host._type == 'HostSystem':
                return self._get_object(host, properties=['name']).obj_identifier
            else:
                return self._get_object(self._build_property('HostSystem', host),
                                        properties=['name']).obj_identifier

    def _validate_session(self):
        """
        Validates wheteher the current session is logged in. If not, it will get logged in
        """
        if self._sessionID is None:
            self._logout()
            self._sessionID = self._client.service.Login(self._serviceContent.sessionManager,
                                                         self._username,
                                                         self._password, None).key
        else:
            active = False
            try:
                active = self._client.service.SessionIsActive(self._serviceContent.sessionManager,
                                                              sessionID=self._sessionID,
                                                              userName=self._username)
            except:
                pass
            if not active:
                self._logout()
                self._sessionID = self._client.service.Login(self._serviceContent.sessionManager,
                                                             self._username,
                                                             self._password, None).key

    def _logout(self):
        """
        Logs out the current session
        """
        try:
            self._client.service.Logout(self._serviceContent.sessionManager)
        except:
            pass

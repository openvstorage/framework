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
This module contains all code for using the VMware SOAP API/SDK
"""

from time import sleep
import datetime
import re

from suds.client import Client, WebFault
from suds.cache import ObjectCache
from suds.sudsobject import Property
from suds.plugin import MessagePlugin
from ovs.log.logHandler import LogHandler

logger = LogHandler('extensions', name='vmware sdk')


class NotAuthenticatedException(BaseException):
    pass


def authenticated(force=False):
    """
    Decorator to make that a login is executed in case the current session isn't valid anymore
    @param force: Force a (re)login, as some methods also work when not logged in
    """
    def wrapper(function):
        def new_function(self, *args, **kwargs):
            self.__doc__ = function.__doc__
            try:
                if force:
                    self._login()
                return function(self, *args, **kwargs)
            except WebFault as fault:
                if 'The session is not authenticated' in str(fault):
                    logger.debug('Received WebFault authentication failure, logging in...')
                    self._login()
                    return function(self, *args, **kwargs)
                raise
            except NotAuthenticatedException:
                logger.debug('Received NotAuthenticatedException, logging in...')
                self._login()
                return function(self, *args, **kwargs)
        return new_function
    return wrapper


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
        self._check_session = True

        self._cache = ObjectCache()
        self._cache.setduration(weeks=1)

        self._client = Client('https://%s/sdk/vimService?wsdl' % host,
                              cache=self._cache,
                              cachingpolicy=1)
        self._client.set_options(location='https://%s/sdk' % host,
                                 plugins=[ValueExtender()])

        service_reference = self._build_property('ServiceInstance')
        self._serviceContent = self._client.service.RetrieveServiceContent(
            service_reference)

        # In case of an ESXi host, this would be 'HostAgent'
        self._is_vcenter = self._serviceContent.about.apiType == 'VirtualCenter'
        if not self._is_vcenter:
            # pylint: disable=line-too-long
            self._login()
            self._esxHost = self._get_object(
                self._serviceContent.rootFolder,
                prop_type='HostSystem',
                traversal={'name': 'FolderTraversalSpec',
                           'type': 'Folder',
                           'path': 'childEntity',
                           'traversal': {'name': 'DatacenterTraversalSpec',
                                         'type': 'Datacenter',
                                         'path': 'hostFolder',
                                         'traversal': {'name': 'DFolderTraversalSpec',
                                                       'type': 'Folder',
                                                       'path': 'childEntity',
                                                       'traversal': {'name': 'ComputeResourceTravelSpec',  # noqa
                                                                     'type': 'ComputeResource',
                                                                     'path': 'host'}}}},
                properties=['name']
            ).obj_identifier
            # pylint: enable=line-too-long
        else:
            self._esxHost = None

    @authenticated(force=True)
    def _get_vcenter_hosts(self):
        """
        reload vCenter info (host name and summary)
        """
        if not self._is_vcenter:
            raise RuntimeError('Must be connected to a vCenter Server API.')
        datacenter_info = self._get_object(
            self._serviceContent.rootFolder,
            prop_type='HostSystem',
            traversal={'name': 'FolderTraversalSpec',
                       'type': 'Folder',
                       'path': 'childEntity',
                       'traversal': {'name': 'DatacenterTraversalSpec',
                                     'type': 'Datacenter',
                                     'path': 'hostFolder',
                                     'traversal': {'name': 'DFolderTraversalSpec',
                                                   'type': 'Folder',
                                                   'path': 'childEntity',
                                                   'traversal': {'name': 'ComputeResourceTravelSpec',  # noqa
                                                                 'type': 'ComputeResource',
                                                                 'path': 'host'}}}},
            properties=['name', 'summary.runtime', 'config.virtualNicManagerInfo.netConfig']
        )
        return datacenter_info

    def get_host_status_by_ip(self, host_ip):
        """
        Return host status by ip, from vcenter info
        Must be connected to a vcenter server api
        """
        host = self._get_host_info_by_ip(host_ip)
        return host.summary.runtime.powerState

    def get_host_status_by_pk(self, pk):
        """
        Return host status by pk, from vcenter info
        Must be connected to a vcenter server api
        """
        host = self._get_host_info_by_pk(pk)
        return host.summary.runtime.powerState

    def get_host_primary_key(self, host_ip):
        """
        Return host primary key, based on current ip
        Must be connected to a vcenter server api
        """
        host = self._get_host_info_by_ip(host_ip)
        return host.obj_identifier.value

    def _get_host_info_by_ip(self, host_ip):
        """
        Return HostSystem object by ip, from vcenter info
        Must be connected to a vcenter server api
        """
        datacenter_info = self._get_vcenter_hosts()
        for host in datacenter_info:
            for nic in host.config.virtualNicManagerInfo.netConfig.VirtualNicManagerNetConfig:
                if nic.nicType == 'management':
                    for vnic in nic.candidateVnic:
                        if vnic.spec.ip.ipAddress == host_ip:
                            return host
        raise RuntimeError('Host with ip {0} not found in datacenter info'.format(host_ip))

    def _get_host_info_by_pk(self, pk):
        """
        Return HostSystem object by pk, from vcenter info
        Must be connected to a vcenter server api
        """
        datacenter_info = self._get_vcenter_hosts()
        for host in datacenter_info:
            if host.obj_identifier.value == pk:
                return host

    def get_hosts(self):
        """
        Gets a neutral list of all hosts available
        """
        host_data = self._get_vcenter_hosts()
        host_data = [] if host_data is None else host_data
        hosts = {}
        for host in host_data:
            ips = []
            for nic in host.config.virtualNicManagerInfo.netConfig.VirtualNicManagerNetConfig:
                if nic.nicType == 'management':
                    for vnic in nic.candidateVnic:
                        ips.append(vnic.spec.ip.ipAddress)
            hosts[host.obj_identifier.value] = {'name': host.name,
                                                'ips': ips}
        return hosts

    def test_connection(self):
        """
        Tests the connection
        """
        self._login()
        return True

    def list_hosts_in_datacenter(self):
        """
        return a list of registered host names in vCenter
        must be connected to a vcenter server api
        """
        datacenter_info = self._get_vcenter_hosts()
        return [host.name for host in datacenter_info]

    def validate_result(self, result, message=None):
        """
        Validates a given result. Returning True if the task succeeded, raising an error if not
        """
        if hasattr(result, '_type') and result._type == 'Task':
            return self.validate_result(self.get_task_info(result), message)
        elif hasattr(result, 'info'):
            if result.info.state == 'success':
                return True
            elif result.info.state == 'error':
                error = result.info.error.localizedMessage
                raise Exception(('%s: %s' % (message, error)) if message else error)
        raise Exception(('%s: %s' % (message, 'Unexpected result'))
                        if message else 'Unexpected result')

    @authenticated()
    def get_task_info(self, task):
        """
        Loads the task details
        """
        return self._get_object(task)

    @authenticated()
    def get_vm_ip_information(self):
        """
        Get the IP information for all vms on a given esxi host
        """
        esxhost = self._validate_host(None)
        configuration = []
        for vm in self._get_object(esxhost,
                                   prop_type='VirtualMachine',
                                   traversal={
                                       'name': 'HostSystemTraversalSpec',
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

    @authenticated()
    def exists(self, name=None, key=None):
        """
        Checks whether a vm with a given name or key exists on a given esxi host
        """
        esxhost = self._validate_host(None)
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
                    return self._get_object(
                        self._build_property('VirtualMachine', key),
                        properties=['name']).obj_identifier
            except:
                return None
        else:
            raise Exception('A name or key should be passed.')

    @authenticated()
    def get_vm(self, key):
        """
        Retreives a vm object, based on its key
        """
        vmid = self.exists(key=key)
        if vmid is None:
            raise RuntimeError('Virtual Machine with key {} could not be found.'.format(key))
        vm = self._get_object(vmid)
        return vm

    @authenticated()
    def get_vms(self, ip, mountpoint):
        """
        Get all vMachines using a given nfs share
        """
        esxhost = self._validate_host(None)
        datastore = self.get_datastore(ip, mountpoint)
        filtered_vms = []
        vms = self._get_object(esxhost,
                               prop_type='VirtualMachine',
                               traversal={'name': 'HostSystemTraversalSpec',
                                          'type': 'HostSystem',
                                          'path': 'vm'},
                               properties=['name', 'config'])
        for vm in vms:
            mapping = self._get_vm_datastore_mapping(vm)
            if datastore.name in mapping:
                filtered_vms.append(vm)
        return filtered_vms

    @authenticated()
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
                backing = self._client.factory.create(
                    'ns0:VirtualDiskFlatVer2BackingInfo')
                backing.diskMode = mode
                device = self._client.factory.create('ns0:VirtualDisk')
                device.backing = backing
                diskSpec = self._client.factory.create(
                    'ns0:VirtualDeviceConfigSpec')
                diskSpec.operation = 'edit'
                diskSpec.fileOperation = None
                diskSpec.device = device
                config.deviceChange.append(diskSpec)

        task = self._client.service.ReconfigVM_Task(vm.obj_identifier, config)

        if wait:
            self.wait_for_task(task)
        return task

    def _create_disk(self, factory, key, disk, unit, datastore):
        """
        Creates a disk spec for a given backing device
        Example for parameter disk: {'name': diskname, 'backingdevice': 'disk-flat.vmdk'}
        """
        deviceInfo = factory.create('ns0:Description')
        deviceInfo.label = disk['name']
        deviceInfo.summary = 'Disk %s' % disk['name']
        backing = factory.create('ns0:VirtualDiskFlatVer2BackingInfo')
        backing.diskMode = 'persistent'
        backing.fileName = '[%s] %s' % (datastore.name, disk['backingdevice'])
        backing.thinProvisioned = True
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

    @authenticated()
    def copy_file(self, source, destination, wait=True):
        """
        Copies a file on the datastore
        """
        task = self._client.service.CopyDatastoreFile_Task(
            _this=self._serviceContent.fileManager,
            sourceName=source,
            destinationName=destination)

        if wait:
            self.wait_for_task(task)
        return task

    @authenticated()
    def create_vm_from_template(self, name, source_vm, disks, ip, mountpoint, wait=True):
        """
        Create a vm based on an existing vtemplate on specified tgt hypervisor
        Raises RuntimeError if datastore is not available at (ip, mountpoint)
        """

        esxhost = self._validate_host(None)
        host_data = self._get_host_data(esxhost)

        datastore = self.get_datastore(ip, mountpoint)
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
        config.deviceChange.append(
            self._create_disk_controller(self._client.factory,
                                         disk_controller_key))

        # Add disk devices
        for disk in disks:
            config.deviceChange.append(
                self._create_disk(self._client.factory, disk_controller_key,
                                  disk, disks.index(disk), datastore))

        # Add network
        nw_type = type(self._client.factory.create('ns0:VirtualEthernetCardNetworkBackingInfo'))
        for device in source_vm.config.hardware.device:
            if hasattr(device, 'backing') and type(device.backing) == nw_type:
                config.deviceChange.append(
                    self._create_nic(self._client.factory,
                                     device.__class__.__name__,
                                     device.deviceInfo.label,
                                     device.deviceInfo.summary,
                                     device.backing.deviceName,
                                     device.unitNumber))

        # Copy additional properties
        extraconfigstoskip = ['nvram']
        for item in source_vm.config.extraConfig:
            if not item.key in extraconfigstoskip:
                config.extraConfig.append(
                    self._create_option_value(self._client.factory,
                                              item.key,
                                              item.value))

        task = self._client.service.CreateVM_Task(host_data['folder'],
                                                  config=config,
                                                  pool=host_data['resourcePool'],
                                                  host=host_data['host'])

        if wait:
            self.wait_for_task(task)

        return task

    @authenticated()
    def clone_vm(self, vmid, name, disks, wait=True):
        """
        Clone a existing VM configuration

        @param vmid: unique id of the vm
        @param name: name of the clone vm
        @param disks: list of disks to use in vm configuration
        @param kvmport: kvm port for the clone vm
        @param esxhost: esx host identifier on which to clone the vm
        @param wait: wait for task to complete or not (True/False)
        """

        esxhost = self._validate_host(None)
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
        config.files = self._create_file_info(
            self._client.factory, datastore.name)

        disk_controller_key = -101
        config.deviceChange.append(
            self._create_disk_controller(self._client.factory,
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
        nw_type = type(self._client.factory.create(
            'ns0:VirtualEthernetCardNetworkBackingInfo'))
        for device in source_vm.config.hardware.device:
            if hasattr(device, 'backing') and type(device.backing) == nw_type:
                config.deviceChange.append(
                    self._create_nic(self._client.factory,
                                     device.__class__.__name__,
                                     device.deviceInfo.label,
                                     device.deviceInfo.summary,
                                     device.backing.deviceName,
                                     device.unitNumber))

        # Copy additional properties
        extraconfigstoskip = ['nvram']
        for item in source_vm.config.extraConfig:
            if not item.key in extraconfigstoskip:
                config.extraConfig.append(
                    self._create_option_value(self._client.factory,
                                              item.key,
                                              item.value))

        task = self._client.service.CreateVM_Task(host_data['folder'],
                                                  config=config,
                                                  pool=host_data['resourcePool'],
                                                  host=host_data['host'])
        if wait:
            self.wait_for_task(task)
        return task

    @authenticated()
    def get_vm(self, key):
        vmid = self.exists(key=key)
        if vmid is None:
            raise RuntimeError('Virtual Machine with key {} could not be found.'.format(key))
        vm = self._get_object(vmid)
        return vm

    @authenticated()
    def get_datastore(self, ip, mountpoint):
        """
        @param ip : hypervisor ip to query for datastore presence
        @param mountpoint: nfs mountpoint on hypervisor
        @rtype: sdk datastore object
        @return: object when found else None
        """

        datastore = None
        esxhost = self._validate_host(None)
        host_system = self._get_object(esxhost, properties=['datastore'])
        for store in host_system.datastore[0]:
            store = self._get_object(store)
            if not store.summary.accessible:
                raise RuntimeError('Datastore {0} is not accessible at mountpoint {1}'.format(store.name, mountpoint))
            if hasattr(store.info, 'nas'):
                if store.info.nas.remoteHost == ip and store.info.nas.remotePath == mountpoint:
                    datastore = store

        return datastore

    @authenticated()
    def is_datastore_available(self, ip, mountpoint):
        """
        @param ip : hypervisor ip to query for datastore presence
        @param mountpoint: nfs mountpoint on hypervisor
        @rtype: boolean
        @return: True | False
        """

        if self.get_datastore(ip, mountpoint):
            return True
        else:
            return False

    def make_agnostic_config(self, vm_object):
        regex = '\[([^\]]+)\]\s(.+)'
        match = re.search(regex, vm_object.config.files.vmPathName)
        esxhost = self._validate_host(None)

        config = {'name': vm_object.config.name,
                  'id': vm_object.obj_identifier.value,
                  'backing': {'filename': match.group(2),
                              'datastore': match.group(1)},
                  'disks': [],
                  'datastores': {}}

        for device in vm_object.config.hardware.device:
            if device.__class__.__name__ == 'VirtualDisk':
                if device.backing is not None and \
                        device.backing.fileName is not None:
                    backingfile = device.backing.fileName
                    match = re.search(regex, backingfile)
                    if match:
                        filename = match.group(2)
                        backingfile = filename.replace('.vmdk', '-flat.vmdk')
                        config['disks'].append({'filename': filename,
                                                'backingfilename': backingfile,
                                                'datastore': match.group(1),
                                                'name': device.deviceInfo.label,
                                                'order': device.unitNumber})

        host_system = self._get_object(esxhost, properties=['datastore'])
        for store in host_system.datastore[0]:
            store = self._get_object(store)
            if hasattr(store.info, 'nas'):
                config['datastores'][store.info.name] = '{}:{}'.format(store.info.nas.remoteHost,
                                                                       store.info.nas.remotePath)

        return config

    @authenticated()
    def delete_vm(self, vmid, wait=False):
        """
        Delete a given vm
        """
        machine = self._build_property('VirtualMachine', vmid)
        task = self._client.service.Destroy_Task(machine)

        if wait:
            self.wait_for_task(task)
        return task

    @authenticated()
    def get_power_state(self, vmid):
        """
        Get the power state of a given vm
        """
        return self._get_object(self._build_property('VirtualMachine', vmid),
                                properties=['runtime.powerState']).runtime.powerState

    @authenticated()
    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Mounts a given NFS export as a datastore
        """
        esxhost = self._validate_host(None)
        host = self._get_object(esxhost, properties=['datastore',
                                                     'name',
                                                     'configManager',
                                                     'configManager.datastoreSystem'])
        for store in host.datastore[0]:
            store = self._get_object(store)
            if hasattr(store.info, 'nas'):
                if store.info.name == name:
                    if store.info.nas.remoteHost == remote_host and \
                            store.info.nas.remotePath == remote_path:
                        # We'll remove this store, as it's identical to the once we'll add,
                        # forcing a refresh
                        self._client.service.RemoveDatastore(host.configManager.datastoreSystem,
                                                             store.obj_identifier)
                        break
                    else:
                        raise RuntimeError('A datastore {0} already exists, pointing to {1}:{2}'.format(
                            name, store.info.nas.remoteHost, store.info.nas.remotePath
                        ))
        spec = self._client.factory.create('ns0:HostNasVolumeSpec')
        spec.accessMode = 'readWrite'
        spec.localPath = name
        spec.remoteHost = remote_host
        spec.remotePath = remote_path
        spec.type = 'nfs'
        return self._client.service.CreateNasDatastore(host.configManager.datastoreSystem, spec)

    @authenticated()
    def wait_for_task(self, task):
        """
        Wait for a task to be completed
        """
        state = self.get_task_info(task).info.state
        while state in ['running', 'queued']:
            sleep(1)
            state = self.get_task_info(task).info.state

    @authenticated()
    def get_nfs_datastore_object(self, ip, mountpoint, filename):
        """
        ip : "10.130.12.200", string
        mountpoint: "/srv/volumefs", string
        filename: "cfovs001/vhd0(-flat).vmdk"
        identify nfs datastore on this esx host based on ip and mount
        check if filename is present on datastore
        if file is .vmdk return VirtualDisk object for corresponding virtual disk
        if file is .vmx return VirtualMachineConfigInfo for corresponding vm

        @rtype: tuple
        @return: A tuple. First item: vm config, second item: Device if a vmdk was given
        """

        filename = filename.replace('-flat.vmdk', '.vmdk')  # Support both -flat.vmdk and .vmdk
        if not filename.endswith('.vmdk') and not filename.endswith('.vmx'):
            raise ValueError('Unexpected filetype')
        esxhost = self._validate_host(None)

        datastore = self.get_datastore(ip, mountpoint)
        if not datastore:
            raise RuntimeError('Could not find datastore')

        vms = self._get_object(esxhost,
                               prop_type='VirtualMachine',
                               traversal={'name': 'HostSystemTraversalSpec',
                                          'type': 'HostSystem',
                                          'path': 'vm'},
                               properties=['name', 'config'])
        if not vms:
            raise RuntimeError('No vMachines found')
        for vm in vms:
            mapping = self._get_vm_datastore_mapping(vm)
            if datastore.name in mapping:
                if filename in mapping[datastore.name]:
                    return vm, mapping[datastore.name][filename]
        raise RuntimeError('Could not locate given file on the given datastore')

    def _get_vm_datastore_mapping(self, vm):
        """
        Creates a datastore mapping for a vm's devices
        Structure
            {<datastore name>: {<backing filename>: <device>,
                                <backing filename>: <device>},
             <datastore name>: {<backing filename>: <device>}}
        Example
            {'datastore A': {'/machine1/machine1.vmx': <device>,
                             '/machine1/disk1.vmdk': <device>},
             'datastore B': {'/machine1/disk2.vmdk': <device>}}
        """
        def extract_names(backingfile, given_mapping, metadata=None):
            match = re.search('\[([^\]]+)\]\s(.+)', backingfile)
            if match:
                datastore_name = match.group(1)
                filename = match.group(2)
                if datastore_name not in mapping:
                    given_mapping[datastore_name] = {}
                given_mapping[datastore_name][filename] = metadata
            return given_mapping

        virtual_disk_type = self._client.factory.create('ns0:VirtualDisk')
        flat_type = self._client.factory.create('ns0:VirtualDiskFlatVer2BackingInfo')

        mapping = {}
        for device in vm.config.hardware.device:
            if isinstance(device, type(virtual_disk_type)):
                if device.backing is not None and isinstance(device.backing, type(flat_type)):
                    mapping = extract_names(device.backing.fileName, mapping, device)
        mapping = extract_names(vm.config.files.vmPathName, mapping)
        return mapping

    def _get_host_data(self, esxhost):
        """
        Get host data for a given esxhost
        """
        hostobject = self._get_object(
            esxhost, properties=['parent', 'datastore', 'network'])
        datastore = self._get_object(
            hostobject.datastore[0][0], properties=['info']).info
        computeresource = self._get_object(
            hostobject.parent, properties=['resourcePool', 'parent'])
        datacenter = self._get_object(
            computeresource.parent, properties=['parent']).parent
        vm_folder = self._get_object(
            datacenter, properties=['vmFolder']).vmFolder

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

        hostobject = self._get_object(
            esxhost, properties=['configManager.storageSystem'])
        stg_ssystem = self._get_object(hostobject.configManager.storageSystem,
                                       properties=['storageDeviceInfo',
                                                   'storageDeviceInfo.plugStoreTopology.device'])
        if rescan:
            # Force a rescan of the vmfs
            self._client.service.RescanVmfs(stg_ssystem.obj_identifier)
            stg_ssystem = self._get_object(
                hostobject.configManager.storageSystem,
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
                select_set_ptr.selectSet = self._client.factory.create(
                    'ns0:TraversalSpec')
                select_set_ptr.selectSet.name = traversal['name']
                select_set_ptr.selectSet.type = traversal['type']
                select_set_ptr.selectSet.path = traversal['path']
                if 'traversal' in traversal:
                    traversal = traversal['traversal']
                    select_set_ptr = select_set_ptr.selectSet
                else:
                    break

        property_filter_spec = self._client.factory.create(
            'ns0:PropertyFilterSpec')
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

                if hasattr(item, 'missingSet'):
                    for missing_item in item.missingSet:
                        if missing_item.fault.fault.__class__.__name__ == 'NotAuthenticated':
                            raise NotAuthenticatedException()

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
            if self._is_vcenter:
                raise Exception(
                    'A HostSystem reference is mandatory for a vCenter Server')
            else:
                return self._esxHost
        else:
            if hasattr(host, '_type') and host._type == 'HostSystem':
                return self._get_object(host, properties=['name']).obj_identifier
            else:
                return self._get_object(
                    self._build_property('HostSystem', host),
                    properties=['name']).obj_identifier

    def _login(self):
        """
        Executes a logout (to make sure we're logged out), and logs in again
        """
        self._logout()
        self._sessionID = self._client.service.Login(
            self._serviceContent.sessionManager,
            self._username,
            self._password,
            None
        ).key

    def _logout(self):
        """
        Logs out the current session
        """
        try:
            self._client.service.Logout(self._serviceContent.sessionManager)
        except:
            pass

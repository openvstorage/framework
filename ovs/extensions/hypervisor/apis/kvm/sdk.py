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
This module contains all code for using the KVM libvirt api
"""

from xml.etree import ElementTree
from threading import Lock
import subprocess
import os, glob
import re
import time
from ovs.log.logHandler import LogHandler

logger = LogHandler('extensions', name='kvm sdk')
ROOT_PATH = '/etc/libvirt/qemu/'  # Get static info from here, or use dom.XMLDesc(0)
RUN_PATH = '/var/run/libvirt/qemu/'  # Get live info from here


# Helpers
def _recurse(treeitem):
    result = {}
    for key, item in treeitem.items():
        result[key] = item
    for child in treeitem.getchildren():
        result[child.tag] = _recurse(child)
        for key, item in child.items():
            result[child.tag][key] = item
    return result


def authenticated(function):
    """
    Decorator that make sure all required calls are running onto a connected SDK
    """
    def new_function(self, *args, **kwargs):
        self.__doc__ = function.__doc__
        try:
            if self._conn is None:
                self._connect()
            return function(self, *args, **kwargs)
        finally:
            try:
                self._disconnect()
            except:
                pass
    return new_function


class Sdk(object):
    """
    This class contains all SDK related methods
    """

    def __init__(self, host='localhost', login='root'):
        logger.debug('Init libvirt')
        import libvirt
        self.states = {libvirt.VIR_DOMAIN_NOSTATE:  'NO STATE',
                       libvirt.VIR_DOMAIN_RUNNING:  'RUNNING',
                       libvirt.VIR_DOMAIN_BLOCKED:  'BLOCKED',
                       libvirt.VIR_DOMAIN_PAUSED:   'PAUSED',
                       libvirt.VIR_DOMAIN_SHUTDOWN: 'SHUTDOWN',
                       libvirt.VIR_DOMAIN_SHUTOFF:  'TURNEDOFF',
                       libvirt.VIR_DOMAIN_CRASHED:  'CRASHED'}

        self.libvirt = libvirt
        self.host = host
        self.login = login
        self._conn = None
        self._ssh_client = None
        logger.debug('Init complete')

    def _connect(self, attempt = 0):
        if self._conn:
            self._disconnect()  # Clean up existing conn
        logger.debug('Init connection: %s, %s, %s, %s', self.host, self.login, os.getgid(), os.getuid())
        try:
            if self.host == 'localhost':  # Or host in (localips...):
                self._conn = self.libvirt.open('qemu:///system')  # Only local connection
            else:
                self._conn = self.libvirt.open('qemu+ssh://{0}@{1}/system'.format(self.login, self.host))
        except self.libvirt.libvirtError as le:
            logger.error('Error during connect: %s (%s)', str(le), le.get_error_code())
            if attempt < 5:
                time.sleep(1)
                self._connect(attempt + 1)
            else:
                raise
        return True

    def _disconnect(self):
        logger.debug('Disconnecting libvirt')
        if self._conn:
            try:
                self._conn.close()
            except self.libvirt.libvirtError as le:
                # Ignore error, connection might be already closed
                logger.error('Error during disconnect: %s (%s)', str(le), le.get_error_code())

        self._conn = None
        return True

    @authenticated
    def test_connection(self):
        """
        Tests the connection
        """
        _ = self
        return True

    @staticmethod
    def _get_disks(vm_object):
        tree = ElementTree.fromstring(vm_object.XMLDesc(0))
        return [_recurse(item) for item in tree.findall('devices/disk')]

    @staticmethod
    def _get_nics(vm_object):
        tree = ElementTree.fromstring(vm_object.XMLDesc(0))
        return [_recurse(item) for item in tree.findall('devices/interface')]

    @staticmethod
    def _get_ram(vm_object):
        """
        returns RAM size in MiB
        MUST BE INTEGER! not float
        """
        tree = ElementTree.fromstring(vm_object.XMLDesc(0))
        mem = tree.findall('memory')[0]
        unit = mem.items()[0][1]
        value = mem.text
        if unit == 'MiB':
            return int(value)
        elif unit == 'KiB':
            return int(value) / 1024
        elif unit == 'GiB':
            return int(value) * 1024

    @staticmethod
    def _get_disk_size(filename):
        cmd = ['qemu-img', 'info', filename]
        try:
            out = subprocess.check_output(' '.join(cmd),
                                          stderr=subprocess.STDOUT,
                                          shell=True)
        except subprocess.CalledProcessError as cpe:
            return cpe.output.strip()

        out = out.split('\n')
        for line in out:
            if line.startswith('virtual size: '):
                size = line.split('virtual size: ')[1].split(' ')[0]
                return size
        return 0

    def _get_vm_pid(self, vm_object):
        """
        return pid of kvm process running this machine (if any)
        """
        if self.get_power_state(vm_object.name()) == 'RUNNING':
            pid_path = '{}/{}.pid'.format(RUN_PATH, vm_object.name())
            try:
                with open(pid_path, 'r') as pid_file:
                    pid = pid_file.read()
                return int(pid)
            except IOError:
                # vMachine is running but no run file?
                return '-1'
        return '-2'  # No pid, machine is halted

    @authenticated
    def make_agnostic_config(self, vm_object):
        """
        return an agnostic config (no hypervisor specific type or structure)
        """
        storage_ip = '127.0.0.1'
        regex = '/mnt/([^/]+)/(.+$)'
        config = {'disks': []}
        mountpoints = []

        order = 0
        for disk in Sdk._get_disks(vm_object):
            # Skip cdrom/iso
            if disk['device'] == 'cdrom':
                continue

            # Load backing filename
            if 'file' in disk['source']:
                backingfilename = disk['source']['file']
            elif 'dev' in disk['source']:
                backingfilename = disk['source']['dev']
            else:
                continue
            match = re.search(regex, backingfilename)
            if match is None:
                continue

            # Cleaning up
            mountpoint = '/mnt/{0}'.format(match.group(1))
            filename = backingfilename.replace(mountpoint, '').strip('/')
            if 'alias' in disk:
                # A diskname was specified
                diskname = disk['alias'].get('name', 'unknown')
            else:
                # No diskname specified. Using the .raw filename
                diskname = filename.split('/')[-1].split('.')[0]

            # Collecting data
            config['disks'].append({'filename': filename,
                                    'backingfilename': filename,
                                    'datastore': mountpoint,
                                    'name': diskname,
                                    'order': order})
            order += 1
            mountpoints.append(mountpoint)

        vm_filename = self.ssh_run("grep -l '<uuid>{0}</uuid>' {1}*.xml".format(vm_object.UUIDString(), ROOT_PATH))
        vm_filename = vm_filename.strip().split('/')[-1]
        vm_location = self._get_unique_id()
        vm_datastore = None
        possible_datastores = self.ssh_run("find /mnt -name '{0}'".format(vm_filename)).split('\n')
        for datastore in possible_datastores:
            # Filter results so only the correct machineid/xml combinations are left over
            if '{0}/{1}'.format(vm_location, vm_filename) in datastore.strip():
                for mountpoint in mountpoints:
                    if mountpoint in datastore.strip():
                        vm_datastore = mountpoint

        config['name'] = vm_object.name()
        config['id'] = str(vm_object.UUIDString())
        config['backing'] = {'filename': '{0}/{1}'.format(vm_location, vm_filename),
                             'datastore': vm_datastore}
        config['datastores'] = dict((mountpoint, '{}:{}'.format(storage_ip, mountpoint)) for mountpoint in mountpoints)

        return config

    @authenticated
    def get_power_state(self, vmid):
        """
        return vmachine state
        vmid is the name
        """
        vm = self.get_vm_object(vmid)
        state = vm.info()[0]
        return self.states.get(state, 'UNKNOWN')

    @authenticated
    def get_vm_object(self, vmid):
        """
        return virDomain object representing virtual machine
        vmid is the name or the uuid
        cannot use ID, since for a stopped vm id is always -1
        """
        func = 'lookupByUUIDString'
        try:
            import uuid
            uuid.UUID(vmid)
        except ValueError:
            func = 'lookupByName'
        try:
            return getattr(self._conn, func)(vmid)
        except self.libvirt.libvirtError as le:
            logger.error(str(le))
            try:
                self._connect()
                return getattr(self._conn, func)(vmid)
            except self.libvirt.libvirtError as le:
                logger.error(str(le))
                raise RuntimeError('Virtual Machine with id/name {} could not be found.'.format(vmid))

    def get_vm_object_by_filename(self, filename):
        """
        get vm based on filename: vmachines/template/template.xml
        """
        vmid = filename.split('/')[-1].replace('.xml', '')
        return self.get_vm_object(vmid)

    @authenticated
    def get_vms(self):
        """
        return a list of virDomain objects, representing virtual machines
        """
        return self._conn.listAllDomains()

    def shutdown(self, vmid):
        vm_object = self.get_vm_object(vmid)
        vm_object.shutdown()
        return self.get_power_state(vmid)

    @authenticated
    def delete_vm(self, vmid, devicename, disks_info):
        """
        Delete domain from libvirt
        Try to delete all files from vpool (xml, .raw)
        """
        vm_object = None
        try:
            vm_object = self.get_vm_object(vmid)
        except Exception as ex:
            logger.error('SDK domain retrieve failed: {}'.format(ex))
        found_file = self.file_exists(devicename)
        if found_file:
            self.ssh_run('rm {0}'.format(found_file))
            logger.info('File on vpool deleted: {0}'.format(found_file))
        if vm_object:
            found_file = ''
            # VM partially created, most likely we have disks
            for disk in self._get_disks(vm_object):
                if disk['device'] == 'cdrom':
                    continue
                if 'file' in disk['source']:
                    found_file = disk['source']['file']
                elif 'dev' in disk['source']:
                    found_file = disk['source']['dev']
                if found_file and os.path.exists(found_file) and os.path.isfile(found_file):
                    self.ssh_run('rm {0}'.format(found_file))
                    logger.info('File on vpool deleted: {0}'.format(found_file))
            vm_object.undefine()
        elif disks_info:
            # VM not created, we have disks to rollback
            for path, devicename in disks_info:
                found_file = '{}/{}'.format(path, devicename)
                if os.path.exists(found_file) and os.path.isfile(found_file):
                    self.ssh_run('rm {0}'.format(found_file))
                    logger.info('File on vpool deleted: {0}'.format(found_file))
        return True

    def power_on(self, vmid):
        vm_object = self.get_vm_object(vmid)
        vm_object.create()
        return self.get_power_state(vmid)

    def file_exists(self, devicename):
        """
        Check if devicename .xml exists on any mnt vpool
        """
        file_matcher = '/mnt/*/{0}'.format(devicename)
        for found_file in glob.glob(file_matcher):
            if os.path.exists(found_file) and os.path.isfile(found_file):
                return found_file
        return False

    @authenticated
    def clone_vm(self, vmid, name, disks):
        """
        create a clone vm
        similar to create_vm_from template (?)
        """
        raise NotImplementedError()

    @authenticated
    def create_vm_from_template(self, name, source_vm, disks, mountpoint):
        """
        Create a vm based on an existing template on specified hypervisor
        #source_vm might be an esx machine object or a libvirt.VirDomain object
         @TODO: make sure source_vm is agnostic object (dict)
        #disks: list of dicts (agnostic)
         {'diskguid': new_disk.guid, 'name': new_disk.name, 'backingdevice': device_location.strip('/')}
        ---
         kvm doesn't have datastores, all files should be in /mnt/vpool_x/name/ and shared between nodes
         to "migrate" a kvm machine just symlink the xml on another node and use virsh define name.xml to reimport it
         (assuming that the vpool is in the same location)
        ---
        """
        vm_disks = []

        # Get agnostic config of source vm
        if hasattr(source_vm, 'config'):
            vcpus = source_vm.config.hardware.numCPU
            ram = source_vm.config.hardware.memoryMB
        elif isinstance(source_vm, self.libvirt.virDomain):
            vcpus = source_vm.info()[3]
            ram = Sdk._get_ram(source_vm)
        else:
            raise ValueError('Unexpected object type {} {}'.format(source_vm, type(source_vm)))

        # Get nics of source ram - for now only KVM
        networks = []
        for nic in Sdk._get_nics(source_vm):
            if nic.get('type', None) == 'network':
                source = nic.get('source', {}).get('network', 'default')
                model = nic.get('model', {}).get('type', 'e1000')
                networks.append(('network={0}'.format(source), 'mac=RANDOM', 'model={0}'.format(model)))
                # MAC is always RANDOM

        # Assume disks are raw
        for disk in disks:
            vm_disks.append(('/{}/{}'.format(mountpoint.strip('/'), disk['backingdevice'].strip('/')), 'virtio'))

        self._vm_create(name = name,
                        vcpus = vcpus,
                        ram = int(ram),
                        disks = vm_disks,
                        networks = networks)

        try:
            return self.get_vm_object(name).UUIDString()
        except self.libvirt.libvirtError as le:
            logger.error(str(le))
            try:
                self._connect()
                return self.get_vm_object(name).UUIDString()
            except self.libvirt.libvirtError as le:
                logger.error(str(le))
                raise RuntimeError('Virtual Machine with id/name {} could not be found.'.format(name))

    def _vm_create(self, name, vcpus, ram, disks,
                   cdrom_iso=None, os_type=None, os_variant=None, vnc_listen='0.0.0.0',
                   networks=None, start = False):
        """
        disks = list of tuples [(disk_name, disk_size_GB, bus ENUM(virtio, ide, sata)]
        #e.g [(/vms/vm1.vmdk,10,virtio), ]
        #when using existing storage, size can be ommited
        #e.g [(/vms/vm1.raw,raw,virtio), ]
        #network: (network name: "default", specific mac or RANDOM, nic model as seen inside vmachine: e1000
        @param start: should the guest be started after create
        """
        if networks is None:
            networks = [('network=default', 'mac=RANDOM', 'model=e1000')]
        command = 'virt-install'
        options = ['--connect qemu+ssh://{}@{}/system'.format(self.login, self.host),
                   '--name {}'.format(name),
                   '--vcpus {}'.format(vcpus),
                   '--ram {}'.format(ram),
                   '--graphics vnc,listen={}'.format(vnc_listen)]  # Have to specify 0.0.0.0 else it will listen on 127.0.0.1 only
        for disk in disks:
            if len(disk) == 2:
                options.append('--disk {},device=disk,bus={}'.format(*disk))
            else:
                options.append('--disk {},device=disk,size={},bus={}'.format(*disk))
        if cdrom_iso is None:
            options.append('--import')
        else:
            options.append('--cdrom {}'.format(cdrom_iso))
        if os_type is not None:
            options.append('--os-type {}'.format(os_type))
        if os_variant is not None:
            options.append('-- os-variant {}'.format(os_variant))
        if networks is None or networks == []:
            options.append('--nonetworks')
        else:
            for network in networks:
                options.append('--network {}'.format(','.join(network)))
        self.ssh_run('{} {}'.format(command, ' '.join(options)))
        if start is False:
            self.ssh_run('virsh destroy {}'.format(name))

    def _get_unique_id(self):
        """
        Gets the unique identifier from the KVM node connected to
        """
        # This needs to use this SSH client, as it need to be executed on the machine the SDK is connected to, and not
        # on the machine running the code
        output = self.ssh_run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g' | sort")
        for mac in output.strip().split('\n'):
            if mac.strip() != '000000000000':
                return mac.strip()

    def ssh_run(self, command):
        """
        Executes an SSH command in a locked context. Since the ssh client is shared in between processes,
        the client should be reconnected before each new call, since another SDK instance running in the same process
        could have connected the client to another node. By adding the connect and run in a locking context,
        it is ensure that within a process the connect and run are executed sequentially.
        """
        if self._ssh_client is None:
            logger.debug('Init SSH client')
            from ovs.plugin.provider.remote import Remote
            self._ssh_client = Remote.cuisine.api
            self._ssh_client.lock = Lock()
        try:
            self._ssh_client.lock.acquire()
            self._ssh_client.connect(self.host)
            return self._ssh_client.run(command)
        except SystemExit as sex:
            # SystemExit kills the worker, WorkerLostError: Worker exited prematurely: exitcode 1.
            # we need to cleanup but also trigger an exception
            raise RuntimeError('Command "{}" returned SystemExit({})'.format(command, sex.message))
        finally:
            self._ssh_client.lock.release()

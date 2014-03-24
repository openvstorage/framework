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
import subprocess
import socket
import shutil
import os
import re
import time

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


class Sdk(object):
    """
    This class contains all SDK related methods
    """

    def __init__(self, host='localhost', login='root'):
        print('Init libvirt')
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
        self._reconnect()

        print 'Init SSH client'
        from ovs.plugin.provider.remote import Remote
        self.ssh_client = Remote.cuisine.api
        self.ssh_client.connect(self.host)

        print 'Init complete'

    def _reconnect(self, attempt=0):
        # Host always comes as ip so we need to ssh-copy-id our own key to ourself
        # @TODO: get local ips
        print('init conn', self.host, self.login, os.getgid(), os.getuid())
        try:
            if self.host == 'localhost':  # Or host in (localips...):
                self._conn = self.libvirt.open('qemu:///system')  # Only local connection
            else:
                self._conn = self.libvirt.open('qemu+ssh://{0}@{1}/system'.format(self.login, self.host))
        except self.libvirt.libvirtError as le:
            print(str(le), le.get_error_code())
            if attempt < 5:
                time.sleep(1)
                self._reconnect(attempt + 1)
            else:
                raise

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
            backingfilename = disk['source']['file']
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

        vm_filename = self.ssh_client.run("grep '<uuid>{0}</uuid>' {1}*.xml".format(vm_object.UUIDString(), ROOT_PATH))
        vm_filename = vm_filename.split(':')[0].strip().split('/')[-1]
        vm_location = sorted(self.ssh_client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0].strip()
        vm_datastore = None
        possible_datastores = self.ssh_client.run("find /mnt -name '{0}/{1}'".format(vm_location, vm_filename)).split('\n')
        for datastore in possible_datastores:
            for mountpoint in mountpoints:
                if mountpoint in datastore.strip():
                    vm_datastore = mountpoint

        config['name'] = vm_object.name()
        config['id'] = str(vm_object.UUIDString())
        config['backing'] = {'filename': '{0}/{1}'.format(vm_location, vm_filename),
                             'datastore': vm_datastore}
        config['datastores'] = dict((mountpoint, '{}:{}'.format(storage_ip, mountpoint)) for mountpoint in mountpoints)

        return config

    def get_power_state(self, vmid):
        """
        return vmachine state
        vmid is the name
        """
        vm = self.get_vm_object(vmid)
        state = vm.info()[0]
        return self.states.get(state, 'UNKNOWN')

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
            print(str(le))
            try:
                self._reconnect()
                return getattr(self._conn, func)(vmid)
            except self.libvirt.libvirtError as le:
                print(str(le))
                raise RuntimeError('Virtual Machine with id/name {} could not be found.'.format(vmid))

    def get_vm_object_by_filename(self, filename):
        """
        get vm based on filename: vmachines/template/template.xml
        """
        vmid = filename.split('/')[-1].replace('.xml', '')
        return self.get_vm_object(vmid)

    def get_vms(self):
        """
        return a list of virDomain objects, representing virtual machines
        """
        return self._conn.listAllDomains()

    def shutdown(self, vmid):
        vm_object = self.get_vm_object(vmid)
        vm_object.shutdown()
        return self.get_power_state(vmid)

    def delete_vm(self, vmid):
        vm_object = self.get_vm_object(vmid)
        vm_object.undefine()
        return True

    def power_on(self, vmid):
        vm_object = self.get_vm_object(vmid)
        vm_object.create()
        return self.get_power_state(vmid)

    def clone_vm(self, vmid, name, disks):
        """
        create a clone vm
        similar to create_vm_from template (?)
        """
        source_vm = self.get_vm_object(vmid)
        # TODO:
        # copy nics
        return self.create_vm_from_template(name, source_vm, disks)

    def create_vm_from_template(self, name, source_vm, disks):
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

        # Get the datastore from the new disks
        # We need the full path of the disk images to be able to create vmachine
        from ovs.dal.hybrids.vdisk import VDisk
        datastore = None
        for disk in disks:
            disk_object = VDisk(disk['diskguid'])
            for vsr in disk_object.vpool.vsrs:
                if vsr.serving_vmachine.name == socket.gethostname():
                    datastore = vsr.mountpoint
        if datastore is None:
            raise RuntimeError('Cannot identify volumedriverfs mountpoint for vmachine {}'.format(name))

        # Get agnostic config of source vm
        if hasattr(source_vm, 'config'):
            vcpus = source_vm.config.hardware.numCPU
            ram = source_vm.config.hardware.memoryMB
        elif isinstance(source_vm, self.libvirt.virDomain):
            vcpus = source_vm.info()[3]
            ram = Sdk._get_ram(source_vm)
        else:
            raise ValueError('Unexpected object type {} {}'.format(source_vm, type(source_vm)))

        # Assume disks are raw
        for disk in disks:
            vm_disks.append(('{}/{}'.format(datastore, disk['backingdevice']), 'virtio'))

        out = self._vm_create(name, vcpus, int(ram), vm_disks)
        print(out)
        if 'ERROR' in out:
            msg = out.replace('ERROR', '').strip().split('\n')[0]
            raise RuntimeError(msg)
        if 'error' in out:
            msg = out.split('error:')[-1].strip()
            raise RuntimeError(msg)

        try:
            return self.get_vm_object(name).UUIDString()
        except self.libvirt.libvirtError as le:
            print(str(le))
            try:
                self._reconnect()
                return self.get_vm_object(name).UUIDString()
            except self.libvirt.libvirtError as le:
                print(str(le))
                raise RuntimeError('Virtual Machine with id/name {} could not be found.'.format(name))

    def _vm_create(self, name, vcpus, ram, disks,
                   cdrom_iso=None, os_type=None, os_variant=None, vnc_listen='0.0.0.0',
                   network=('network=default', 'mac=RANDOM', 'model=e1000')):
        """
        disks = list of tuples [(disk_name, disk_size_GB, bus ENUM(virtio, ide, sata)]
        #e.g [(/vms/vm1.vmdk,10,virtio), ]
        #when using existing storage, size can be ommited
        #e.g [(/vms/vm1.raw,raw,virtio), ]
        #network: (network name: "default", specific mac or RANDOM, nic model as seen inside vmachine: e1000
        """
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
        if network is None:
            options.append('--nonetworks')
        else:
            options.append('--network {}'.format(','.join(network)))
        print(' '.join(options))
        try:
            return subprocess.check_output('{} {}'.format(command, ' '.join(options)),
                                           stderr=subprocess.STDOUT,
                                           shell=True)
        except subprocess.CalledProcessError as cpe:
            return cpe.output.strip()

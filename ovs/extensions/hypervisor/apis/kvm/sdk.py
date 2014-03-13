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

ROOT_PATH = "/etc/libvirt/qemu/" #get static info from here, or use dom.XMLDesc(0)
RUN_PATH = "/var/run/libvirt/qemu/" #get live info from here

#Helpers
def _recurse(treeitem):
    result = {}
    for child in treeitem.getchildren():
        result[child.tag] = _recurse(child)
        for key, item in child.items():
            result[child.tag][key] = item
    return result


class Sdk(object):
    """
    This class contains all SDK related methods
    """

    def __init__(self, host='localhost', login='root', passwd=None):
        print('init libvirt')
        import libvirt  #required
        self.STATES = {libvirt.VIR_DOMAIN_NOSTATE:  'NO STATE',
                       libvirt.VIR_DOMAIN_RUNNING:  'RUNNING',
                       libvirt.VIR_DOMAIN_BLOCKED:  'BLOCKED',
                       libvirt.VIR_DOMAIN_PAUSED:   'PAUSED',
                       libvirt.VIR_DOMAIN_SHUTDOWN: 'SHUTDOWN',
                       libvirt.VIR_DOMAIN_SHUTOFF:  'TURNEDOFF',
                       libvirt.VIR_DOMAIN_CRASHED:  'CRASHED'}

        #password is not used, must set up SSH keys (passwordless login)
        # see ssh-copy-id ...
        self.libvirt = libvirt
        self.host = host
        self.login = login
        self._reconnect()
        print('init complete')

    def _reconnect(self, attempt = 0):
        # host always comes as ip so we need to ssh-copy-id our own key to ourself (!)
        #TODO: get local ips
        print('init conn', self.host, self.login, os.getgid(), os.getuid())
        try:
            if self.host == 'localhost': #or host in (localips...):
                self._conn = self.libvirt.open("qemu:///system") #only local connection
            else:
                self._conn = self.libvirt.open("qemu+ssh://{0}@{1}/system".format(self.login, self.host))
        except self.libvirt.libvirtError as le:
            print(str(le), le.get_error_code())
            if attempt < 5:
                time.sleep(1)
                self._reconnect(attempt + 1)
            else:
                raise

    def _get_disks(self, vm_object):
        tree=ElementTree.fromstring(vm_object.XMLDesc(0))
        return [_recurse(item) for item in tree.findall("devices/disk")]

    def _get_nics(self, vm_object):
        tree=ElementTree.fromstring(vm_object.XMLDesc(0))
        return [_recurse(item) for item in tree.findall("devices/interface")]

    def _get_ram(self, vm_object):
        """
        returns RAM size in MiB
        MUST BE INTEGER! not float
        """
        tree=ElementTree.fromstring(vm_object.XMLDesc(0))
        mem = tree.findall('memory')[0]
        unit = mem.items()[0][1]
        value = mem.text
        if unit == 'MiB':
            return int(value)
        elif unit == 'KiB':
            return int(value) / 1024
        elif unit == 'GiB':
            return int(value) * 1024

    def _get_disk_size(self, filename):
        cmd = ['qemu-img', 'info', filename]
        try:
            out = subprocess.check_output(" ".join(cmd),
                                          stderr = subprocess.STDOUT,
                                          shell = True)
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
                #vmachine is running but no run file?
                return '-1'
        return '-2' #no pid, machine is halted

    def make_agnostic_config(self, vm_object):
        """
        return an agnostic config (no hypervisor specific type or structure)
        """
        storage_ip = '127.0.0.1' #TODO : make sure vsr.storage_ip is set to 127.0.0.1 !ALWAYS
        regex = '/mnt/([^/]+)/(.+$)'
        config = {}
        config['disks'] = []
        mountpoint = 'UNKNOWN'

        order = 0
        for disk in self._get_disks(vm_object):
            backingfilename = disk['source']['file']
            match = re.search(regex, backingfilename)
            mountpoint = os.path.join('/mnt', match.group(1))
            config['disks'].append({'filename': os.path.basename(backingfilename),
                                    'backingfilename': backingfilename.replace(mountpoint, '').strip('/'),
                                    'datastore': mountpoint,
                                    'name': disk.get('alias', {}).get('name', 'UNKNOWN'),
                                    'order': order})
            order += 1

        datastore = "{}:{}".format(storage_ip, mountpoint) #this is "storageIP:/mnt/vpool_X"

        config['name'] =  vm_object.name()
        config['id'] = str(vm_object.UUIDString())
        config['backing'] = {'filename': vm_object.name() + '.xml',
                             'datastore': mountpoint}
        config['datastores'] =  {mountpoint: datastore}

        return config

    def get_power_state(self, vmid):
        """
        return vmachine state
        vmid is the name
        """
        vm = self.get_vm_object(vmid)
        state = vm.info()[0]
        return self.STATES.get(state, 'UNKNOWN')

    def get_vm_object(self, vmid):
        """
        return virDomain object representing virtual machine
        vmid is the name or the uuid
        cannot use ID, since for a stopped vm id is always -1
        """
        func = "lookupByUUIDString"
        try:
            import uuid
            uuid.UUID(vmid)
        except ValueError:
            func = "lookupByName"
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
        #TODO:
        ## copy nics
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

        #get the datastore from the new disks
        #we need the full path of the disk images to be able to create vmachine
        from ovs.dal.hybrids.vdisk import VDisk
        datastore = None
        for disk in disks:
            disk_object = VDisk(disk['diskguid'])
            for vsr in disk_object.vpool.vsrs:
                if vsr.serving_vmachine.name == socket.gethostname():
                    datastore = vsr.mountpoint
        if datastore is None:
            raise RuntimeError('Cannot identify volumedriverfs mountpoint for vmachine {}'.format(vmid))

        #get agnostic config of source vm
        if hasattr(source_vm, 'config'):
            vcpus = source_vm.config.hardware.numCPU
            ram = source_vm.config.hardware.memoryMB
        elif isinstance(source_vm, self.libvirt.virDomain):
            vcpus = source_vm.info()[3]
            ram = self._get_ram(source_vm)
        else:
            raise ValueError('Unexpected object type {} {}'.format(source_vm, type(source_vm)))

        #assume disks are raw
        for disk in disks:
            vm_disks.append(("{}/{}".format(datastore, disk['backingdevice']), 'virtio'))

        out = self._vm_create(name, vcpus, int(ram), vm_disks)
        print(out)
        if 'ERROR' in out:
            msg = out.replace('ERROR', '').strip().split('\n')[0]
            raise RuntimeError(msg)
        if 'error' in out:
            msg = out.split('error:')[-1].strip()
            raise RuntimeError(msg)
        source_xml = '{}{}.xml'.format(ROOT_PATH, name)
        dest_xml = '{}/{}.xml'.format(datastore, name)
        shutil.move(source_xml, dest_xml)
        os.symlink(dest_xml, source_xml)
        try:
            return self.get_vm_object(name).UUIDString()
        except self.libvirt.libvirtError as le:
            print(str(le))
            try:
                self._reconnect()
                return self.get_vm_object(name).UUIDString()
            except self.libvirt.libvirtError as le:
                print(str(le))
                raise RuntimeError('Virtual Machine with id/name {} could not be found.'.format(vmid))

    def _vm_create(self, name, vcpus, ram, disks,
                   cdrom_iso=None, os_type=None, os_variant=None, vnc_listen='0.0.0.0',
                   network = ('network=default', 'mac=RANDOM', 'model=e1000')):
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
                   '--graphics vnc,listen={}'.format(vnc_listen)] #have to specify 0.0.0.0 else it will listen on 127.0.0.1 only
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
            return subprocess.check_output("{} {}".format(command, " ".join(options)),
                                           stderr = subprocess.STDOUT,
                                           shell = True)
        except subprocess.CalledProcessError as cpe:
            return cpe.output.strip()


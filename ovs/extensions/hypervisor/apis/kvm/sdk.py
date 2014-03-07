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

import libvirt  #required
from xml.etree import ElementTree
import subprocess

ROOT_PATH = "/etc/libvirt/qemu/" #get static info from here, or use dom.XMLDesc(0)
RUN_PATH = "/var/run/libvirt/qemu/" #get live info from here

STATES = {libvirt.VIR_DOMAIN_NOSTATE:  'NO STATE',
          libvirt.VIR_DOMAIN_RUNNING:  'RUNNING',
          libvirt.VIR_DOMAIN_BLOCKED:  'BLOCKED',
          libvirt.VIR_DOMAIN_PAUSED:   'PAUSED',
          libvirt.VIR_DOMAIN_SHUTDOWN: 'SHUTDOWN',
          libvirt.VIR_DOMAIN_SHUTOFF:  'TURNEDOFF',
          libvirt.VIR_DOMAIN_CRASHED:  'CRASHED'}

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
        self._conn = libvirt.open("qemu:///system") #only local connection
        #remote:
        #self._conn = libvirt.open("qemu+ssh://{0}@{1}/system".format(login, host))
        #password is not used, must set up SSH keys (passwordless login)
        # see ssh-copy-id ...

    def _get_disks(self, vm_object):
        tree=ElementTree.fromstring(vm_object.XMLDesc(0))
        return [_recurse(item) for item in tree.findall("devices/disk")]

    def _get_nics(self, vm_object):
        tree=ElementTree.fromstring(vm_object.XMLDesc(0))
        return [_recurse(item) for item in tree.findall("devices/interface")]

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

    def make_agnostic_config(self, vm_object):
        config = {'name': vm_object.name(),
                  'id': vm_object.ID(),
                  'backing': {'filename': vm_object.name() + '.xml',
                              'datastore': ROOT_PATH},
                  'disks': [],
                  'datastores': {ROOT_PATH: "localhost:{}".format(ROOT_PATH)}}
        order = 0
        for disk in self._get_disks(vm_object):
            config['disks'].append({'filename': disk['source']['file'],
                                    'backingfilename': disk['source']['file'],
                                    'datastore': ROOT_PATH,
                                    'name': disk.get('alias', {}).get('name', 'UNKNOWN'),
                                    'order': order})
            order += 1
        return config

    def get_power_state(self, vmid):
        """
        return vmachine state
        vmid is the name
        """
        vm = self.get_vm_object(vmid)
        state = vm.info()[0]
        return STATES.get(state, 'UNKNOWN')

    def get_vm_object(self, vmid):
        """
        return virDomain object representing virtual machine
        """
        try:
            return self._conn.lookupByName(str(vmid))
        except libvirt.libvirtError:
            raise RuntimeError('Virtual Machine with id {} could not be found.'.format(vmid))

    def get_vms(self):
        """
        return a list of virDomain objects, representing virtual machines
        """
        return self._conn.listAllDomains()

    def _get_vm_pid(self, vmid):
        """
        return pid of kvm process running this machine (if any)
        """
        vm_object = self.get_vm_object(vmid)
        if self.get_power_state(vmid) == 'RUNNING':
            xml_path = '{}/{}.xml'.format(RUN_PATH, vm_object.name())
            try:
                with open(xml_path, 'r') as xml_file:
                    xml_tree = ElementTree.fromstring(xml_file.read())
                items = dict(xml_tree.items())
                return items.get('pid', '-0') #file found but no pid
            except IOError:
                #vmachine is running but no run file?
                return '-1'
        return '-2' #no pid, machine is halted

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

    def create_vm_from_template(name, source_vm, disks, ip, mountpoint):
        """
        Create a vm based on an existing template on specified hypervisor
        #source_vm might be an esx machine object or a libvirt.VirDomain object
         @TODO: make sure source_vm is agnostic object (dict)
        #disks: list of dicts (agnostic)
         {'diskguid': new_disk.guid, 'name': new_disk.name, 'backingdevice': device_location.strip('/')}
        #ip, mountpoint = are used to locate the datastore
         kvm doesn't have datastores, all files are in /mnt/vpool_x/name/
         @TODO: we need to receive /mnt/vpool_x (?) or we can take it from disks ['backingdevice']
        """
        vm_disks = []
        #get agnostic config of source vm

        #assume disks are raw
        for disk in disks:
            vm_disks.append((disk['backingdevice']))

    def _vm_create(self, name, vcpus, ram, disks, cdrom_iso=None, os_type=None, os_variant=None, vnc_listen='0.0.0.0'):
        """
        disks = list of tuples [(disk_name, disk_size_GB, disk_format ENUM(raw, qcow2, vmdk), bus ENUM(virtio, ide, sata)]
        #e.g [(/vms/vm1.vmdk,10,vmdk,virtio), ]
        """
        command = 'virt-install'
        options = ['--connect qemu:///system', #only local connections
                   '--name {}'.format(name),
                   '--vcpus {}'.format(vcpus),
                   '--ram {}'.format(ram),
                   '--graphics vnc,listen={}'.format(vnc_listen)] #have to specify 0.0.0.0 else it will listen on 127.0.0.1 only
        for disk in disks:
            options.append('--disk {},device=disk,size={},format={},bus={}'.format(*disk))
        if cdrom_iso is None:
            options.append('--import')
        else:
            options.append('--cdrom {}'.format(cdrom_iso))
        if os_type is not None:
            options.append('--os-type {}'.format(os_type))
        if os_variant is not None:
            options.append('-- os-variant {}'.format(os_variant))
        print(' '.join(options))
        try:
            return subprocess.check_output("{} {}".format(command, " ".join(options)),
                                           stderr = subprocess.STDOUT,
                                           shell = True)
        except subprocess.CalledProcessError as cpe:
            return cpe.output.strip()


#!/bin/python
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
Prerequisites:
* ESXi pre-installed
* At least 1 datastore

This script will automatically create disk RDM's and an OVS vm config
and boot the OVSVSA from that iso to install.
"""

import subprocess as sp
import os
import re
import shlex


class InstallHelper():
    """
    This class contains various helper methods for the installation procedure
    """

    def __init__(self):
        """
        Nothing here
        """
        pass

    @staticmethod
    def ask_integer(question, min_value, max_value, default_value=None, invalid_message=None):
        """
        Asks an integer to the user
        """
        if invalid_message is None:
            invalid_message = 'Invalid input please try again.'
        if default_value is not None:
            question = '{0} [{1}]: '.format(question, default_value)
        while True:
            i = raw_input(question).rstrip()
            if i == '' and default_value is not None:
                i = str(default_value)
            if not i.isdigit():
                print invalid_message
            else:
                i = int(i)
                if min_value <= i <= max_value:
                    return i
                else:
                    print invalid_message

    @staticmethod
    def ask_choice(choice_options, default_value=None, columns=[]):
        """
        Lets the user chose one of a set of options
        """
        if not choice_options:
            return None
        if len(choice_options) == 1:
            print "Found exactly one choice: {0}".format(choice_options[0])
            return choice_options[0]
        choice_options.sort()
        print 'Make a selection please: '
        nr = 0
        default_nr = None
        for section in choice_options:
            nr += 1
            output = ''
            for column in columns:
                output = output + ' {0:<18}'.format(section[column])
            if not output:
                print '   {0}: {1}'.format(nr, section)
            else:
                print '   {0}: {1}'.format(nr, output)
            if section == default_value:
                default_nr = nr

        result = InstallHelper.ask_integer(
            question='   Select Nr: ',
            min_value=1,
            max_value=len(choice_options),
            default_value=default_nr
        )
        return choice_options[result - 1]

    @staticmethod
    def convert_keyvalue(output, datatype='list'):
        """
        Converts a keyvalue string
        """
        typecasting = {'string'  : lambda x: str(x),
                       'integer' : lambda x: int(x),
                       'boolean' : lambda x: eval(x.capitalize()),
                       'string[]': lambda x: list()}
        if datatype == 'list':
            strucregex = 'structure\[(?P<order>\d+)\].*'
            detailregex = 'structure\[%s\]\.(?P<object>\w+)\.(?P<property>\w+)\.?(?P<subtype>.*)\.(?P<dtype>(?:string|integer|boolean)).*=(?P<value>.+)'
            # Find total number of structures
            nrofstructures = set(re.findall(strucregex, output, re.M))
            result = list()
            for nr in list(nrofstructures):
                properties = dict()
                # Find specific structure
                strucs = re.findall(detailregex % nr, output, re.M)
                for obj, prop, subtype, dtype, val in strucs:
                    # Cast to specific object type
                    val = typecasting[dtype.strip()](val.strip())
                    # Check for subtype (multi values = string[])
                    if subtype:
                        if not prop in properties:
                            properties[prop] = [val]
                        elif not val in properties[prop]:
                            properties[prop].append(val)
                    # Single value is present
                    else:
                        properties[prop] = val
                result.append(properties)
            return result
        elif datatype == 'get':
            detailregex = '(?P<object>\w+)\.(?P<property>\w+)\.(?P<dtype>.+)=(?P<value>.+)'
            result = dict()
            for obj, prop, dtype, val in re.findall(detailregex, output):
                result[prop] = typecasting[dtype.strip()](val.strip())
            return result
        else:
            raise NotImplementedError('Unknown type: {0}'.format(datatype))

    @staticmethod
    def ask_yesno(message="", default_value=None):
        """
        Asks the user a yes/no question
        """
        if default_value is None:
            ynstring = " (y/n): "
            failuremsg = "Illegal value. Press 'y' or 'n'."
        elif default_value is True:
            ynstring = " ([y]/n): "
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        elif default_value is False:
            ynstring = " (y/[n]): "
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        else:
            raise ValueError("Invalid default value {0}".format(default_value))
        while True:
            result = raw_input(str(message) + ynstring).rstrip(chr(13))
            if not result and default_value is not None:
                return default_value
            if result.lower() in ('y', 'yes'):
                return True
            if result.lower() in ('n', 'no'):
                return False
            print failuremsg

    @staticmethod
    def execute_command(cmd, catch_output=True):
        """
        Executes a command
        """
        if catch_output:
            process = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
            output = process.stdout.readlines()
            process.wait()
            return process.returncode, output
        else:
            process = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            output = process.communicate()
            return process.returncode, output

    @staticmethod
    def boxed_message(lines, character='+', maxlength=60):
        """
        Embeds a set of lines into a box
        """
        character = str(character)  # This must be a string
        corrected_lines = []
        for line in lines:
            if len(line) > maxlength:
                linepart = ''
                for word in line.split(' '):
                    if len(linepart + ' ' + word) <= maxlength:
                        linepart += word + ' '
                    elif len(word) >= maxlength:
                        if len(linepart) > 0:
                            corrected_lines.append(linepart.strip())
                            linepart = ''
                        corrected_lines.append(word.strip())
                    else:
                        if len(linepart) > 0:
                            corrected_lines.append(linepart.strip())
                        linepart = word + ' '
                if len(linepart) > 0:
                    corrected_lines.append(linepart.strip())
            else:
                corrected_lines.append(line)
        maxlen = len(max(corrected_lines, key=len))
        newlines = [character * (maxlen + 10)]
        for line in corrected_lines:
            newlines.append('{0}  {1}{2}  {3}'.format(character * 3, line, ' ' * (maxlen - len(line)), character * 3))
        newlines.append(character * (maxlen + 10))
        return '\n'.join(newlines)


class VMwareSystem():
    """
    This class provides VMware specific methods
    """
    _verbose = False

    def __init__(self):
        """
        Nothing in here
        """
        pass

    def get_rdms_in_use(self):
        """
        Retrieve raw device mapping currently in use
        """

        KEY = 'Maps to: '
        vmdk_to_rdm_map = {}
        found_datastores = {}
        cmd = """find /vmfs/volumes/ -type f -name '*.vmdk' -size -1024k -exec grep -l '^createType=.*RawDeviceMap' {} \;"""
        _, rdms_in_use = InstallHelper.execute_command(shlex.split(cmd),
                                                       catch_output=False)
        for rdm in rdms_in_use[0].split():
            _, mapping = InstallHelper.execute_command(['vmkfstools', '-q', rdm])
            for line in mapping:
                if KEY in line:
                    vml_link = line.replace(KEY, '')
                    _, disk = InstallHelper.execute_command(['ls', '-alh', '/vmfs/devices/disks/' + vml_link.strip()])
                    vmdk_to_rdm_map[rdm] = disk[0].split(' -> ')[1].strip()

        return vmdk_to_rdm_map

    def get_vmfs_devices(self):
        """
        Retrieve disk containing datastore
        """
        found_vmfsdevices = []
        found_datastores = {}
        _, vmfs = InstallHelper.execute_command(['esxcli', '--formatter=keyvalue', 'storage', 'vmfs', 'extent', 'list'])
        convertedvmfs = InstallHelper.convert_keyvalue(''.join(vmfs))
        for fs in convertedvmfs:
            if self._verbose:
                print 'Datastore:{0} UUID:{1} Device:{2}'.format(fs['VolumeName'], fs['VMFSUUID'], fs['DeviceName'])
            found_vmfsdevices.append(fs['DeviceName'])
            found_datastores[fs['VolumeName']] = fs['VMFSUUID']

        return found_datastores, found_vmfsdevices

    def get_boot_device(self):
        """
        Determine boot disk
        """
        bootdevice = None
        bootbank = os.readlink('/bootbank')
        _, bootdeviceout = InstallHelper.execute_command(['vmkfstools', '-P', bootbank])
        for line in bootdeviceout:
            if line.startswith('Partitions spanned'):
                bootdevice = bootdeviceout[bootdeviceout.index(line) + 1].strip()
        if bootdevice and self._verbose:
            print "Bootdisk: {0}".format(bootdevice.split(':')[0])
        return bootdevice

    def list_disks(self):
        """
        Retrieve scsi disks
        """
        devicestoexclude = list(vmfsdevices)
        devicestoexclude.append(self.get_boot_device())
        freedevices = []
        _, scsiluns = InstallHelper.execute_command(['esxcli', '--formatter=keyvalue', 'storage', 'core', 'device', 'list'])
        convertedscsiluns = InstallHelper.convert_keyvalue(''.join(scsiluns))
        for lun in convertedscsiluns:
            if not lun['Device'] in devicestoexclude and lun['Size'] != 0:
                if self._verbose:
                    print 'Model:{0} Size:{1} SSD:{2} Device:{3}'.format(lun['Model'], lun['Size'], lun['IsSSD'], lun['Device'])
                freedevices.append(lun)
        return freedevices

    def enable_vmware_ssd_option(self):
        """
        Enable the Vmware SSD option for all detected disks with a size smaller then 500Gb
        """
        freedevices = self.list_disks()
        for device in freedevices:
            if not device['IsSSD'] and device['Size'] < 500000:
                print 'Enabling SSD option for {0}'.format(device['Device'])
                os.system('/sbin/esxcli storage nmp satp rule add --satp VMW_SATP_LOCAL --device {0} --option "enable_ssd"'.format(device['Device']))
                os.system('/sbin/esxcli storage core claiming unclaim --type device --device {0}'.format(device['Device']))
                os.system('/sbin/esxcli storage core claimrule run -d {0}'.format(device['Device']))
                os.system('/sbin/esxcli storage core claiming reclaim -d {0}'.format(device['Device']))
                os.system('/sbin/esxcli storage core claimrule run -d {0}'.format(device['Device']))

    def list_switches(self, uplink=None):
        """
        List virtual switches
        """
        _, vswitchesout = InstallHelper.execute_command(['esxcli', '--formatter=keyvalue', 'network', 'vswitch', 'standard', 'list'])
        found_vswitches = InstallHelper.convert_keyvalue(''.join(vswitchesout))
        if self._verbose:
            for switch in found_vswitches:
                print 'Name:{0} Uplinks:{1} Portgroups:{2}'.format(switch['Name'], switch['Uplinks'], switch['Portgroups'])
            print '\n'
        return [vs for vs in found_vswitches if (uplink and uplink in vs['Uplinks']) or (not uplink)]

    @staticmethod
    def list_vm_portgroups():
        """
        List the current available portgroups
        """
        portgroupsresult = {}
        _, portgroupsout = InstallHelper.execute_command(['esxcli', '--formatter=keyvalue', 'network', 'vswitch', 'standard', 'portgroup', 'list'])
        portgroups = InstallHelper.convert_keyvalue(''.join(portgroupsout))
        for portgroup in portgroups:
            portgroupsresult[portgroup['Name']] = portgroup['VLANID']
        return portgroupsresult

    @staticmethod
    def list_vmkernel_ports():
        """
        List the current vmkernel ports
        """
        vmkernelports_result = {}
        _, interfaceout = InstallHelper.execute_command(['esxcli', '--formatter=keyvalue', 'network', 'ip', 'interface', 'list'])
        vmkernel_interfaces = InstallHelper.convert_keyvalue(''.join(interfaceout))
        return vmkernel_interfaces

    @staticmethod
    def build_nic_config(nics):
        """
        Build the nic vmx content for the required nics and assign them with semi-random MAC addresses
        """
        # We first take VMware's 00:0c:29 and set the U/L bit to 1, stating it's a locally administered MAC
        # Then we add the NIC specific part of one of the local NICs to the mac, creating an address that
        # will most likely be unique. However, this means that every VSA generated on a certain node will have
        # the same MAC address. This is not a supported use-case so should introduce no problems.

        vmx_base = """
                   ethernet%(nicseq)s.present = "true"
                   ethernet%(nicseq)s.virtualDev = "vmxnet3"
                   ethernet%(nicseq)s.networkName = "%(portgroup)s"
                   ethernet%(nicseq)s.connectionType = "bridged"
                   """

        nic_vmx = ''
        for portgroup in nics:
            sequence = nics.index(portgroup)

            nic_vmx += vmx_base % {'nicseq'   : sequence,
                                   'portgroup': portgroup}
        return nic_vmx

    def create_vm_config(self, name, cpu_value, memory_value, guestos_value, osbits_value, nic_vmx, disk_vmx, iso_vmx):
        vmpath = '/vmfs/volumes/{0}/{1}'.format(datastore, name)
        vmconfigfile = '{0}/{1}.vmx'.format(vmpath, name)
        if not (os.path.exists(vmpath) or os.path.islink(vmpath)):
            os.mkdir(vmpath)
        template_vmx = """.encoding = "UTF-8"
config.version = "8"
virtualHW.version = "8"
displayName = "%(name)s"
guestOS = "%(guestos)s"
memsize = "%(memory)s"
numvcpus = "%(cpus)s"
cpuid.coresPerSocket = "%(cpus)s"
extendedConfigFile = "%(name)s.vmxf"
virtualHW.productCompatibility = "hosted"
scsi0.present = "TRUE"
scsi0.virtualDev = "lsisas1068"
%(dvdrom)s
%(disks)s
%(nics)s
svga.vramSize = "4194304"
pciBridge0.present = "TRUE"
pciBridge4.present = "TRUE"
pciBridge4.virtualDev = "pcieRootPort"
pciBridge4.functions = "8"
pciBridge5.present = "TRUE"
pciBridge5.virtualDev = "pcieRootPort"
pciBridge5.functions = "8"
pciBridge6.present = "TRUE"
pciBridge6.virtualDev = "pcieRootPort"
pciBridge6.functions = "8"
pciBridge7.present = "TRUE"
pciBridge7.virtualDev = "pcieRootPort"
pciBridge7.functions = "8"
bios.bootDelay = "1000"
bios.forceSetupOnce = "TRUE"
tools.syncTime = "FALSE"
time.synchronize.continue = 0
time.synchronize.restore = 0
time.synchronize.resume.disk = 0
time.synchronize.shrink = 0
time.synchronize.tools.startup = 0
time.synchronize.tools.enable = 0
time.synchronize.resume.host = 0
sched.cpu.min = "500"
sched.cpu.units = "mhz"
sched.cpu.shares = "high"
sched.mem.min = "%(memory)s"
sched.mem.shares = "normal"
usb_xhci.present = "TRUE"
""" % {'name'   : name,
       'cpus'   : cpu_value,
       'memory' : memory_value,
       'guestos': guestos_value if osbits_value == 32 else '%s-64' % guestos_value,
       'nics'   : nic_vmx,
       'disks'  : disk_vmx,
       'dvdrom' : iso_vmx}
        vmconfig = open(vmconfigfile, "wb")
        vmconfig.write(template_vmx)
        vmconfig.close()
        return vmconfigfile

    def create_vdisk(self, vm, seq, size, dconfig):
        """
        Creates a virtual disk (vmdk)
        @param vm: name of the vm to incorporate into the name
        @param seq: the sequence of the disk in the given config
        @param size: the size of the disk, given in a <number><k|K|m|M|g|G> format
        @param dconfig: the configuration to which the config has to be appended
        @return: the diskConfig
        """
        vmpath = '/vmfs/volumes/{0}/{1}'.format(datastore, vm)
        if not (os.path.exists(vmpath) or os.path.islink(vmpath)):
            os.mkdir(vmpath)
        vmdkpath = '{0}/vhd{1}.vmdk'.format(vmpath, seq)

        dconfig += """scsi0:%(target)s.present = "TRUE"
        scsi0:%(target)s.fileName = "%(disksource)s"
        scsi0:%(target)s.deviceType = "scsi-hardDisk"
        """ % {'disksource': vmdkpath,
               'target'    : seq if seq <= 6 else seq + 1}

        if os.path.exists(vmdkpath):
            print 'Existing vmdk {0} detected - please remove vm on esx level first!'.format(vmdkpath)
            sys.exit(1)

        if self._verbose:
            print 'Creating vmdk'

        returncode, output = InstallHelper.execute_command(['vmkfstools', '-c', size, '-d', 'zeroedthick', vmdkpath])
        if returncode != 0:
            print InstallHelper.boxed_message(['Error occurred during creation of vhd:'] + output)
            sys.exit(1)

        return dconfig

    def create_vdisk_mapping(self, vm, seq, dsk, dconfig):
        """
        Create a raw device mapping for disk and assign to VSA
        """
        self._verbose = False
        vmpath = '/vmfs/volumes/{0}/{1}'.format(datastore, vm)
        if not (os.path.exists(vmpath) or os.path.islink(vmpath)):
            os.mkdir(vmpath)
        vmdkpath = '{0}/{1}{2}.vmdk'.format(vmpath, 'ssd' if dsk['IsSSD'] else 'hdd', seq)
        if self._verbose:
            print 'Setting up RDM {0} for /vmfs/devices/disks/{1}'.format(vmdkpath, dsk['Device'])
        InstallHelper.execute_command(['vmkfstools', '-z', '/vmfs/devices/disks/{0}'.format(dsk['Device']), vmdkpath])
        dconfig += """scsi0:%(target)s.present = "TRUE"
scsi0:%(target)s.fileName = "%(disksource)s"
scsi0:%(target)s.deviceType = "scsi-hardDisk"
""" % {'disksource': vmdkpath,
       'target'    : seq if seq <= 6 else seq + 1}
        self._verbose = False
        return dconfig

if __name__ == '__main__':
    import sys
    from optparse import OptionParser

    vm_sys = VMwareSystem()

    vm_basename = 'ovsvsa'
    vm_name = vm_basename

    # ISO selection
    parser = OptionParser(description='Open vStorage VSA Setup')
    parser.add_option('-i', '--image', dest='image', help='absolute path to your ubuntu iso')
    parser.add_option('-s', '--skip', action="store_true", dest='skip', help='skip the sata as 3rd disk')
    (options, args) = parser.parse_args()
    if not options.image:
        print 'No ISO image was specified, you\'ll need to attach it to the VM yourself'
        proceed = InstallHelper.ask_yesno('Continue with the install?', True)
        if not proceed:
            sys.exit(1)
    if vm_name == 'ovsvsa':
        vm_name += '001'
    imagefile = options.image
    if imagefile and not os.path.isfile(imagefile):
        print InstallHelper.boxed_message(['Unable to find ISO', imagefile])
        sys.exit(1)


    # Warning
    print InstallHelper.boxed_message(['WARNING. Use with caution.',
                                       'This script assumes it is executed on an ESXi hypervisor',
                                       'dedicated to Open vStorage. It will create raw device',
                                       'mappings and configure virtual machines without further',
                                       'interaction. If you want to install Open vStorage on an',
                                       'existing server, please refer to the Open vStorage',
                                       'documentation on how to do so.'])
    proceed = InstallHelper.ask_yesno('Continue with the install?', True)
    if not proceed:
        sys.exit(1)

    # Datastores
    datastores, vmfsdevices = vm_sys.get_vmfs_devices()
    if len(datastores) == 0:
        print InstallHelper.boxed_message(['No datastores using local disks were found'])
        sys.exit(1)
    elif len(datastores) == 1:
        datastore_key = datastores.keys()[0]
    else:
        print 'Please select the datastore, you need at least 70GB free space:'
        keys = datastores.keys()
        keys.sort()
        datastore_key = InstallHelper.ask_choice(keys, default_value=keys[0])
    datastore = datastores[datastore_key]
    print 'Using datastore \'{0}\''.format(datastore_key)

    # Networking
    print 'Determine ESX host networking to use'
    vswitches = vm_sys.list_switches()
    if not vswitches:
        print InstallHelper.boxed_message(['No portgroups found in your ESXi network configuration'])
        sys.exit(1)
    all_pgs = VMwareSystem.list_vm_portgroups()
    kernel_pgs = map(lambda p: p['Portgroup'], VMwareSystem.list_vmkernel_ports())
    vm_pg_names = filter(lambda p: not p in kernel_pgs, all_pgs.keys())
    if len(vm_pg_names) < 2:
        print InstallHelper.boxed_message(['There should be at least two portgroups configured'])
        sys.exit(1)
    print 'Please select your public network:'
    public_pg = InstallHelper.ask_choice(vm_pg_names)
    print 'Please select your storage network:'
    private_pg = InstallHelper.ask_choice(vm_pg_names)

    nic_config = VMwareSystem.build_nic_config([private_pg, public_pg])

    print '------'
    rdms_in_use = vm_sys.get_rdms_in_use()
    if rdms_in_use:
        print 'RDM mappings currently in use:'
        for disk, rdm in vm_sys.get_rdms_in_use().iteritems():
            print 'vmdk: {0}, rdm: {1}'.format(disk, rdm)
    else:
        print 'No RDM mapping are currently in use'
    print '------'

    # RDM
    print 'Creating Raw Device Mappings'
    ssds, hdds = [], []
    vm_sys.enable_vmware_ssd_option()
    all_disks = vm_sys.list_disks()
    for disk in all_disks:
        if disk['IsSSD']:
            ssds.append(disk)
        else:
            hdds.append(disk)
    if not ssds:
        print InstallHelper.boxed_message(['Not enough SSD devices available to continue the install. Min: 1'])
        sys.exit(1)
    size = InstallHelper.ask_integer('Specify the size in GB (min: 50)', 50, 9999, default_value=100)

    disk_config = ''
    disk_config = vm_sys.create_vdisk(vm_name, 0, '{0}G'.format(size), disk_config)

    ssd = InstallHelper.ask_choice(ssds, columns=['Vendor', 'Model', 'DevfsPath'])
    disk_config = vm_sys.create_vdisk_mapping(vm_name, 1, ssd, disk_config)

    if len(hdds) > 0 and not options.skip:
        hdd = InstallHelper.ask_choice(hdds, columns=['Vendor', 'Model', 'DevfsPath'])
        disk_config = vm_sys.create_vdisk_mapping(vm_name, 2, hdd, disk_config)

    # Add CD drive
    if imagefile:
        cd_config = """ide1:0.present = "TRUE"
ide1:0.fileName = "{0}"
ide1:0.deviceType = "cdrom-image"
ide1:0.clientDevice = "FALSE"
ide1:0.startConnected = "TRUE"
""".format(imagefile)
    else:
        cd_config = """ide1:0.present = "TRUE"
ide1:0.fileName = ""
ide1:0.deviceType = "atapi-cdrom"
ide1:0.clientDevice = "TRUE"
ide1:0.startConnected = "FALSE"
"""

    # Creating config
    print 'Creating Open vStorage virtual machine config'
    cpu = 4
    memory = 16 * 1024
    guestos = 'ubuntu'
    osbits = 64
    vm_config = vm_sys.create_vm_config(vm_name, cpu, memory, guestos, osbits, nic_config, disk_config, cd_config)

    # Configure states
    print 'Configuring PStates and CStates'
    InstallHelper.execute_command(['esxcli', 'system', 'settings', 'advanced', 'set', '-o', '/Power/UseCStates', '--int-value=1'])
    InstallHelper.execute_command(['esxcli', 'system', 'settings', 'advanced', 'set', '-o', '/Power/UsePStates', '--int-value=0'])

    # Register VSA
    print 'Register VSA'
    _, out = InstallHelper.execute_command(['vim-cmd', 'solo/registervm', vm_config], True)
    vm_id = out[0].strip()

    print 'Starting VSA'
    InstallHelper.execute_command(['vim-cmd', 'hostsvc/autostartmanager/enable_autostart', '1'])
    InstallHelper.execute_command(['vim-cmd', 'hostsvc/autostartmanager/update_autostartentry',
                              '{0}'.format(vm_id), 'PowerOn', '5', '1', 'guestShutdown', '5',
                              'systemDefault'])
    InstallHelper.execute_command(['vim-cmd', 'vmsvc/power.on', vm_id])

    print InstallHelper.boxed_message(['The Open vStorage VM (%s) is powered on' % vm_name,
                                       '* Open the vSphere Client',
                                       '* Connect to the %s Virtual Machine\'s console' % vm_name,
                                       '* Make sure the VM boots from CD',
                                       '* Install the Operating System'])

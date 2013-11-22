#!/bin/python

"""
Prerequisites:
* ESXi 5.1 pre-installed
* At least 1 datastore
* vSwitch with "CloudFramesStorage" portgroup and uplink to vmnic
* vSwitch with "CloudFramesPublic" portgroup and uplink to connected vmnic
* vmkernel ports on both vSwitches

This script will automatically create disk RDM's and a CFVSA vm config
and boot the CFVSA from that iso to install.
"""

import subprocess as sp
import os, re

class InstallHelper():
    def askString(self, question, defaultparam='', regex=None, retry=-1, validate=None):
        if validate and not callable(validate):
            raise TypeError('The validate argument should be a callable')
        response = ""
        if not defaultparam == "" and defaultparam:
            question += " [%s]"%defaultparam
        question += ": "
        retryCount = retry
        while retryCount != 0:
            response = raw_input(question).rstrip()
            if response == "" and not defaultparam == "" and defaultparam:
                return defaultparam
            if (not regex or re.match(regex,response)) and (not validate or validate(response)):
                return response
            else:
                print "Please insert a valid value!"
                retryCount = retryCount - 1
        raise ValueError("Console.askString() failed: tried %d times but user didn't fill out a value that matches '%s'." % (retry, regex))

    def askIntegers(self, question, invalid_message="invalid input please try again.", min=None, max=None):
        def f():
            s = raw_input(question).rstrip()
            return s.split(",")
        def clean(l):
            try:
                return [int(i.strip()) for i in l if i.strip() != ""]
            except ValueError:
                return list()
        def all_between(l, min, max):
            for i in l:
                if (not min is None) and i < min:
                    return False
                elif (not max is None) and i > max:
                    return False
            return True
        def invalid(l):
            return len(l) == 0 or (not all_between(l, min, max))

        parts = clean(f())
        while invalid(parts):
            print invalid_message
            parts = clean(f())
        return parts

    def askChoiceMultiple(self, choicearray, descr=None, sort=None):
        if not choicearray:
            return []
        if len(choicearray) == 1:
            print "Found exactly one choice: %s"%(choicearray[0])
            return choicearray
        descr = descr or "\nMake a selection please: "
        if sort:
            choicearray.sort()
        print descr
        nr=0
        for section in choicearray:
            nr=nr+1
            print "   %s: %s" % (nr, section)
        print ""
        results = self.askIntegers("   Select Nr, use comma separation if more e.g. \"1,4\", 0 is all",
                                   "Invalid choice, please try again",
                                   min=0,
                                   max=len(choicearray))
        if results==[0]:
            return choicearray
        else:
            return [choicearray[i-1] for i in results]

    def convertKeyValue(self, output, type='list'):
        typeCasting = {'string'  : lambda x: str(x),
                       'integer' : lambda x: int(x),
                       'boolean' : lambda x: eval(x.capitalize()),
                       'string[]': lambda x: list()}
        if type == 'list':
            strucRegex = 'structure\[(?P<order>\d+)\].*'
            detailRegex = 'structure\[%s\]\.(?P<object>\w+)\.(?P<property>\w+)\.?(?P<subtype>.*)\.(?P<type>(?:string|integer|boolean)).*=(?P<value>.+)'
            #Find total number of structures
            nrOfStructures = set(re.findall(strucRegex, output, re.M))
            result = list()
            for nr in list(nrOfStructures):
                properties = dict()
                #Find specific structure
                strucs = re.findall(detailRegex%nr, output, re.M)
                for obj, prop, subtype, type, val in strucs:
                    #Cast to specific object type
                    val = typeCasting[type.strip()](val.strip())
                    #Check for subtype (multi values = string[])
                    if subtype:
                        if not prop in properties:
                            properties[prop] = [val]
                        elif not val in properties[prop]:
                            properties[prop].append(val)
                    #Single value is present
                    else:
                        properties[prop] = val
                result.append(properties)
            return result
        elif type == 'get':
            detailRegex = '(?P<object>\w+)\.(?P<property>\w+)\.(?P<type>.+)=(?P<value>.+)'
            result = dict()
            for obj, prop, type, val in re.findall(detailRegex, output):
                result[prop] = typeCasting[type.strip()](val.strip())
            return result
        else:
            raise NotImplementedError('Unknown type: %s'%type)

    def executeCommand(self, cmd, catchOutput=True):
        if catchOutput:
            process = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
            out = process.stdout.readlines()
            process.poll()
            return process.returncode, out
        else:
            sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT)

class vmwareSystem():
    _helper = InstallHelper()
    _verbose = False
    def getVMFSDevices(self):
        """
        Retrieve disk containing datastore
        """
        vmfsDevices = []
        exit, vmfs = self._helper.executeCommand(['esxcli', '--formatter=keyvalue', 'storage', 'vmfs', 'extent', 'list'])
        convertedVmfs = self._helper.convertKeyValue(''.join(vmfs))
        #print convertedVmfs
        for fs in convertedVmfs:
            if self._verbose:
                print 'Datastore:%s UUID:%s Device:%s'%(fs['VolumeName'], fs['VMFSUUID'], fs['DeviceName'])
            vmfsDevices.append(fs['DeviceName'])
            datastore = fs['VMFSUUID']
        return datastore, vmfsDevices

    def getBootDevice(self):
        """
        Determine boot disk
        """
        bootDevice = None
        bootBank = os.readlink('/bootbank')
        exit, bootDeviceOut = self._helper.executeCommand(['vmkfstools', '-P', bootBank])
        for line in bootDeviceOut:
            if line.startswith('Partitions spanned'):
                bootDevice = bootDeviceOut[bootDeviceOut.index(line)+1].strip()
        if bootDevice and self._verbose: print "Bootdisk:%s"%bootDevice.split(':')[0]
        return bootDevice

    def listvms(self):
        """
        Retrieve current virtual machine list
        """
        exit, allvms = self._helper.executeCommand(['vim-cmd', 'vmsvc/getallvms'], True)
        return allvms

    def listDisks(self):
        """
        Retrieve scsi disks
        """
        datastore, vmfsDevices = self.getVMFSDevices()
        devicesToExclude = list(vmfsDevices)
        devicesToExclude.append(self.getBootDevice())
        freeDevices = []
        exit, scsiluns = self._helper.executeCommand(['esxcli', '--formatter=keyvalue', 'storage', 'core', 'device', 'list'])
        convertedScsiLuns = self._helper.convertKeyValue(''.join(scsiluns))
        for lun in convertedScsiLuns:
            if not lun['Device'] in devicesToExclude and lun['Size'] != 0:
                if self._verbose:
                    print 'Model:%s Size:%s SSD:%s Device:%s'%(lun['Model'], lun['Size'], lun['IsSSD'], lun['Device'])
                freeDevices.append(lun)
        return freeDevices

    def enableVmwareSSDOption(self):
        """
        Enable the Vmware SSD option for all detected disks with a size smaller then 500Gb
        """
        freeDevices = self.listDisks()
        for disk in freeDevices:
            if disk['IsSSD'] == False and disk['Size'] < 500000:
                print 'Enabling SSD option for %s'%disk['Device']
                os.system('/sbin/esxcli storage nmp satp rule add --satp VMW_SATP_LOCAL --device %s --option "enable_ssd"'%disk['Device'])
                os.system('/sbin/esxcli storage core claiming unclaim --type device --device %s'%disk['Device'])
                os.system('/sbin/esxcli storage core claimrule run -d %s'%disk['Device'])
                os.system('/sbin/esxcli storage core claiming reclaim -d %s'%disk['Device'])
                os.system('/sbin/esxcli storage core claimrule run -d %s'%disk['Device'])

    def listSwitches(self, uplink=None):
        """
        List virtual switches
        """
        exit, vSwitchesOut = self._helper.executeCommand(['esxcli', '--formatter=keyvalue', 'network', 'vswitch', 'standard', 'list'])
        vSwitches = self._helper.convertKeyValue(''.join(vSwitchesOut))
        if self._verbose:
            for switch in vSwitches:
                print 'Name:%s Uplinks:%s Portgroups:%s'%(switch['Name'], switch['Uplinks'], switch['Portgroups'])
            print '\n'
        return [vs for vs in vSwitches if (uplink and uplink in vs['Uplinks']) or (not uplink)]

    def addVSwitch(self, name, vswitch):
        """
        Add vSwitch with name to uplink
        """
        self._helper.executeCommand(['esxcli', 'network', 'vswitch', 'standard', 'uplink' , 'add', '-v', name, '-u', vswitch])

    def listVMPortgroups(self):
        """
        List the current available portgroups
        """
        portgroupsResult = {}
        exit, portgroupsOut = self._helper.executeCommand(['esxcli', '--formatter=keyvalue', 'network', 'vswitch', 'standard', 'portgroup', 'list'])
        portgroups = self._helper.convertKeyValue(''.join(portgroupsOut))
        for portgroup in portgroups:
            portgroupsResult[portgroup['Name']] = portgroup['VLANID']
        return portgroupsResult

    def addVMPortgroup(self, name, vswitch):
        """
        Add VM Portgroup with name to vswitch
        """
        self._helper.executeCommand('network', 'vswitch', 'standard', 'portgroup', 'add', '-v', name, '-p', vswitch)

    def listVMNics(self):
        """
        List vmware nics
        """
        _, vmnicsOut = self._helper.executeCommand(['esxcli', '--formatter=keyvalue', 'network', 'nic', 'list'])
        # List of: nic['Name'], nic['MACAddress'], nic['Link'], nic['Description']
        return self._helper.convertKeyValue(''.join(vmnicsOut))

    def buildNicConfig(self, nics):
        """
        Build the nic vmx content for the required nics and assign them with semi-random MAC addresses
        """

        # We first take VMware's 00:0c:29 and set the U/L bit to 1, stating it's a locally administered MAC
        # Then we add the NIC specific part of one of the local NICs to the mac, creating an address that
        # will most likely be unique. However, this means that every VSA generated on a certain node will have
        # the same MAC address. This is not a supported use-case so should introduce no problems.
        mac = self.listVMNics()[0]['MACAddress']
        mac = '02:0c:29:%s' % mac[9:]
        mac_nr = int(mac.replace(':', ''), 16)

        vmx_base = """
                   ethernet%(nicseq)s.present = "true"
                   ethernet%(nicseq)s.virtualDev = "vmxnet3"
                   ethernet%(nicseq)s.networkName = "%(portgroup)s"
                   ethernet%(nicseq)s.connectionType = "bridged"
                   ethernet%(nicseq)s.addressType = "static"
                   ethernet%(nicseq)s.address = "%(mac)s"
                   """

        nic_vmx = ''
        for portgroup in nics:
            sequence = nics.index(portgroup)

            current_mac = hex(mac_nr + sequence)
            if current_mac[-1:] == 'L':  # Python 2.6.7
                current_mac = current_mac[:-1]
            current_mac = ':'.join([current_mac[2:].zfill(12)[i:i + 2] for i in range(0, 12, 2)])

            nic_vmx += vmx_base % {'nicseq'   : sequence,
                                   'portgroup': portgroup,
                                   'mac'      : current_mac}
        return nic_vmx

    def createVmConfig(self, name, cpu, memory, guestos, osbits, nic_vmx, disk_vmx, iso_vmx):
        datastore, vmfsDevices = self.getVMFSDevices()
        vmpath = '/vmfs/volumes/%s/%s'%(datastore, name)
        vmconfigFile = '%s/%s.vmx'%(vmpath,name)
        if not (os.path.exists(vmpath) or os.path.islink(vmpath)):
            vmpath = os.mkdir(vmpath)
        template_vmx = """.encoding = "UTF-8"
config.version = "8"
virtualHW.version = "8"
displayName = "%(name)s"
guestOS = "%(guestos)s"
memsize = "%(memory)s"
numvcpus = "%(cpus)s"
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
       'cpus'   : cpu,
       'memory' : memory,
       'guestos': guestos if osbits == 32 else '%s-64'%guestos,
       'nics'   : nic_vmx,
       'disks'  : disk_vmx,
       'dvdrom' : iso_vmx}
        vmConfig = open(vmconfigFile, "wb")
        vmConfig.write(template_vmx)
        vmConfig.close()
        return vmconfigFile

    def createVDiskMapping(self, vm, disk, seq, diskConfig):
        """
        Create a raw device mapping for disk and assign to VSA
        """
        datastore, vmfsDevices = self.getVMFSDevices()
        self._verbose = False
        vmpath = '/vmfs/volumes/%s/%s'%(datastore, vm)
        if not (os.path.exists(vmpath) or os.path.islink(vmpath)):
            os.mkdir(vmpath)
        vmdkPath = '%s/%s%s.vmdk'%(vmpath, 'ssd' if disk['IsSSD'] else 'hdd', seq)
        if self._verbose:
            print 'Setting up RDM %s for /vmfs/devices/disks/%s"'%(vmdkPath, disk['Device'])
        self._helper.executeCommand(['vmkfstools', '-z', '/vmfs/devices/disks/%s'%disk['Device'], vmdkPath])
        diskConfig += """scsi0:%(target)s.present = "TRUE"
scsi0:%(target)s.fileName = "%(disksource)s"
scsi0:%(target)s.deviceType = "scsi-hardDisk"
"""%{'disksource': vmdkPath,
     'target'    : seq if seq <= 6 else seq+1}
        self._verbose = False
        return diskConfig

    def getInstalledVibs(self, includePending=False):
        """
        list installed vibs
        """
        parameters = ['esxcli', '--formatter=keyvalue', 'software', 'vib', 'get']
        if includePending:
            parameters.append('--rebooting-image')
        _, vibsinstalledrun = self._helper.executeCommand(parameters)
        return self._helper.convertKeyValue(''.join(vibsinstalledrun))

    def checkforInstalledVib(self, vibname, includePending=False):
        """
        check for an installed vib
        """
        vibsinstalled = self.getInstalledVibs(includePending)
        if vibname in [vib['Name'] for vib in vibsinstalled]:
            return True
        else:
            return False

    def installVib(self, vibfilepath):
        """
        install esx cli shell plugin
        assumes plugin is available on the datastore in zipped form
        """
        self._helper.executeCommand(['esxcli', 'software', 'acceptance', 'set', '--level', 'CommunitySupported'])
        exit = os.system('/sbin/esxcli software vib install -v=%s --no-sig-check'%os.path.abspath(vibfilepath))
        return exit

    def getInstalledVibInformation(self, vibname):
        vibsinstalled = self.getInstalledVibs()
        matchingVibs = [vib for vib in vibsinstalled if vib['Name'] == vibname]
        if matchingVibs:
            vib = matchingVibs[0]
            versionParts = vib['Version'].split('-')[0].split('.')
            return int(versionParts[0]), int(versionParts[1]) if len(versionParts) > 1 else 0, vib
        else:
            remsg = errormsg.format("Could not find %s." % vibname)
            raise RuntimeError(remsg)

    def restartService(self, service):
        self._helper.executeCommand(['/etc/init.d/%s'%service, 'restart'])


if __name__ == '__main__':
    vmName = 'cfovs001'
    errormsg = "\n{0}\n**\n** {{0}}\n**\n{0}\n".format('*'*79)
    infomsg = "\n{0}"
    vibs = [{'description': 'Esxcli shell plugin', 'filename': 'esxcli-shell-1.1.0-15.x86_64.vib', 'vibname': 'esxcli-shell','install': True}]

    vmSys = vmwareSystem()
    vmHelper = InstallHelper()

    # Checking for FusionIO card drivers, if appropriate
    driver = 'scsi-iomemory-vsl'
    print infomsg.format("""Checking for Fusion-io card""")
    prc = sp.Popen('lspci | grep Fusion-io', stdout=sp.PIPE, shell=True)
    out = prc.stdout.read()
    if out != '':
        import sys
        try:
            print "* A card of type '%s' was found, checking for required driver." % out.strip().split(':')[-1].strip()
        except:
            print "* A Fusion-io card was found, checking for required driver."

        if vmSys.checkforInstalledVib(driver):
            _, _, vib = vmSys.getInstalledVibInformation(driver)
            print "* Driver '%s' version %s found" % (driver, vib['Version'])
        elif vmSys.checkforInstalledVib(driver, includePending=True):
            print """
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++  The required driver '%s' is installed,           +++
+++  but a reboot is required to finalize the installation.          +++
+++  Please restart this script once the reboot has been completed.  +++
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
""" % driver
            sys.exit()
        else:
            print """
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++  The required driver '%s' was not found.                       +++
+++  Please install the correct driver, reboot the host and restart this script.  +++
+++  The driver can be downloaded at the VMware website at:                       +++
+++                                                                               +++
+++  https://my.vmware.com/web/vmware/searchresults/#start=0&q=scsi-iomemory-vsl  +++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
""" % driver
            sys.exit()
    else:
        print "* No Fusion-io card was found"

    # Checking for Megaraid driver
    print infomsg.format("""Checking for compatible scsi-megaraid-sas driver""")
    major, minor, _ = vmSys.getInstalledVibInformation('scsi-megaraid-sas')
    print """Found scsi-megaraid-sas version {0}.{1}\n""".format(major, minor)

    if major < 6 or (major == 6 and minor < 506):
        remsg = errormsg.format("Older scsi-megaraid-sas version detected {0}.{1}, this is known to have issues. Please update to at least 6.506\n** http://kb.vmware.com/selfservice/microsites/search.do?cmd=displayKC&externalId=2052368".format(major, minor))
        raise RuntimeError(remsg)

    print infomsg.format("""Checking for previous cfovs installations""")
    allVMS = vmSys.listvms()
    # first line of allVMS is a header and first column all vm names
    # this will most likely fail if vmName contains any spaces
    if vmName in [v.split()[1] for v in allVMS[1:]]:
        remsg = errormsg.format("Detected another possible cfovs on this host. Delete current cfovs before installing a new one.")
        raise RuntimeError(remsg)

    from optparse import OptionParser
    parser = OptionParser(description='CloudFrames vRun Setup')
    parser.add_option('--image', dest='image', help='absolute path to your install iso')
    parser.add_option('--producttype', dest='producttype', help='Product type (vrun25 or vrun50)')
    (options, args) = parser.parse_args()
    if not options.image:
        print parser.get_usage()
        print "valid options:"
        for option in parser.option_list:
            print"\t%s\t%s"%(option.get_opt_string(), option.help)
        print ''
        exit(0)
    if not options.producttype:
        print parser.get_usage()
        print "please specify a producttype: --producttype vrun25"
        print ''
        exit(0)

    imagefile = os.path.abspath(options.image)
    producttype = options.producttype
    product_memory = {'vrun25': 16384,
                      'vrun50': 24576}

    if producttype not in ('vrun25', 'vrun50'):
        remsg = errormsg.format("Unknown product type: {0} \n".format(producttype))
        raise RuntimeError(remsg)

    if not os.path.isfile(imagefile):
        remsg = errormsg.format("Unable to find installer file:\n** {0}".format(imagefile))
        raise RuntimeError(remsg)

    for vib in vibs:
        if vmSys.checkforInstalledVib(vib['vibname']) == True:
            print infomsg.format("{0} already installed".format(vib['description']))
            vib['install'] = False
        else:
            pluginfile = os.path.abspath(vib['filename'])
            if not os.path.isfile(pluginfile):
                remsg = errormsg.format("Unable to find {0}:{1}\n**".format(vib['description'], vib['filename']))
                raise RuntimeError(remsg)
            print infomsg.format("{0} needs to be installed".format(vib['description']))

    vmSys.listDisks()
    print infomsg.format("""Determine ESX host networking to use""")

    vSwitches = vmSys.listSwitches()
    if not vSwitches:
        remsg = errormsg.format('No virtual switches found in your ESXi network configuration')
        raise RuntimeError(remsg)
    privatePG = 'CloudFramesStorage'
    publicPG = 'CloudFramesPublic'
    publicVlanid = 4095
    availablePGs = vmSys.listVMPortgroups()
    for pg in [privatePG, publicPG]:
        if not pg in availablePGs.keys():
            remsg = errormsg.format('No portgroup "%s" found in your ESXi network configuration\n** Current available portgroups are %s'%(pg, availablePGs.keys()))
            raise RuntimeError(remsg)
        if pg == publicPG and availablePGs[pg] != publicVlanid:
            remsg = errormsg.format('The portgroup "%s" should be configured with vlanID "%s"'%(pg, publicVlanid))
            raise RuntimeError(remsg)
    if len([v for v in vSwitches if privatePG in v['Portgroups'] and len(v['Uplinks']) > 0]) == 0:
        remsg = errormsg.format("Virtual Machine Portgroup %s is missing a required vmnic - exiting"%privatePG)
        raise RuntimeError(remsg)

    nicConfig = vmSys.buildNicConfig([privatePG, publicPG])

    print infomsg.format("""Configuring firewall to allow inbound VNC access to VM consoles""")
    datastore, vmfsDevices = vmSys.getVMFSDevices()
    remoteVncXML = """
<ConfigRoot>
<service id='0032'>
 <id>remoteVNC</id>
 <rule id = '0000'>
  <direction>inbound</direction>
  <protocol>tcp</protocol>
  <porttype>dst</porttype>
  <port>
   <begin>5900</begin>
   <end>5950</end>
  </port>
 </rule>
 <enabled>true</enabled>
</service>
</ConfigRoot>
"""
    vncPersistentFirewallPath = '/vmfs/volumes/%s/remoteVnc.xml'%datastore
    vncFirewallPath = '/etc/vmware/firewall/remoteVnc.xml'
    vncFirewall = open(vncFirewallPath, "wb")
    vncFirewall.write(remoteVncXML)
    vncFirewall.close()
    vncPersistentFirewall = open(vncPersistentFirewallPath, "wb")
    vncPersistentFirewall.write(remoteVncXML)
    vncPersistentFirewall.close()
    rcLocal = open('/etc/rc.local.d/copyRemoteVnc', 'wb')
    rcLocal.write("""#!/bin/sh
cp /vmfs/volumes/%s/remoteVnc.xml /etc/vmware/firewall/
"""%datastore)
    rcLocal.close()
    vmHelper.executeCommand(['esxcli', 'network', 'firewall', 'refresh'])

    print infomsg.format("""Creating Raw Device Mappings""")
    selectedDisks, ssdDisks = [], []
    vmSys.enableVmwareSSDOption()
    allDisks = vmSys.listDisks()
    if len(allDisks) < 5:
        remsg = errormsg.format('Not enough disks available to install a VSA')
        raise RuntimeError(remsg)
    #while len(selectedDisks) <> 8:
    #    selectedDisks = vmHelper.askChoiceMultiple(allDisks, 'Select the disks to use for VSA')
    seq, diskConfig = 0, ''
    for disk in allDisks:
        if disk['IsSSD']:
            ssdDisks.append(disk)
        else:
            diskConfig = vmSys.createVDiskMapping(vmName, disk, seq, diskConfig)
            seq+=1
    for ssd in ssdDisks:
        diskConfig = vmSys.createVDiskMapping(vmName, ssd, seq, diskConfig)
        seq+=1

    isoConfig = """ide1:0.present = "TRUE"
ide1:0.fileName = "%(installISO)s"
ide1:0.deviceType = "cdrom-image"
ide1:0.clientDevice = "FALSE"
ide1:0.startConnected = "TRUE"
"""%{'ds': datastore,
     'installISO': imagefile}
    nonIsoConfig = """ide1:0.present = "TRUE"
ide1:0.fileName = ""
ide1:0.deviceType = "atapi-cdrom"
ide1:0.clientDevice = "TRUE"
ide1:0.startConnected = "FALSE"
"""

    print infomsg.format("""Creating CloudFrames vRun virtual machine config""")
    cpu = 4
    memory = product_memory[producttype]
    guestos = 'ubuntu'
    osbits = 64
    vmconfigFile = vmSys.createVmConfig(vmName, cpu, memory, guestos, osbits, nicConfig, diskConfig, isoConfig)

    """
    Register and poweron VSA
    """
    exit, out = vmHelper.executeCommand(['vim-cmd', 'solo/registervm', vmconfigFile], True)
    vmId = out[0].strip()
    #print "++++ REGISTER OUTPUT: %s"%registerOutput
    #allVMS = vmHelper.executeCommand(['vim-cmd', 'vmsvc/getallvms'], True)
    #vmId = None
    #for vmLine in allVMS:
    #    id, name, file, guestOS, version, annotation = vmLine.split()
    #    if vmName == name:
    #        vmId = id
    #        break
    #if not vmId:
    #    raise('The machine register most likely failed as we were unable to retrieve its id')
    print infomsg.format("""Starting CloudFrames VSA""")
    vmHelper.executeCommand(['vim-cmd', 'hostsvc/autostartmanager/enable_autostart', '1'])
    vmHelper.executeCommand(['vim-cmd', 'hostsvc/autostartmanager/update_autostartentry', '{0}'.format(vmId), 'PowerOn', '5', '1', 'guestShutdown', '5', 'systemDefault'])
    vmHelper.executeCommand(['vim-cmd', 'vmsvc/power.on', vmId])

    restarthostd = False
    for vib in vibs:
        if vib['install'] == True:
            restarthostd = True
            print infomsg.format("""Installing {0}""".format(vib['description']))
            installresult = vmSys.installVib(vib['filename'])
            if installresult != 0:
                remsg = errormsg.format("Failed to install plugin:\n** {0}".format(vib['filename']))
                raise RuntimeError(remsg)

    if restarthostd == True:
        print infomsg.format("""Restarting hostd - this resets all vSphere client connections""")
        vmSys.restartService('hostd')

    print """
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++     The CloudFrames vRun Virtual Machine (cfovs001) is powered on     +++
+++     * Open the vSphere Client                                         +++
+++     * Connect to the {0} Virtual Machine's console               +++
+++     * Change the boot order so the VM boots from CD-ROM Drive         +++
+++     * Follow the on-screen install instructions                       +++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
""".format(vmName)

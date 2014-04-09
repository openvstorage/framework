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
VMachine module
"""

import time
import copy

from subprocess import check_output
from ovs.celery import celery
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.generic.system import Ovs
from ovs.lib.vdisk import VDiskController
from ovs.lib.messaging import MessageController
from ovs.plugin.provider.configuration import Configuration
from ovs.log.logHandler import LogHandler
from ovs.extensions.generic.volatilemutex import VolatileMutex

logger = LogHandler('ovs.lib', name='vmachine')


class VMachineController(object):
    """
    Contains all BLL related to VMachines
    """

    @staticmethod
    @celery.task(name='ovs.machine.create_multiple_from_template')
    def create_multiple_from_template(name, machineguid, pmachineguids, start, amount, description=None):
        pmachine_pointer = 0
        for i in xrange(start, start + amount):
            new_name = name if amount == 1 else '{0}-{1}'.format(name, i)
            pmachineguid = pmachineguids[pmachine_pointer]
            pmachine_pointer += 1
            if pmachine_pointer >= len(pmachineguids):
                pmachine_pointer = 0
            VMachineController.create_from_template(name=new_name,
                                                    machineguid=machineguid,
                                                    pmachineguid=pmachineguid,
                                                    description=description)

    @staticmethod
    @celery.task(name='ovs.machine.create_from_template')
    def create_from_template(name, machineguid, pmachineguid, description=None):
        """
        Create a new vmachine using an existing vmachine template

        @param machineguid: guid of the template vmachine
        @param name: name of new vmachine
        @param pmachineguid: guid of hypervisor to create new vmachine on
        @return: guid of the newly created vmachine | False on any failure
        """

        template_vm = VMachine(machineguid)
        if not template_vm.is_vtemplate:
            return False

        target_pm = PMachine(pmachineguid)
        target_hypervisor = Factory.get(target_pm)

        vpool = None
        vpool_guids = set()
        if template_vm.vpool is not None:
            vpool = template_vm.vpool
            vpool_guids.add(vpool.guid)
        for disk in template_vm.vdisks:
            vpool = disk.vpool
            vpool_guids.add(vpool.guid)
        if len(vpool_guids) != 1:
            raise RuntimeError('Only 1 vpool supported on template disk(s) - {0} found!'.format(len(vpool_guids)))

        if not template_vm.pmachine.hvtype == target_pm.hvtype:
            raise RuntimeError('Source and target hypervisor not identical')

        # Currently, only one vPool is supported, so we can just use whatever the `vpool` variable above
        # was set to as 'the' vPool for the code below. This obviously will have to change once vPool mixes
        # are supported.

        target_vsr = None
        source_vsr = None
        for vpool_vsr in vpool.vsrs:
            if vpool_vsr.serving_vmachine.pmachine_guid == target_pm.guid:
                target_vsr = vpool_vsr
            if vpool_vsr.serving_vmachine.pmachine_guid == template_vm.pmachine_guid:
                source_vsr = vpool_vsr
        if target_vsr is None:
            raise RuntimeError('Volume not served on target hypervisor')

        source_hv = Factory.get(template_vm.pmachine)
        target_hv = Factory.get(target_pm)
        if not source_hv.is_datastore_available(source_vsr.storage_ip, source_vsr.mountpoint):
            raise RuntimeError('Datastore unavailable on source hypervisor')
        if not target_hv.is_datastore_available(target_vsr.storage_ip, target_vsr.mountpoint):
            raise RuntimeError('Datastore unavailable on target hypervisor')

        source_vm = source_hv.get_vm_object(template_vm.hypervisorid)
        if not source_vm:
            raise RuntimeError('VM with key reference {0} not found'.format(template_vm.hypervisorid))

        name_duplicates = VMachineList.get_vmachine_by_name(name)
        if name_duplicates is not None and len(name_duplicates) > 0:
            raise RuntimeError('A vMachine with name {0} already exists'.format(name))

        vm_path = target_hypervisor.get_vmachine_path(name, target_vsr.serving_vmachine.machineid)

        new_vm = VMachine()
        new_vm.copy_blueprint(template_vm)
        new_vm.vpool = template_vm.vpool
        new_vm.pmachine = target_pm
        new_vm.name = name
        new_vm.description = description
        new_vm.is_vtemplate = False
        new_vm.devicename = target_hypervisor.clean_vmachine_filename(vm_path)
        new_vm.status = 'CREATED'
        new_vm.save()

        vsrs = [vsr for vsr in vpool.vsrs if vsr.serving_vmachine.pmachine_guid == new_vm.pmachine_guid]
        if len(vsrs) == 0:
            raise RuntimeError('Cannot find VSR serving {0} on {1}'.format(vpool.name,
                                                                           new_vm.pmachine.name))
        vsrguid = vsrs[0].guid

        disks = []
        disks_by_order = sorted(template_vm.vdisks, key=lambda x: x.order)
        try:
            for disk in disks_by_order:
                prefix = '{0}-clone'.format(disk.name)
                result = VDiskController.create_from_template(
                    diskguid=disk.guid,
                    devicename=prefix,
                    pmachineguid=target_pm.guid,
                    machinename=new_vm.name,
                    machineguid=new_vm.guid,
                    vsrguid=vsrguid
                )
                disks.append(result)
                logger.debug('Disk appended: {0}'.format(result))
        except Exception:
            # @TODO cleanup strategy to be defined
            new_vm.delete()
            raise

        try:
            result = target_hv.create_vm_from_template(
                name, source_vm, disks, target_vsr.storage_ip, target_vsr.mountpoint, wait=True
            )
        except:
            VMachineController.delete(machineguid=new_vm.guid)
            raise

        new_vm.hypervisorid = result
        new_vm.status = 'SYNC'
        new_vm.save()
        return new_vm.guid

    @staticmethod
    @celery.task(name='ovs.machine.clone')
    def clone(machineguid, timestamp, name, **kwargs):
        """
        Clone a vmachine using the disk snapshot based on a snapshot timestamp

        @param machineguid: guid of the machine to clone
        @param timestamp: timestamp of the disk snapshots to use for the clone
        @param name: name for the new machine
        """
        _ = kwargs
        machine = VMachine(machineguid)

        disks = {}
        for snapshot in machine.snapshots:
            if snapshot['timestamp'] == timestamp:
                for diskguid, snapshotguid in snapshot['snapshots'].iteritems():
                    disks[diskguid] = snapshotguid

        new_machine = VMachine()
        new_machine.copy_blueprint(machine)
        new_machine.name = name
        new_machine.pmachine = machine.pmachine
        new_machine.save()

        new_disk_guids = []
        disks_by_order = sorted(machine.vdisks, key=lambda x: x.order)
        for currentDisk in disks_by_order:
            if machine.is_vtemplate and currentDisk.templatesnapshot:
                snapshotid = currentDisk.templatesnapshot
            else:
                snapshotid = disks[currentDisk.guid]
            prefix = '%s-clone' % currentDisk.name

            result = VDiskController.clone(diskguid=currentDisk.guid,
                                           snapshotid=snapshotid,
                                           devicename=prefix,
                                           pmachineguid=new_machine.pmachine_guid,
                                           machinename=new_machine.name,
                                           machineguid=new_machine.guid)
            new_disk_guids.append(result['diskguid'])

        hv = Factory.get(machine.pmachine)
        try:
            result = hv.clone_vm(machine.hypervisorid, name, disks, None, True)
        except:
            VMachineController.delete(machineguid=new_machine.guid)
            raise

        new_machine.hypervisorid = result
        new_machine.save()
        return new_machine.guid

    @staticmethod
    @celery.task(name='ovs.machine.delete')
    def delete(machineguid, **kwargs):
        """
        Delete a vmachine

        @param machineguid: guid of the machine
        """
        _ = kwargs
        machine = VMachine(machineguid)

        if machine.pmachine:
            hv = Factory.get(machine.pmachine)
            hv.delete_vm(machine.hypervisorid, True)

        for disk in machine.vdisks:
            disk.delete()
        machine.delete()

    @staticmethod
    @celery.task(name='ovs.machine.delete_from_voldrv')
    def delete_from_voldrv(name, vsrid):
        """
        This method will delete a vmachine based on the name of the vmx given
        """
        pmachine = PMachineList.get_by_vsrid(vsrid)
        if pmachine.hvtype not in ['VMWARE', 'KVM']:
            return

        hypervisor = Factory.get(pmachine)
        name = hypervisor.clean_vmachine_filename(name)
        if pmachine.hvtype == 'VMWARE':
            vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
            vpool = vsr.vpool
        else:
            vpool = None
        vm = VMachineList.get_by_devicename_and_vpool(name, vpool)
        if vm is not None:
            MessageController.fire(MessageController.Type.EVENT, {'type': 'vmachine_deleted',
                                                                  'metadata': {'name': vm.name}})
            vm.delete(abandon=True)

    @staticmethod
    @celery.task(name='ovs.machine.rename_from_voldrv')
    def rename_from_voldrv(old_name, new_name, vsrid):
        """
        This machine will handle the rename of a vmx file
        """
        pmachine = PMachineList.get_by_vsrid(vsrid)
        if pmachine.hvtype not in ['VMWARE', 'KVM']:
            return

        hypervisor = Factory.get(pmachine)
        if pmachine.hvtype == 'VMWARE':
            vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
            vpool = vsr.vpool
        else:
            vpool = None

        old_name = hypervisor.clean_vmachine_filename(old_name)
        new_name = hypervisor.clean_vmachine_filename(new_name)
        scenario = hypervisor.get_rename_scenario(old_name, new_name)
        if scenario == 'RENAME':
            # Most likely a change from path. Updaing path
            vm = VMachineList.get_by_devicename_and_vpool(old_name, vpool)
            if vm is not None:
                vm.devicename = new_name
                vm.save()
        elif scenario == 'UPDATE':
            vm = VMachineList.get_by_devicename_and_vpool(new_name, vpool)
            if vm is None:
                # The vMachine doesn't seem to exist, so it's likely the create didn't came trough
                # Let's create it anyway
                VMachineController.update_from_voldrv(new_name, vsrid)
            vm = VMachineList.get_by_devicename_and_vpool(new_name, vpool)
            if vm is None:
                raise RuntimeError('Could not create vMachine on rename. Aborting.')
            try:
                VMachineController.sync_with_hypervisor(vm.guid, vsrid)
                vm.status = 'SYNC'
            except:
                vm.status = 'SYNC_NOK'
            vm.save()

    @staticmethod
    @celery.task(name='ovs.machine.set_as_template')
    def set_as_template(machineguid):
        """
        Set a vmachine as template

        @param machineguid: guid of the machine
        @return: vmachine template conversion successful: True|False
        """
        # Do some magic on the storage layer?
        # This is most likely required as extra security measure
        # Suppose the template is set back to a real machine
        # it can be deleted within vmware which should be blocked.
        # This might also require a storagerouter internal check
        # to be implemented to discourage volumes from being deleted
        # when clones were made from it.

        vmachine = VMachine(machineguid)
        if vmachine.hypervisor_status == 'RUNNING':
            raise RuntimeError('vMachine {0} may not be running to set it as vTemplate'.format(vmachine.name))

        for disk in vmachine.vdisks:
            VDiskController.set_as_template(diskguid=disk.guid)

        vmachine.is_vtemplate = True
        vmachine.invalidate_dynamics(['snapshots'])
        vmachine.save()

    @staticmethod
    @celery.task(name='ovs.machine.rollback')
    def rollback(machineguid, timestamp):
        """
        Rolls back a VM based on a given disk snapshot timestamp
        """
        vmachine = VMachine(machineguid)
        if vmachine.hypervisor_status == 'RUNNING':
            raise RuntimeError('vMachine {0} may not be running to set it as vTemplate'.format(
                vmachine.name
            ))

        snapshots = [snap for snap in vmachine.snapshots if snap['timestamp'] == timestamp]
        if not snapshots:
            raise ValueError('No vmachine snapshots found for timestamp {}'.format(timestamp))

        for disk in vmachine.vdisks:
            VDiskController.rollback(diskguid=disk.guid,
                                     timestamp=timestamp)

        vmachine.invalidate_dynamics(['snapshots'])

    @staticmethod
    @celery.task(name='ovs.machine.snapshot')
    def snapshot(machineguid, label=None, is_consistent=False, timestamp=None, is_automatic=False):
        """
        Snapshot VMachine disks

        @param machineguid: guid of the machine
        @param label: label to give the snapshots
        @param is_consistent: flag indicating the snapshot was consistent or not
        @param timestamp: override timestamp, if required. Should be a unix timestamp
        """

        timestamp = timestamp if timestamp is not None else time.time()
        timestamp = str(int(float(timestamp)))

        metadata = {'label': label,
                    'is_consistent': is_consistent,
                    'timestamp': timestamp,
                    'machineguid': machineguid,
                    'is_automatic': is_automatic}
        machine = VMachine(machineguid)

        # @todo: we now skip creating a snapshot when a vmachine's disks
        #        is missing a mandatory property: volumeid
        #        subtask will now raise an exception earlier in the workflow
        for disk in machine.vdisks:
            if not disk.volumeid:
                message = 'Missing volumeid on disk {0} - unable to create snapshot for vm {1}'.format(
                    disk.guid, machine.guid
                )
                logger.info('Error: {0}'.format(message))
                raise RuntimeError(message)

        snapshots = {}
        success = True
        try:
            for disk in machine.vdisks:
                snapshots[disk.guid] = VDiskController.create_snapshot(diskguid=disk.guid,
                                                                       metadata=metadata)
        except Exception as ex:
            logger.info('Error snapshotting disk {0}: {1}'.format(disk.name, str(ex)))
            success = False
            for diskguid, snapshotid in snapshots.iteritems():
                VDiskController.delete_snapshot(diskguid=diskguid,
                                                snapshotid=snapshotid)
        logger.info('Create snapshot for vMachine {0}: {1}'.format(
            machine.name, 'Success' if success else 'Failure'
        ))
        machine.invalidate_dynamics(['snapshots'])
        if not success:
            raise RuntimeError('Failed to snapshot vMachine {0}'.format(machine.name))

    @staticmethod
    @celery.task(name='ovs.machine.sync_with_hypervisor')
    def sync_with_hypervisor(vmachineguid, vsrid=None):
        """
        Updates a given vmachine with data retreived from a given pmachine
        """
        try:
            vmachine = VMachine(vmachineguid)
            if vsrid is None and vmachine.hypervisorid is not None and vmachine.pmachine is not None:
                # Only the vmachine was received, so base the sync on hypervisorid and pmachine
                hypervisor = Factory.get(vmachine.pmachine)
                logger.info('Syncing vMachine (name {})'.format(vmachine.name))
                vm_object = hypervisor.get_vm_agnostic_object(vmid=vmachine.hypervisorid)
            elif vsrid is not None and vmachine.devicename is not None:
                # VSR id was given, using the devicename instead (to allow hypervisorid updates
                # which can be caused by re-adding a vm to the inventory)
                pmachine = PMachineList.get_by_vsrid(vsrid)
                vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
                hypervisor = Factory.get(pmachine)
                vmachine.pmachine = pmachine
                vmachine.save()

                logger.info('Syncing vMachine (device {}, ip {}, mtpt {})'.format(vmachine.devicename,
                                                                                  vsr.storage_ip,
                                                                                  vsr.mountpoint))
                vm_object = hypervisor.get_vm_object_by_devicename(devicename=vmachine.devicename,
                                                                   ip=vsr.storage_ip,
                                                                   mountpoint=vsr.mountpoint)
            else:
                message = 'Not enough information to sync vmachine'
                logger.info('Error: {0}'.format(message))
                raise RuntimeError(message)
        except Exception as ex:
            logger.info('Error while fetching vMachine info: {0}'.format(str(ex)))
            raise

        if vm_object is None:
            message = 'Could not retreive hypervisor vmachine object'
            logger.info('Error: {0}'.format(message))
            raise RuntimeError(message)
        else:
            VMachineController.update_vmachine_config(vmachine, vm_object)

    @staticmethod
    @celery.task(name='ovs.machine.update_from_voldrv')
    def update_from_voldrv(name, vsrid):
        """
        This method will update/create a vmachine based on a given vmx/xml file
        """
        pmachine = PMachineList.get_by_vsrid(vsrid)
        if pmachine.hvtype not in ['VMWARE', 'KVM']:
            return

        hypervisor = Factory.get(pmachine)
        name = hypervisor.clean_vmachine_filename(name)
        if hypervisor.should_process(name):
            if pmachine.hvtype == 'VMWARE':
                vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
                vpool = vsr.vpool
            else:
                vpool = None
            pmachine = PMachineList.get_by_vsrid(vsrid)
            mutex = VolatileMutex('{}_{}'.format(name, vpool.guid))
            try:
                mutex.acquire(wait=5)
                vmachine = VMachineList.get_by_devicename_and_vpool(name, vpool)
                if not vmachine:
                    vmachine = VMachine()
                    vmachine.vpool = vpool
                    vmachine.pmachine = pmachine
                    vmachine.status = 'CREATED'
                vmachine.devicename = name
                vmachine.save()
            finally:
                mutex.release()

            if pmachine.hvtype == 'KVM':
                try:
                    VMachineController.sync_with_hypervisor(vmachine.guid, vsrid)
                    vmachine.status = 'SYNC'
                except:
                    vmachine.status = 'SYNC_NOK'
                vmachine.save()

    @staticmethod
    @celery.task(name='ovs.machine.update_vmachine_config')
    def update_vmachine_config(vmachine, vm_object, pmachine=None):
        """
        Update a vMachine configuration with a given vMachine configuration
        """
        try:
            vdisks_synced = 0
            if vmachine.name is None:
                MessageController.fire(MessageController.Type.EVENT,
                                       {'type': 'vmachine_created',
                                        'metadata': {'name': vm_object['name']}})
            elif vmachine.name != vm_object['name']:
                MessageController.fire(MessageController.Type.EVENT,
                                       {'type': 'vmachine_renamed',
                                        'metadata': {'old_name': vmachine.name,
                                                     'new_name': vm_object['name']}})
            if pmachine is not None:
                vmachine.pmachine = pmachine
            vmachine.name = vm_object['name']
            vmachine.hypervisorid = vm_object['id']
            vmachine.devicename = vm_object['backing']['filename']
            vmachine.save()
            # Updating and linking disks
            vsrs = VolumeStorageRouterList.get_volumestoragerouters()
            datastores = dict([('{}:{}'.format(vsr.storage_ip, vsr.mountpoint), vsr) for vsr in vsrs])
            vdisk_guids = []
            for disk in vm_object['disks']:
                if disk['datastore'] in vm_object['datastores']:
                    datastore = vm_object['datastores'][disk['datastore']]
                    if datastore in datastores:
                        vdisk = VDiskList.get_by_devicename_and_vpool(disk['filename'], datastores[datastore].vpool)
                        if vdisk is None:
                            # The disk couldn't be located, but is in our datastore. We might be in a recovery scenario
                            vdisk = VDisk()
                            vdisk.vpool = datastores[datastore].vpool
                            vdisk.save()
                            vdisk = VDisk(vdisk.guid)  # Reload the vDisk, loading the vsr_client
                            vdisk.devicename = disk['filename']
                            vdisk.volumeid = vdisk.vsr_client.get_volume_id(str(disk['backingfilename']))
                            vdisk.size = vdisk.info['volume_size']
                        # Update the disk with information from the hypervisor
                        if vdisk.vmachine is None:
                            MessageController.fire(MessageController.Type.EVENT,
                                                   {'type': 'vdisk_attached',
                                                    'metadata': {'vmachine_name': vmachine.name,
                                                                 'vdisk_name': disk['name']}})
                        vdisk.vmachine = vmachine
                        vdisk.name = disk['name']
                        vdisk.order = disk['order']
                        vdisk.save()
                        vdisk_guids.append(vdisk.guid)
                        vdisks_synced += 1

            for vdisk in vmachine.vdisks:
                if vdisk.guid not in vdisk_guids:
                    MessageController.fire(MessageController.Type.EVENT,
                                           {'type': 'vdisk_detached',
                                            'metadata': {'vmachine_name': vmachine.name,
                                                         'vdisk_name': vdisk.name}})
                    vdisk.vmachine = None
                    vdisk.save()

            logger.info('Updating vMachine finished (name {}, {} vdisks (re)linked)'.format(
                vmachine.name, vdisks_synced
            ))
        except Exception as ex:
            logger.info('Error during vMachine update: {0}'.format(str(ex)))
            raise

    @staticmethod
    @celery.task(name='ovs.vsa.get_physical_metadata')
    def get_physical_metadata():
        """
        Gets physical information about the machine this task is running on
        """
        mountpoints = check_output('mount -v', shell=True).strip().split('\n')
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2
                       and not p.split(' ')[2].startswith('/dev') and not p.split(' ')[2].startswith('/proc')
                       and not p.split(' ')[2].startswith('/sys') and not p.split(' ')[2].startswith('/run')
                       and p.split(' ')[2] != '/']
        ipaddresses = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().split('\n')
        ipaddresses = [ip.strip() for ip in ipaddresses]
        xmlrpcport = Configuration.get('volumedriver.filesystem.xmlrpc.port')
        return {'mountpoints': mountpoints,
                'ipaddresses': ipaddresses,
                'xmlrpcport': xmlrpcport}

    @staticmethod
    @celery.task(name='ovs.vsa.add_vpool')
    def add_vpool(parameters):
        """
        Add a vPool to the machine this task is running on
        """
        from ovs.extensions.grid.manager import Manager
        Manager.init_vpool(parameters['vsa_ip'], parameters['vpool_name'], parameters=parameters)

    @staticmethod
    @celery.task(name='ovs.vsa.remove_vsr')
    def remove_vsr(vsr_guid):
        """
        Removes a VSR (and, if it was the last VSR for a vPool, the vPool is removed as well)
        """
        from ovs.extensions.grid.manager import Manager

        Manager.remove_vpool(vsr_guid)

    @staticmethod
    @celery.task(name='ovs.vsa.update_vsrs')
    def update_vsrs(vsr_guids, vsas, parameters):
        """
        Add/remove multiple vPools
        @param vsr_guids: VSRs to be removed
        @param vsas: VSA's on which to add a new link
        @param parameters: Settings for new links
        """
        success = True
        # Add VSRs
        for vsa_ip, vsa_machineid in vsas:
            try:
                new_parameters = copy.copy(parameters)
                new_parameters['vsa_ip'] = vsa_ip
                local_machineid = Ovs.get_my_machine_id()
                if local_machineid == vsa_machineid:
                    # Inline execution, since it's on the same node (preventing deadlocks)
                    VMachineController.add_vpool(new_parameters)
                else:
                    # Async execution, since it has to be executed on another node
                    # @TODO: Will break in Celery 3.2, need to find another solution
                    # Requirements:
                    # - This code cannot continue until this new task is completed (as all these VSAs need to be
                    #   handled sequentially
                    # - The wait() or get() method are not allowed anymore from within a task to prevent deadlocks
                    result = VMachineController.add_vpool.s(new_parameters).apply_async(
                        routing_key='vsa.{0}'.format(vsa_machineid)
                    )
                    result.wait()
            except:
                success = False
        # Remove VSRs
        for vsr_guid in vsr_guids:
            try:
                VMachineController.remove_vsr(vsr_guid)
            except:
                success = False
        return success

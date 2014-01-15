# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachine module
"""
import time
import logging
import uuid

from celery import group
from ovs.celery import celery
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.extensions.hypervisor.factory import Factory
from ovs.lib.vdisk import VDiskController
from ovs.lib.messaging import MessageController


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

        vpool = None
        vpool_guids = set()
        for disk in template_vm.vdisks:
            vpool = disk.vpool
            vpool_guids.add(vpool.guid)
        if len(vpool_guids) != 1:
            raise RuntimeError('Only 1 vpool supported on template disk(s) - {0} found!'.format(len(vpool_guids)))

        if not template_vm.pmachine.hvtype == target_pm.hvtype:
            raise RuntimeError('Source and target hypervisor not identical')

        vsr = None
        for vsr in vpool.vsrs:
            if vsr.serving_vmachine.pmachine.guid == target_pm.guid:
                break
            raise RuntimeError('Volume not served on target hypervisor')
        if vsr is None:
            raise RuntimeError('No VSR found')

        source_hv = Factory.get(template_vm.pmachine)
        target_hv = Factory.get(target_pm)
        if not source_hv.is_datastore_available(vsr.ip, vsr.mountpoint):
            raise RuntimeError('Datastore unavailable on source hypervisor')
        if not target_hv.is_datastore_available(vsr.ip, vsr.mountpoint):
            raise RuntimeError('Datastore unavailable on target hypervisor')

        source_vm = source_hv.get_vm_object(template_vm.hypervisorid)
        if not source_vm:
            raise RuntimeError('VM with key reference {0} not found'.format(template_vm.hypervisorid))

        name_duplicates = VMachineList.get_vmachine_by_name(name)
        if name_duplicates is not None and len(name_duplicates) > 0:
            raise RuntimeError('A vMachine with name {0} already exists'.format(name))

        # @todo verify all disks can be cloned on target
        # @todo ie vpool is available on both hypervisors
        # @todo if so, continue

        new_vm = VMachine()
        new_vm.copy_blueprint(template_vm)
        new_vm.name = name
        new_vm.description = description
        new_vm.is_vtemplate = False
        new_vm.devicename = '{}/{}.vmx'.format(name.replace(' ', '_'), name.replace(' ', '_'))
        new_vm.status = 'CREATED'
        new_vm.save()

        disks = []
        disks_by_order = sorted(template_vm.vdisks, key=lambda x: x.order)
        for disk in disks_by_order:
            prefix = '{0}-clone'.format(disk.name)
            result = VDiskController.create_from_template(
                diskguid=disk.guid,
                devicename=prefix,
                location=new_vm.name.replace(' ', '_'),
                machineguid=new_vm.guid
            )
            disks.append(result)

        # @todo: cleanup when not all disks could be successfully created
        # @todo: skip vm creation on hypervisor in that case

        provision_machine_task = target_hv.create_vm_from_template.s(
            target_hv, name, source_vm, disks, esxhost=None, wait=True
        )
        provision_machine_task.link_error(VMachineController.delete.s(machineguid=new_vm.guid))
        result = provision_machine_task()

        new_vm.hypervisorid = result
        new_vm.status = 'SYNC'
        new_vm.save()
        return new_vm.guid

    @staticmethod
    @celery.task(name='ovs.machine.create_from_voldrv')
    def create_from_voldrv(name):
        """
        This method will create a vmachine based on a given vmx file
        """
        name = name.strip('/')
        if name.endswith('.vmx'):
            vmachine = VMachineList.get_by_devicename(name)
            if not vmachine:
                vmachine = VMachine()
                vmachine.status = 'CREATED'
            vmachine.devicename = name
            vmachine.save()

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
                                           location=new_machine.name,
                                           machineguid=new_machine.guid)
            new_disk_guids.append(result['diskguid'])

        hv = Factory.get(machine.pmachine)
        provision_machine_task = hv.clone_vm.s(
            hv, machine.hypervisorid, name, disks, None, True
        )
        provision_machine_task.link_error(
            VMachineController.delete.s(machineguid=new_machine.guid)
        )
        result = provision_machine_task()

        new_machine.hypervisorid = result.get()
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
            delete_vmachine_task = hv.delete_vm.s(
                hv, machine.hypervisorid, None, True)
            delete_vmachine_task()

        for disk in machine.vdisks:
            disk.delete()
        machine.delete()

    @staticmethod
    @celery.task(name='ovs.machine.delete_from_voldrv')
    def delete_from_voldrv(name):
        """
        This method will delete a vmachine based on the name of the vmx given
        """
        name = name.strip('/')
        if name.endswith('.vmx'):
            vm = VMachineList.get_by_devicename(name)
            if vm is not None:
                MessageController.fire(MessageController.Type.EVENT, {'type': 'vmachine_deleted',
                                                                      'metadata': {'name': vm.name}})
                vm.delete()

    @staticmethod
    @celery.task(name='ovs.machine.rename_from_voldrv')
    def rename_from_voldrv(old_name, new_name, vsrid):
        """
        This machine will handle the rename of a vmx file
        """
        old_name = old_name.strip('/')
        new_name = new_name.strip('/')
        # @TODO: When implementing more hypervisors, move part of code to hypervisor factory
        # vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
        # hypervisor = Factory.get(vsr.serving_vmachine.pmachine)
        # scenario = hypervisor.get_scenario(old_name, new_name)
        # if scenario == 'RENAME': f00bar
        # if scenario == 'UPDATED': f00bar
        # > This way, this piece of code is hypervisor agnostic
        if old_name.endswith('.vmx') and new_name.endswith('.vmx'):
            # Most likely a change from path. Updaing path
            vm = VMachineList.get_by_devicename(old_name)
            if vm is not None:
                vm.devicename = new_name
                vm.save()
        elif old_name.endswith('.vmx~') and new_name.endswith('.vmx'):
            vm = VMachineList.get_by_devicename(new_name)
            # The configuration has been updated (which happens in a tempfile), start a sync
            if vm is not None:
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
            raise RuntimeError('vMachine {0} may not be running to set it as vTemplate'.format(
                vmachine.name
            ))

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
    def snapshot(machineguid, label=None, is_consistent=False, timestamp=None):
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
                    'machineguid': machineguid}
        machine = VMachine(machineguid)

        # @todo: we now skip creating a snapshot when a vmachine's disks
        #        is missing a mandatory property: volumeid
        #        subtask will now raise an exception earlier in the workflow
        for disk in machine.vdisks:
            if not disk.volumeid:
                message = 'Missing volumeid on disk {0} - unable to create snapshot for vm {1}' \
                    .format(disk.guid, machine.guid)
                logging.info('Error: {0}'.format(message))
                raise RuntimeError(message)

        snapshots = {}
        success = True
        try:
            for disk in machine.vdisks:
                snapshots[disk.guid] = VDiskController.create_snapshot(diskguid=disk.guid,
                                                                       metadata=metadata)
        except:
            success = False
            for diskguid, snapshotid in snapshots.iteritems():
                VDiskController.delete_snapshot(diskguid=diskguid,
                                                snapshotid=snapshotid)
        logging.info('Create snapshot for vMachine {0}: {1}'.format(
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
        vmachine = VMachine(vmachineguid)
        if vsrid is None and vmachine.hypervisorid is not None and vmachine.pmachine is not None:
            # Only the vmachine was received, so base the sync on hypervisorid and pmachine
            hypervisor = Factory.get(vmachine.pmachine)
            logging.info('Syncing vMachine (name {})'.format(vmachine.name))
            vm_object = hypervisor.get_vm_agnostic_object(vmid=vmachine.hypervisorid)
        elif vsrid is not None and vmachine.devicename is not None:
            # VSR id was given, using the devicename instead (to allow hypervisorid updates
            # which can be caused by re-adding a vm to the inventory
            vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
            if vsr is None:
                raise RuntimeError('VolumeStorageRouter could not be found')
            vsa = vsr.serving_vmachine
            if vsa is None:
                raise RuntimeError('VolumeStorageRouter {} not linked to a VSA'.format(vsr.name))
            pmachine = vsa.pmachine
            if pmachine is None:
                raise RuntimeError('VSA {} not linked to a pMachine'.format(vsa.name))
            hypervisor = Factory.get(pmachine)
            vmachine.pmachine = pmachine
            vmachine.save()

            logging.info('Syncing vMachine (device {}, ip {}, mtpt {})'.format(vmachine.devicename,
                                                                               vsr.ip,
                                                                               vsr.mountpoint))
            vm_object = hypervisor.get_vm_object_by_devicename(devicename=vmachine.devicename,
                                                               ip=vsr.ip,
                                                               mountpoint=vsr.mountpoint)
        else:
            message = 'Not enough information to sync vmachine'
            logging.info('Error: {0}'.format(message))
            raise RuntimeError(message)

        vdisks_synced = 0
        if vm_object is None:
            message = 'Could not retreive hypervisor vmachine object'
            logging.info('Error: {0}'.format(message))
            raise RuntimeError(message)
        else:
            try:
                if vmachine.name is None:
                    MessageController.fire(MessageController.Type.EVENT,
                                           {'type': 'vmachine_created',
                                            'metadata': {'name': vm_object['name']}})
                elif vmachine.name != vm_object['name']:
                    MessageController.fire(MessageController.Type.EVENT,
                                           {'type': 'vmachine_renamed',
                                            'metadata': {'old_name': vmachine.name,
                                                         'new_name': vm_object['name']}})
                vmachine.name = vm_object['name']
                vmachine.hypervisorid = vm_object['id']
                vmachine.devicename = vm_object['backing']['filename']
                vmachine.save()
                # Updating and linking disks
                vdisk_guids = []
                for disk in vm_object['disks']:
                    vdisk = VDiskList.get_by_devicename(disk['filename'])
                    if vdisk is not None:
                        vsr = VolumeStorageRouterList.get_by_vsrid(vdisk.vsrid)
                        if vsr is None:
                            raise RuntimeError('vDisk without VSR found')
                        datastore = vm_object['datastores'][disk['datastore']]
                        if datastore == '{}:{}'.format(vsr.ip, vsr.mountpoint):
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

                logging.info('Syncing vMachine finished (name {}, {} vdisks (re)linked)'.format(
                    vmachine.name, vdisks_synced
                ))
            except Exception as ex:
                logging.info('Error during sync: {0}'.format(str(ex)))
                raise

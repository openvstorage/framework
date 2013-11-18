"""
VMachine module
"""
import time

from celery import group, chain
from ovs.celery import celery
from ovs.lib.vdisk import VDiskController
from ovs.dal.hybrids.vmachine import VMachine
from ovs.hypervisor.factory import Factory


class VMachineController(object):
    """
    Contains all BLL related to VMachines
    """
    @staticmethod
    @celery.task(name='ovs.machine.snapshot')
    def snapshot(machineguid, **kwargs):
        """
        Snapshot VMachine disks

        @param machineguid: guid of the machine
        """
        _ = kwargs
        machine = VMachine(machineguid)
        tasks = []
        for disk in machine.disks:
            t = VDiskController().create_snapshot.s(diskguid = disk.guid)
            t.link_error(VDiskController().delete_snapshot.s())
            tasks.append(t)
        snapshot_vmachine_wf = group(t for t in tasks)
        snapshot_vmachine_wf()

    @staticmethod
    @celery.task(name='ovs.machine.clone')
    def clone(machineguid, disks, name, **kwargs):
        """
        Clone a vmachine using the specified disks

        @param machineguid: guid of the machine to clone
        @param disks: dict with key/value pairs of diskguid/snapshotid (Will be ignored
        if cloning from template)
        @param name: name for the new machine
        """

        _ = kwargs
        machine = VMachine(machineguid)
        new_machine = VMachine()
        properties_to_clone = ['description', 'hvtype', 'cpu', 'memory', 'node']
        for item in properties_to_clone:
            setattr(new_machine, item, getattr(machine, item))
        new_machine.name = name
        new_machine.save()

        disk_tasks = []
        disks_by_order = sorted(machine.disks, key=lambda x: x.order)
        for currentDisk in disks_by_order:
            if machine.template and currentDisk.templatesnapshot:
                snapshotid = currentDisk.templatesnapshot
            else:
                snapshotid = disks[currentDisk.guid]
            prefix = '%s-clone'%currentDisk.name
            clone_task = VDiskController.clone.s(diskguid=currentDisk.guid,
                                                 snapshotid=snapshotid,
                                                 devicename=prefix,
                                                 location=new_machine.name,
                                                 machineguid=new_machine.guid)
            disk_tasks.append(clone_task)
        clone_disk_tasks = group(t for t in disk_tasks)
        group_result = clone_disk_tasks()
        while not group_result.ready():
            time.sleep(1)
        if group_result.successful():
            disks = group_result.join()
        else:
            for taskResult in group_result:
                if taskResult.successfull():
                    VDiskController.delete(diskguid = taskResult.get()['diskguid'])
            new_machine.delete()
            return None

        hv = Factory.get(machine.node)
        provision_machine_task = hv.clone_vm.s(hv, machine.vmid, name, disks, None, True)
        provision_machine_task.link_error(VMachineController.delete.s(machineguid = new_machine.guid))
        result = provision_machine_task()

        new_machine.vmid = result.get()
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

        hv = Factory.get(machine.node)
        delete_vmachine_task = hv.delete_vm.si(hv, machine.vmid, None, True)
        async_result = delete_vmachine_task()
        async_result.wait()
        if async_result.successful():
            for disk in machine.disks:
                disk.delete()
            machine.delete()

    @staticmethod
    @celery.task(name='ovs.machine.setAsTemplate')
    def set_as_template(machineguid, snapshots, **kwargs):
        """
        Set a vmachine as template

        @param machineguid: guid of the machine
        @param snapshots: dictionary of diskguids(key)/snapshotid(value)
        """
        _ = kwargs
        vmachine = VMachine(machineguid)
        if vmachine.template:
            return
        hv = Factory.get(vmachine.node)
        # Configure disks as Independent Non-persistent
        disks = map(lambda d: '[{0}] {1}/{2}'.format(d.vpool.name,
                                                     d.machine.name,
                                                     d.devicename), vmachine.disks)
        hv.set_as_template.s(hv, vmachine.vmid, disks, esxhost=None, wait=True)

        # Do some magic on the storage layer?
        # This is most likely required as extra security measure
        # Suppose the template is set back to a real machine it can be deleted from within vmware,
        # this should be blocked.
        # This might also require a storagerouter internal check to be implemented to discourage
        # the volumes from being deleted when clones were made from it.

        # Set template flag to True in our model
        # Save templatesnapshot to each relevant disk
        vmachine.template = True
        disks_not_in_template = []
        for disk in vmachine.disks:
            if disk.guid in snapshots.keys():
                disk.templatesnapshot = snapshots[disk.guid]
                disk.save()
            else:
                disks_not_in_template.append(disk.guid)
        vmachine.save()

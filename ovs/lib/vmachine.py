import time
import uuid
import logging

from celery import group, chain
from ovs.celery import celery
from ovs.lib.vdisk import VDiskController
from ovs.dal.hybrids.vmachine import VMachine
from ovs.hypervisor.factory import Factory

class VMachineController(object):

    @celery.task(name='ovs.machine.snapshot')
    def snapshot(*args, **kwargs):
        """
        Snapshot VMachine disks

        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = VMachine(machineguid)
        tasks = []
        for disk in machine.disks:
            t = VDiskController().createSnapshot.s(diskguid = disk.guid)
            t.link_error(VDiskController().deleteSnapshot.s())
            tasks.append(t)
        snapshot_vmachine_wf = group(t for t in tasks)
        snapshot_vmachine_wf()

    @celery.task(name='ovs.machine.clone')
    def clone(*args, **kwargs):
        """
        Clone a vmachine using the specified disks

        @param parentmachineguid: guid of the machine to clone
        @param disks: dict with key/value pairs of diskguid/snapshotid (Will be ignored if cloning from template)
        @param name: name for the new machine
        """
        machineguid = kwargs['parentmachineguid']
        disks = kwargs['disks']
        name = kwargs['name']

        machine = VMachine(machineguid)
        newMachine = VMachine()
        propertiesToClone = ['description', 'hvtype', 'cpu', 'memory', 'node']
        for property in propertiesToClone:
            setattr(newMachine, property, getattr(machine, property))
        newMachine.name = name
        newMachine.save()

        diskTasks = []
        diskClones = []
        disksByOrder = sorted(machine.disks, key=lambda x: x.order)
        for currentDisk in disksByOrder:
            if machine.template and currentDisk.templatesnapshot:
                snapshotid = currentDisk.templatesnapshot
            else:
                snapshotid = disks[currentDisk.guid]
            deviceNamePrefix = '%s-clone'%currentDisk.name
            cloneTask = VDiskController.clone.s(parentdiskguid=currentDisk.guid, snapshotid=snapshotid, devicename=deviceNamePrefix, location=newMachine.name, machineguid=newMachine.guid)
            diskTasks.append(cloneTask)
        clone_disk_tasks = group(t for t in diskTasks)
        groupResult = clone_disk_tasks()
        while not groupResult.ready():
            time.sleep(1)
        if groupResult.successful():
            disks = groupResult.join()
        else:
            for taskResult in groupResult:
                if taskResult.successfull():
                    VDiskController.delete(diskguid = taskResult.get()['diskguid'])
            newMachine.delete()
            return None

        hv = Factory.get(machine.node)
        provision_machine_task = hv.clone_vm.s(hv, machine.vmid, name, disks, None, True)
        provision_machine_task.link_error(VMachineController.delete.s(machineguid = newMachine.guid))
        result = provision_machine_task()

        newMachine.vmid = result.get()
        newMachine.save()
        return newMachine.guid

    @celery.task(name='ovs.machine.delete')
    def delete(*args, **kwargs):
        """
        Delete a vmachine

        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = VMachine(machineguid)

        diskTasks = []
        hv = Factory.get(machine.node)
        delete_vmachine_task = hv.delete_vm.si(hv, machine.vmid, None, True)
        asyncResult = delete_vmachine_task()
        asyncResult.wait()
        if asyncResult.successful():
            for disk in machine.disks:
                disk.delete()
            machine.delete()

    @celery.task(name='ovs.machine.setAsTemplate')
    def setAsTemplate(*args, **kwargs):
        """
        Set a vmachine as template

        @param machineguid: guid of the machine
        @param snapshots: dictionary of diskguids(key)/snapshotid(value)
        """
        machineguid = kwargs['machineguid']
        snapshots = kwargs['snapshots']
        vmachine = VMachine(machineguid)

        if vmachine.template:
            return
        hv = Factory.get(vmachine.node)
        #Configure disks as Independent Non-persistent
        disks = map(lambda d: '[{0}] {1}/{2}'.format(d.vpool.name, d.machine.name, devicename),vmachine.disks)
        hv.set_as_template.s(hv, vmachine.vmid, disks, esxhost=None, wait=True)

        """
        Do some magic on the storage layer?
        This is most likely required as extra security measure
        Suppose the template is set back to a real machine it can be deleted from within vmware, this should be blocked.
        This might also require a storagerouter internal check to be implemented to discourage the volumes from being deleted when clones were made from it.
        """

        #Set template flag to True in our model
        #Save templatesnapshot to each relevant disk
        vmachine.template = True
        disksNotInTemplate = []
        for disk in vmachine.disks:
            if disk.guid in snapshots.keys():
                disk.templatesnapshot = snapshots[disk.guid]
                disk.save()
            else:
                disksNotInTemplate.append(disk.guid)
        vmachine.save()

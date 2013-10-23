import time
import uuid
import logging

from celery import group, chain
from ovs.celery import celery
from ovs.lib.vdisk import VDiskController
from ovs.lib.dummy import DummyController
from ovs.dal.hybrids.vdisk import vDisk
from ovs.dal.hybrids.vmachine import vMachine
from ovs.hypervisor.factory import Factory

class VMachineController(object):

    @celery.task(name='ovs.machine.snapshot')
    def snapshot(*args, **kwargs):
        """
        Snapshot vMachine disks

        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = vMachine(machineguid)
        tasks = []
        for disk in machine.disks:
            t = VDiskController().createSnapshot.s({'diskguid': disk.guid})
            t.link_error(VDiskController().deleteSnapshot.s())
            tasks.append(t)
        snapshot_vmachine_wf = group(t for t in tasks)
        return snapshot_vmachine_wf

    @celery.task(name='ovs.machine.clone')
    def clone(*args, **kwargs):
        """
        Clone a vmachine using the specified disks

        @param parentmachineguid: guid of the machine to clone
        @param disks: dict with key/value pairs of diskguid/snapshotid
        @param name: name for the new machine
        
        @todo: Do we want the framework clone to reflect to a real vmware clone?
        """
        machineguid = kwargs['parentmachineguid']
        disks = kwargs['disks']
        name = kwargs['name']

        machine = vMachine(machineguid)
        newMachine = vMachine()
        propertiesToClone = ['description', 'hvtype', 'cpu', 'memory', 'vmid']
        for property in propertiesToClone:
            setattr(newMachine, property, getattr(machine, property))
        newMachine.name = name
        newMachine.save()
        
        diskTasks = []
        diskClones = []
        disksByOrder = sorted(machine.disks, key=lambda x: x.order)
        for currentDisk in disksByOrder:
            snapshotid = disks[currentDisk.guid]
            devicename = '%s-clone.vmdk'%currentDisk.name
            flatdevicename = '%s-clone-flat.vmdk'%currentDisk.name
            #t = DummyController().echo.s('De zoveelsten disk')
            t = VDiskController().clone.s(parentdiskguid=currentDisk.guid, snapshotid=snapshotid, devicename=flatdevicename, location=newMachine.name, machineguid=newMachine.guid)
            diskTasks.append(t)
            diskClones.append({'name': currentDisk.name, 'backingdevice': '{0}/{1}'.format(newMachine.name, devicename)})
        clone_disk_tasks = group(t for t in diskTasks)
        
        hv = Factory.get(machine.node)
        provision_machine_task = hv.cloneVM.si(hv, machine.vmid, name, diskClones, None, True)
        provision_machine_task.link_error(VMachineController.delete.s(machineguid = newMachine.guid).apply_async())
        clone_vmachine_wf = chain(clone_disk_tasks, provision_machine_task)
        return clone_vmachine_wf

    @celery.task(name='ovs.machine.delete')
    def delete(*args, **kwargs):
        """
        Delete a vmachine

        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = vMachine(machineguid)

        diskTasks = []
        for disk in machine.disks:
            t = VDiskController().delete.s({'diskguid': disk.guid})
            diskTasks.append(t)
        if diskTasks:
            delete_disk_wf = group(t for t in diskTasks)
            return delete_disk_wf
        return None

    @celery.task(name='ovs.machine.remove')
    def remove(*args, **kwargs):
        """
        Remove a vmachine from hypervisor

        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = vMachine(machineguid)
        hv = Factory.get(machine.node)
        hv.remove(machine.vmid)

import time
import uuid
import logging

from celery import group
from ovs.celery import celery
from ovs.lib.vdisk import vdisk
from ovs.dal.hybrids.vmachine import vMachine
from ovs.hypervisor.factory import hvFactory

class vMachine(object):
    @celery.task(name='ovs.machine.provision')
    def provision(self, *args, **kwargs):
        """
        Provision a machine on the hypervisor
        
        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = vMachine(machineguid)
        hv = hvFactory.get(machine.node)
        hv.provision(machine.vmid)
    
    @celery.task(name='ovs.machine.snapshot')
    def snapshot(self, *args, **kwargs):
        """
        Snapshot vMachine disks
        
        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = vMachine(machineduid)
        tasks = []
        for disk in machine.disks:
            t = vdisk().createSnapshot.s({'diskguid': disk.guid})
            t.link_error(vdisk().deleteSnapshot.s())
            tasks.append(t)
        snapshot_vmachine_wf = group(t for t in tasks)
        return snapshot_vmachine_wf

    @celery.task(name='ovs.machine.clone')
    def clone(self, *args, **kwargs):
        """
        Clone a vmachine using the specified disks
        
        @param machineguid: guid of the machine to clone
        @param disks: dict with key/value pairs of disk/snapshot
        @param name: name for the new machine
        """
        machineguid = kwargs['parentmachineguid']
        disks = kwargs['disks']
        name = kwargs['name']

        machine = vMachine(machineguid)
        newMachine = vMachine()
        propertiesToClone = ['description', 'hvtype', 'cpu', 'memory', 'hypervisorid']
        for property in propertiesToClone:
            setattr(newMachine, property, getattr(machine, property))
        newMachine.name = name
        newMachine.save()
        diskTasks = []
        
        for disk in disks:
            t = vdisk().clone.s({'parentdiskguid': disk['diskguid'], 'snapshotguid': disk['snapshotguid'], 'devicepath': devicepath, 'machineguid': newMachine.guid})
            diskTasks.append(t)
        clone_disk_tasks = group(t for t in diskTaks)
        provision_machine_task = self.provision.s({'machineguid': newMachine.guid})
        provision_machine_task.link_error(self.delete.s({'machineguid': newMachine.guid}))
        clone_vmachine_wf = chain(clone_disk_tasks, provision_machine_task)
        return clone_vmachine_wf

    @celery.task(name='ovs.machine.delete')
    def delete(self, *args, **kwargs):
        """
        Delete a vmachine
        
        @param machineguid: guid of the machine
        """
        machineguid = kwargs['parentmachineguid']
        machine = vMachine(machineguid)
        
        diskTasks = []
        for disk in machine.disks:
            t = vdisk().delete.s({'diskguid': disk.guid})
            diskTasks.append(t)
        delete_disk_tasks = group(t for t in diskTasks)
        
    @celery.task(name='ovs.machine.remove')
    def remove(self, *args, **kwargs):
        """
        Remove a vmachine from hypervisor
        
        @param machineguid: guid of the machine
        """
        machineguid = kwargs['machineguid']
        machine = vMachine(machineguid)
        hv = hvFactory.get(machine.node)
        hv.remove(machine.vmid)

from celery import group, chain
from ovs.celery import celery
from ovs.lib.vdisk import VDiskController
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.celery import loghandler


class ScheduledTaskController(object):
    @celery.task(name='ovs.scheduled.snapshotall')
    def snapshot_all_vms(*args, **kwargs):
        """
        Snapshots all VMachines
        """
        print '[SSA] started'
        loghandler.logger.info('[SSA] started')
        tasks = []
        machines = VMachineList.get_vmachines()
        for machine in machines:
            for disk in machine.disks:
                task = VDiskController().createSnapshot.s(diskguid=disk.guid)
                task.link_error(VDiskController().deleteSnapshot.s())
                tasks.append(task)
        workflow = group(task for task in tasks)
        print '[SSA] %d disk snapshots launched' % len(tasks)
        loghandler.logger.info('[SSA] %d disk snapshots launched' % len(tasks))
        return workflow()

    @celery.task(name='ovs.scheduled.dummy')
    def dummy(*args, **kwargs):
        print '[DUMMY]'
        loghandler.logger.info('[DUMMY]')
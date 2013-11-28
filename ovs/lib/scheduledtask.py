# license see http://www.openvstorage.com/licenses/opensource/

"""
ScheduledTaskController module
"""

from celery import group, chain
from ovs.celery import celery
from ovs.lib.vdisk import VDiskController
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.celery import loghandler

import datetime


class ScheduledTaskController(object):

    """
    This controller contains all scheduled task code. These tasks can be
    executed at certain intervals and should be self-containing
    """

    @celery.task(name='ovs.scheduled.snapshotall')
    def snapshot_all_vms(*args, **kwargs):
        """
        Snapshots all VMachines
        """

        _ = (args, kwargs)
        loghandler.logger.info('[SSA] started')
        tasks = []
        machines = VMachineList.get_vmachines()
        for machine in machines:
            timestamp = str(datetime.datetime.now()).split('.')[0]
            for disk in machine.vdisks:
                metadata = dict()
                metadata['label'] = ''
                metadata['timestamp'] = timestamp
                metadata['machineguid'] = machine.guid
                task = VDiskController.create_snapshot.s(diskguid=disk.guid,
                                                         metadata=metadata)
                task.link_error(VDiskController.delete_snapshot.s())
                tasks.append(task)
        workflow = group(task for task in tasks)
        loghandler.logger.info('[SSA] %d disk snapshots launched'
                               % len(tasks))
        return workflow()

    @celery.task(name='ovs.scheduled.dummy')
    def dummy(*args, **kwargs):
        """
        Dummy handler to test scheduler working
        """

        _ = (args, kwargs)
        loghandler.logger.info('[DUMMY]')

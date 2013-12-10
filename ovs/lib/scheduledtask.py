# license see http://www.openvstorage.com/licenses/opensource/

"""
ScheduledTaskController module
"""

from celery import group, chain
from ovs.celery import celery
from ovs.lib.vmachine import VMachineController
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.celery import loghandler


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
            tasks.append(VMachineController.snapshot.s(machineguid=machine.guid,
                                                       label='',
                                                       is_consistent=False))
        workflow = group(task for task in tasks)
        loghandler.logger.info('[SSA] %d disk snapshots launched' % len(tasks))
        return workflow()

    @celery.task(name='ovs.scheduled.dummy')
    def dummy(*args, **kwargs):
        """
        Dummy handler to test scheduler working
        """

        _ = (args, kwargs)
        loghandler.logger.info('[DUMMY]')

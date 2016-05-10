# Copyright 2016 iNuron NV
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
Module containing celery helpers
"""

import datetime
import time
from celery.task.control import revoke
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.service import ServiceManager
from ovs.log.logHandler import LogHandler


class CeleryToolbox():
    """
    Generic class for various celery helpers
    """

    @staticmethod
    def manage_running_tasks(tasklist, timesleep=10):
        """
        Manage a list of running celery task
        - discard PENDING tasks after a certain timeout
        - validate RUNNING tasks are actually running
        :param tasklist: Dictionary of tasks to wait {IP address: AsyncResult}
        :type tasklist: dict
        :param timesleep: leep between checks -
          -for long running tasks it's better to sleep for a longer period of time to reduce number of ssh calls
        :type timesleep: int
        :return: results
        :rtype: dict
        """
        logger = LogHandler.get('lib', name='celery toolbox')
        ssh_clients = {}
        tasks_pending = {}
        tasks_pending_timeout = 1800  # 30 minutes
        results = {}
        failed_nodes = []
        while len(tasklist.keys()) > 0:
            for ip, task in tasklist.items():
                if task.state in ('SUCCESS', 'FAILURE'):
                    logger.info('Task {0} finished: {1}'.format(task.id, task.state))
                    results[ip] = task.get(propagate=False)
                    del tasklist[ip]
                elif task.state == 'PENDING':
                    if task.id not in tasks_pending:
                        tasks_pending[task.id] = time.time()
                    else:
                        task_pending_since = tasks_pending[task.id]
                        if time.time() - task_pending_since > tasks_pending_timeout:
                            logger.warning('Task {0} is pending since {1} on node {2}. Task will be revoked'.format(task.id, datetime.datetime.fromtimestamp(task_pending_since), ip))
                            revoke(task.id)
                            del tasklist[ip]
                            del tasks_pending[task.id]
                            failed_nodes.append(ip)
                elif task.state == 'STARTED':
                    if ip not in ssh_clients:
                        ssh_clients[ip] = SSHClient(ip, username='root')
                    client = ssh_clients[ip]
                    if ServiceManager.get_service_status('workers', client) is False:
                        logger.error('Service ovs-workers on node {0} appears halted while there is a task PENDING for it {1}. Task will be revoked.'.format(ip, task.id))
                        revoke(task.id)
                        del tasklist[ip]
                        failed_nodes.append(ip)
                    else:
                        ping_result = task.app.control.inspect().ping()
                        storage_router = StorageRouterList.get_by_ip(ip)
                        if "celery@{0}".format(storage_router.name) not in ping_result:
                            logger.error('Service ovs-workers on node {0} is not reachable via rabbitmq while there is a task STARTED for it {1}. Task will be revoked.'.format(ip, task.id))
                            revoke(task.id)
                            del tasklist[ip]
                            failed_nodes.append(ip)
            if len(tasklist.keys()) > 0:
                time.sleep(timesleep)
        return results, failed_nodes

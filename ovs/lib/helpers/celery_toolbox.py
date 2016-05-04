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
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.service import ServiceManager
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='celery toolbox')


def manage_running_tasks(tasklist, timesleep=10):
    """
    Manage a list of running celery task
    - discard PENDING tasks after a certain timeout
    - validate RUNNING tasks are actually running
    @param tasklist: dict {IP address: AsyncResult}
    @param timesleep: int (sleep between checks -
      -for long running tasks it's better to sleep for a longer period of time to reduce number of ssh calls
    @return: list of results
    """
    ssh_clients = {}
    results = []
    while len(tasklist.keys()) > 0:
        for ip, task in tasklist.items():
            if task.state in ('SUCCESS', 'FAILURE'):
                logger.info('Task {0} finished: {1}'.format(task.id, task.state))
                results.append(task.get(propagate=False))
                del tasklist[ip]
            elif task.state in ('PENDING', 'STARTED'):
                if ip not in ssh_clients:
                    ssh_clients[ip] = SSHClient(ip, username='root')
                client = ssh_clients[ip]
                if ServiceManager.get_service_status('workers', client) is False:
                    logger.error('Service ovs-workers on node {0} appears halted while there is a task {1} for it {2}. Task will be revoked.'.format(ip, task.state, task.id))
                    revoke(task.id)
                    del tasklist[ip]
        time.sleep(timesleep)
    return results

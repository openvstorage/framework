# Copyright 2014 CloudFounders NV
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

import time
from subprocess import check_output
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.system import Ovs

current_time = int(time.time())
machine_id = Ovs.get_my_machine_id()
worker_states = check_output("/usr/local/bin/celery inspect ping 2> /dev/null | grep OK | perl -pe 's/\x1b\[[0-9;]*m//g' || true", shell=True)
routers = StorageRouterList.get_storagerouters()
for node in routers:
    if node.heartbeats is None:
        node.heartbeats = {}
    if 'celery@{0}: OK'.format(node.name) in worker_states:
        node.heartbeats['celery'] = current_time
    if node.machine_id == machine_id:
        node.heartbeats['process'] = current_time
    node.save()

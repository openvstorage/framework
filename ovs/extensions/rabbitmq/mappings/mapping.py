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

import volumedriver.storagerouter.EventMessages_pb2 as EventMessages
from ovs.lib.vdisk import VDiskController
from ovs.lib.vmachine import VMachineController
from ovs.lib.vpool import VPoolController
from ovs.lib.storagerouter import StorageRouterController


class Mapping(object):
    """
    Mapping container
    """
    mapping = {EventMessages.EventMessage.VolumeDelete: [{'property': 'volume_delete',
                                                          'task': VDiskController.delete_from_voldrv,
                                                          'arguments': {'name': 'volumename'}}],
               EventMessages.EventMessage.VolumeResize: [{'property': 'volume_resize',
                                                          'task': VDiskController.resize_from_voldrv,
                                                          'arguments': {'name': 'volumename',
                                                                        'size': 'volumesize',
                                                                        'path': 'volumepath',
                                                                        '[NODE_ID]': 'storagerouter_id'}}],
               EventMessages.EventMessage.VolumeRename: [{'property': 'volume_rename',
                                                          'task': VDiskController.rename_from_voldrv,
                                                          'arguments': {'name': 'volumename',
                                                                        'old_path': 'volume_old_path',
                                                                        'new_path': 'volume_new_path',
                                                                        '[NODE_ID]': 'storagerouter_id'}}],
               EventMessages.EventMessage.FileCreate: [{'property': 'file_create',
                                                        'task': VMachineController.update_from_voldrv,
                                                        'arguments': {'path': 'name',
                                                                      '[NODE_ID]': 'storagerouter_id'},
                                                        'options': {'delay': 3,
                                                                    'dedupe': True,
                                                                    'dedupe_key': '[TASK_NAME]_[name]_[storagerouter_id]',
                                                                    'execonstorageappliance': True}}],
               EventMessages.EventMessage.FileWrite: [{'property': 'file_write',
                                                       'task': VMachineController.update_from_voldrv,
                                                       'arguments': {'path': 'name',
                                                                     '[NODE_ID]': 'storagerouter_id'},
                                                       'options': {'delay': 3,
                                                                   'dedupe': True,
                                                                   'dedupe_key': '[TASK_NAME]_[name]_[storagerouter_id]',
                                                                    'execonstorageappliance': True}}],
               EventMessages.EventMessage.FileDelete: [{'property': 'file_delete',
                                                        'task': VMachineController.delete_from_voldrv,
                                                        'arguments': {'path': 'name',
                                                                      '[NODE_ID]': 'storagerouter_id'}}],
               EventMessages.EventMessage.FileRename: [{'property': 'file_rename',
                                                        'task': VMachineController.rename_from_voldrv,
                                                        'arguments': {'old_path': 'old_name',
                                                                      'new_path': 'new_name',
                                                                      '[NODE_ID]': 'storagerouter_id'},
                                                        'options': {'delay': 3,
                                                                    'dedupe': True,
                                                                    'dedupe_key': '[TASK_NAME]_[new_name]_[storagerouter_id]',
                                                                    'execonstorageappliance': True}}],
               EventMessages.EventMessage.UpAndRunning: [{'property': 'up_and_running',
                                                          'task': VPoolController.mountpoint_available_from_voldrv,
                                                          'arguments': {'mountpoint': 'mountpoint',
                                                                        '[NODE_ID]': 'storagerouter_id'},
                                                          'options': {'execonstorageappliance': True}}],
               EventMessages.EventMessage.RedirectTimeoutWhileOnline: [{'property': 'redirect_timeout_while_online',
                                                                        'task': StorageRouterController.update_status,
                                                                        'arguments': {'remote_node_id': 'storagerouter_id'},
                                                                        'options': {'dedupe': True,
                                                                                    'dedupe_key': '[TASK_NAME]_[storagerouter_id]'}}]}

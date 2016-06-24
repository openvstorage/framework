# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

import volumedriver.storagerouter.FileSystemEvents_pb2 as FileSystemEvents
import volumedriver.storagerouter.VolumeDriverEvents_pb2 as VolumeDriverEvents
from ovs.lib.vdisk import VDiskController
from ovs.lib.vmachine import VMachineController
from ovs.lib.vpool import VPoolController
from ovs.lib.storagedriver import StorageDriverController


class Mapping(object):
    """
    Mapping container
    """
    mapping = {FileSystemEvents.volume_delete: [{'task': VDiskController.delete_from_voldrv,
                                                 'arguments': {'name': 'volumename',
                                                               '[NODE_ID]': 'storagedriver_id'}}],
               FileSystemEvents.volume_resize: [{'task': VDiskController.resize_from_voldrv,
                                                 'arguments': {'name': 'volumename',
                                                               'size': 'volumesize',
                                                               'path': 'volumepath',
                                                               '[NODE_ID]': 'storagedriver_id'}}],
               FileSystemEvents.volume_rename: [{'task': VDiskController.rename_from_voldrv,
                                                 'arguments': {'name': 'volumename',
                                                               'old_path': 'volume_old_path',
                                                               'new_path': 'volume_new_path',
                                                               '[NODE_ID]': 'storagedriver_id'}}],
               FileSystemEvents.up_and_running: [{'task': VPoolController.up_and_running,
                                                  'arguments': {'mountpoint': 'mountpoint',
                                                                '[NODE_ID]': 'storagedriver_id'},
                                                  'options': {'execonstoragerouter': True}}],
               FileSystemEvents.owner_changed: [{'task': VDiskController.migrate_from_voldrv,
                                                 'arguments': {'name': 'volume_id',
                                                               'new_owner_id': 'new_owner_id'}}],
               FileSystemEvents.redirect_timeout_while_online: [{'task': StorageDriverController.update_status,
                                                                 'arguments': {'remote_node_id': 'storagedriver_id'},
                                                                 'options': {'dedupe': True,
                                                                             'dedupe_key': '[TASK_NAME]_[storagedriver_id]'}}],
               VolumeDriverEvents.volumedriver_error: [{'task': StorageDriverController.volumedriver_error,
                                                        'arguments': {'code': 'code',
                                                                      'volume_name': 'volumename'}}],
               VolumeDriverEvents.dtl_state_transition: [{'task': VDiskController.dtl_state_transition,
                                                          'arguments': {'volume_name': 'volume_name',
                                                                        'old_state': 'old_state',
                                                                        'new_state': 'new_state',
                                                                        '[NODE_ID]': 'storagedriver_id'}}]}

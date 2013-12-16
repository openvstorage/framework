# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the process method for processing rabbitmq messages
"""

from celery.task.control import revoke
from ovs.lib.vdisk import VDiskController
from ovs.lib.vmachine import VMachineController
from ovs.extensions.storage.volatilefactory import VolatileFactory


def process(queue, body):
    """
    Processes the actual received body
    """
    if queue == 'storagerouter':
        import json
        import volumedriver.storagerouter.EventMessages_pb2 as EventMessages
        cache = VolatileFactory.get_client()

        data = EventMessages.EventMessage().FromString(body)

        mapping = {EventMessages.EventMessage.VolumeCreate:               # Disk create
                       {'property': 'volume_create',
                        'task': VDiskController.create_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'size': 'volumesize',
                                      'path': 'volumepath',
                                      '[NODE_ID]': 'vsrid'}},
                   EventMessages.EventMessage.VolumeDelete:
                       {'property': 'volume_delete',
                        'task': VDiskController.delete_from_voldrv,
                        'arguments': {'name': 'volumename'}},
                   EventMessages.EventMessage.VolumeResize:
                       {'property': 'volume_resize',
                        'task': VDiskController.resize_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'size': 'volumesize'}},
                   EventMessages.EventMessage.VolumeRename:
                       {'property': 'volume_rename',
                        'task': VDiskController.rename_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'old_path': 'volume_old_path',
                                      'new_path': 'volume_new_path'}},
                   EventMessages.EventMessage.FileCreate:                 # Machine create
                       {'property': 'file_create',
                        'task': VMachineController.create_from_voldrv,
                        'arguments': {'path': 'name'}},
                   EventMessages.EventMessage.FileDelete:
                       {'property': 'file_delete',
                        'task': VMachineController.delete_from_voldrv,
                        'arguments': {'path': 'name'}},
                   EventMessages.EventMessage.FileRename:
                       {'property': 'file_rename',
                        'task': VMachineController.rename_from_voldrv,
                        'arguments': {'old_path': 'old_name',
                                      'new_path': 'new_name',
                                      '[NODE_ID]': 'vsrid'},
                        'options': {'delay': 5,
                                    'dedupe': True,
                                    'dedupe_key': 'new_name'}}}

        if data.type in mapping:
            task = mapping[data.type]['task']
            data_container = getattr(data, mapping[data.type]['property'])
            kwargs = {}
            delay = 0
            for field, target in mapping[data.type]['arguments'].iteritems():
                if field == '[NODE_ID]':
                    kwargs[target] = data.node_id
                elif field == '[CLUSTER_ID]':
                    kwargs[target] = data.cluster_id
                else:
                    kwargs[target] = getattr(data_container, field)
            if 'options' in mapping[data.type]:
                delay = mapping[data.type]['options'].get('delay', 0)
                dedupe = mapping[data.type]['options'].get('dedupe', False)
                dedupe_key = mapping[data.type]['options'].get('dedupe_key', None)
                if dedupe and dedupe_key:  # We can't dedupe without a key
                    key = '{}({})'.format(task.__class__.__name__, kwargs[dedupe_key])
                    task_id = cache.get(key)
                    if task_id:
                        # Key exists, task was already scheduled
                        # If task is already running, the revoke message will be ignored
                        revoke(task_id)
                    async_result = task.s(**kwargs).apply_async(countdown=delay)
                    cache.set(key, async_result.id)  # Store the task id
                else:
                    task.s(**kwargs).apply_async(countdown=delay)
            else:
                task.delay(**kwargs)
            print '[{}] mapped {} to {} with args {}. Delay: {}s'.format(queue,
                                                                         str(data.type),
                                                                         task.__name__,
                                                                         json.dumps(kwargs),
                                                                         delay)
        else:
            raise RuntimeError('Type %s is not yet supported' % str(data.type))
    else:
        raise NotImplementedError('Queue {} is not yet implemented'.format(queue))

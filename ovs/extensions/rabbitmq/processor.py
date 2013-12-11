# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the process method for processing rabbitmq messages
"""

from ovs.lib.vdisk import VDiskController
from ovs.lib.vmachine import VMachineController
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.extensions.storage.volatilefactory import VolatileFactory


def process(queue, body):
    """
    Processes the actual received body
    """
    if queue == 'storagerouter':
        import json
        import volumedriver.storagerouter.EventMessages_pb2 as EventMessages
        cache = VolatileFactory.get_client()
        mutex = VolatileMutex('voldrv_processor')

        data = EventMessages.EventMessage().FromString(body)

        mapping = {EventMessages.EventMessage.VolumeCreate:
                       {'property': 'volume_create',
                        'task': VDiskController.create_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'size': 'volumesize',
                                      'path': 'volumepath'},
                        'options': {'delay': 10}},
                   EventMessages.EventMessage.VolumeDelete:
                       {'property': 'volume_delete',
                        'task': VDiskController.delete_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'path': 'volumepath'}},
                   EventMessages.EventMessage.VolumeResize:
                       {'property': 'volume_resize',
                        'task': VDiskController.resize_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'size': 'volumesize',
                                      'path': 'volumepath'}},
                   EventMessages.EventMessage.VolumeRename:
                       {'property': 'volume_rename',
                        'task': VDiskController.rename_from_voldrv,
                        'arguments': {'name': 'volumename',
                                      'old_path': 'volume_old_path',
                                      'new_path': 'volume_new_path'}},
                   EventMessages.EventMessage.MachineCreate:
                       {'property': 'machine_create',
                        'task': VMachineController.create_from_voldrv,
                        'arguments': {'name': 'name'}},
                   EventMessages.EventMessage.MachineUpdate:
                       {'property': 'machine_update',
                        'task': VMachineController.update_from_voldrv,
                        'arguments': {'name': 'name'},
                        'options': {'dedupe': True,
                                    'delay': 10}},
                   EventMessages.EventMessage.MachineDelete:
                       {'property': 'machine_delete',
                        'task': VMachineController.delete_from_voldrv,
                        'arguments': {'name': 'name'}},
                   EventMessages.EventMessage.MachineRename:
                       {'property': 'machine_rename',
                        'task': VMachineController.rename_from_voldrv,
                        'arguments': {'old_name': 'old_name',
                                      'new_name': 'new_name'}}}

        if data.type in mapping:
            task = mapping[data.type]['task']
            data_container = getattr(data, mapping[data.type]['property'])
            kwargs = {}
            for field, target in mapping[data.type]['arguments'].iteritems():
                kwargs[target] = getattr(data_container, field)
            if 'options' in mapping[data.type]:
                delay = mapping[data.type]['options'].get('delay')
                dedupe = mapping[data.type]['options'].get('dedupe', False)
                if dedupe:
                    # Do some deduping, we might use "cache" and "mutex"
                    pass
                if delay:
                    task.s(**kwargs).apply_async(countdown=delay)
            else:
                task.delay(**kwargs)
            print '[{}] mapped {} to {} with args {}'.format(queue,
                                                             str(data.type),
                                                             task.__name__,
                                                             json.dumps(kwargs))
        else:
            raise RuntimeError('Type %s is not yet supported' % str(data.type))
    else:
        raise NotImplementedError('Queue {} is not yet implemented'.format(queue))

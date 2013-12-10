# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the process method for processing rabbitmq messages
"""

from ovs.lib.vdisk import VDiskController
from ovs.lib.vmachine import VMachineController


def process(queue, body):
    """
    Processes the actual received body
    """
    if queue == 'storagerouter':
        import volumedriver.storagerouter.EventMessages_pb2 as EventMessages
        volumedrivermessage = EventMessages.EventMessage()
        data = volumedrivermessage.FromString(body)

        if data.type == EventMessages.EventMessage.VolumeCreate:
            _name = data.volume_create.name
            _size = data.volume_create.size
            _path = data.volume_create.path

            VDiskController.create_from_voldrv.s(_path, _name, _size).apply_async()

            print '[{}] Created Volume {} Size {} location {}'.format(queue, _name, _size, _path)

        elif data.type == EventMessages.EventMessage.VolumeDelete:
            _name = data.volume_delete.name
            _path = data.volume_delete.path

            VDiskController.delete_from_voldrv.s(_path, _name).apply_async()

            print '[{}] Deleted Volume {} location {}'.format(queue, _name, _path)

        elif data.type == EventMessages.EventMessage.VolumeResize:
            _name = data.volume_resize.name
            _size = data.volume_resize.size
            _path = data.volume_resize.path

            VDiskController.resize_from_voldrv.s(_path, _name, _size).apply_async()

            print '[{}] Resized Volume {} location {} to size {}'.format(queue, _name, _path, _size)

        elif data.type == EventMessages.EventMessage.VolumeRename:
            _name = data.volume_rename.name
            _old_path = data.volume_rename.old_path
            _new_path = data.volume_rename.new_path

            VDiskController.rename_from_voldrv.s(_name, _old_path, _new_path).apply_async()

            print '[{}] Renamed Volume {} from location {} to location {}'.format(queue, _name, _old_path, _new_path)

        elif data.type == EventMessages.EventMessage.MachineCreate:
            _name = data.machine_create.name

            VMachineController.create_from_voldrv.s(_name).apply_async()

            print '[{}] Created Machine {}'.format(queue, _name)

        elif data.type == EventMessages.EventMessage.MachineUpdate:
            _name = data.machine_update.name

            VMachineController.update_from_voldrv.s(_name).apply_async()

            print '[{}] Update Machine {}'.format(queue, _name)
        else:
            raise RuntimeError('Invalid task specified')
    else:
        raise NotImplementedError('Queue {} is not yet implemented'.format(queue))

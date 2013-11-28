# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the process method for processing rabbitmq messages
"""

from ovs.lib.vdisk import VDiskController

def process(queue, body):
    """
    Processes the actual received body
    """
    if queue == 'voldrv_queue':
        import volumedriver.storagerouter.EventMessages_pb2 as EventMessages
        vdmsg = EventMessages.EventMessage()
        data = vdmsg.FromString(body)
        
        if data.type == EventMessages.EventMessage.VolumeCreate:
            _name = data.volume_create.name
            _size = data.volume_create.size
            _path = data.volume_create.path

            VDiskController._create(_path, _name, _size)

            print '[voldrv_queue] Created Volume {} Size {} location {}'.format(_name, _size, _path) 

        elif data.type == EventMessages.EventMessage.VolumeDelete:
            _name = data.volume_delete.name
            _path = data.volume_delete.path

            VDiskController._delete(_path, _name)

            print '[voldrv_queue] Deleted Volume {} location {}'.format(_name, _path) 

        elif data.type == EventMessages.EventMessage.VolumeResize:
            _name = data.volume_resize.name 
            _size = data.volume_resize.size
            _path = data.volume_resize.path

            VDiskController.resize(_path, _name, _size)

            print '[voldrv_queue] Resized Volume {} location {} to size {}'.format(_name, _path, _size)

        elif data.type == EventMessages.EventMessage.VolumeRename:
            _name = data.volume_rename.name
            _old_path = data.volume_rename.old_path
            _new_path = data.volume_rename.new_path

            VDiskController.rename(_name, _old_path, _new_path)

            print '[voldrv_queue] Renamed Volume {} from location {} to location {}'.format(_name, _old_path, _new_path)
        else:
            raise RuntimeError('Invalid task specified')
    else:
        raise NotImplementedError('Queue {} is not yet implemented'.format(queue))

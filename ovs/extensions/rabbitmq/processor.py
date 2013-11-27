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
        print "\ntype:{}\nstring:{}\ntypeofdata:{}\nfields:\n{}\n".format(vdmsg.type, data, type(data), data.ListFields())
        
        if isinstance(data, vdmsg.VolumeCreate):
            _name = data.volume_create.name
            _size = data.volume_create.size
            _path = data.volume_create.path

            VDiskController._create(_path, _name, _size)

            print '[voldrv_queue] Created Volume {} Size {} location {}'.format(_name, _size, _path) 
        else:
            raise RuntimeError('Invalid task specified')
    else:
        raise NotImplementedError('Queue {} is not yet implemented'.format(queue))

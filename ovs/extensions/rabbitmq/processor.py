# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the process method for processing rabbitmq messages
"""
import json


def process(queue, body):
    """
    Processes the actual received body
    """
    if queue == 'voldrv_queue':
        data = json.loads(body)
        if 'task' not in data or 'data' not in data:
            raise RuntimeError('Invalid data received')

        if data['task'] == 'ovs.volume.created':
            print '... creating volume with data: %s' % data['data']
        else:
            raise RuntimeError('Invalid task specified')
    else:
        raise NotImplementedError('Queue %s is not yet implemented' % queue)

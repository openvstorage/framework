# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains the process method for processing rabbitmq messages
"""

import inspect
import json
from celery.task.control import revoke
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.generic.configuration import Configuration
import volumedriver.storagerouter.FileSystemEvents_pb2 as FileSystemEvents
import volumedriver.storagerouter.VolumeDriverEvents_pb2 as VolumeDriverEvents
from google.protobuf.descriptor import FieldDescriptor
from ovs.log.logHandler import LogHandler
from ovs.lib.vmachine import VMachineController
from ovs.lib.vdisk import VDiskController

logger = LogHandler.get('extensions', name='processor')

CINDER_VOLUME_UPDATE_CACHE = {}


def process(queue, body, mapping):
    """
    Processes the actual received body
    :param queue:   Type of queue to be used
    :param body:    Body of the message
    :param mapping:
    """
    if queue == Configuration.get('ovs.core.broker.queues.storagedriver'):
        cache = VolatileFactory.get_client()
        all_extensions = None

        message = FileSystemEvents.EventMessage()
        message.ParseFromString(body)

        # Possible special tags used as `arguments` key:
        # - [NODE_ID]: Replaced by the storagedriver_id as reported by the event
        # - [CLUSTER_ID]: Replaced by the clusterid as reported by the event
        # Possible dedupe key tags:
        # - [EVENT_NAME]: The name of the event message type
        # - [TASK_NAME]: Task method name
        # - [<argument value>]: Any value of the `arguments` dictionary.

        logger.info('Got event, processing...')
        event = None
        for extension in mapping.keys():
            if not message.event.HasExtension(extension):
                continue
            event = message.event.Extensions[extension]
            node_id = message.node_id
            cluster_id = message.cluster_id
            for current_map in mapping[extension]:
                task = current_map['task']
                kwargs = {}
                delay = 0
                routing_key = 'generic'
                for field, target in current_map['arguments'].iteritems():
                    if field == '[NODE_ID]':
                        kwargs[target] = node_id
                    elif field == '[CLUSTER_ID]':
                        kwargs[target] = cluster_id
                    else:
                        kwargs[target] = getattr(event, field)
                if 'options' in current_map:
                    options = current_map['options']
                    if options.get('execonstoragerouter', False):
                        storagedriver = StorageDriverList.get_by_storagedriver_id(node_id)
                        if storagedriver is not None:
                            routing_key = 'sr.{0}'.format(storagedriver.storagerouter.machine_id)
                    delay = options.get('delay', 0)
                    dedupe = options.get('dedupe', False)
                    dedupe_key = options.get('dedupe_key', None)
                    if dedupe is True and dedupe_key is not None:  # We can't dedupe without a key
                        key = 'ovs_dedupe_volumedriver_events_{0}'.format(dedupe_key)
                        key = key.replace('[EVENT_NAME]', extension.full_name)
                        key = key.replace('[TASK_NAME]', task.__class__.__name__)
                        for kwarg_key in kwargs:
                            key = key.replace('[{0}]'.format(kwarg_key), kwargs[kwarg_key])
                        key = key.replace(' ', '_')
                        task_id = cache.get(key)
                        if task_id:
                            # Key exists, task was already scheduled
                            # If task is already running, the revoke message will
                            # be ignored
                            revoke(task_id)
                        _log(task, kwargs, node_id)
                        async_result = task.s(**kwargs).apply_async(
                            countdown=delay,
                            routing_key=routing_key
                        )
                        cache.set(key, async_result.id, 600)  # Store the task id
                        new_task_id = async_result.id
                    else:
                        _log(task, kwargs, node_id)
                        async_result = task.s(**kwargs).apply_async(
                            countdown=delay,
                            routing_key=routing_key
                        )
                        new_task_id = async_result.id
                else:
                    async_result = task.delay(**kwargs)
                    new_task_id = async_result.id
                logger.info('[{0}] {1}({2}) started on {3} with taskid {4}. Delay: {5}s'.format(
                    queue,
                    task.__name__,
                    json.dumps(kwargs),
                    routing_key,
                    new_task_id,
                    delay
                ))
        if event is None:
            message_type = 'unknown'
            if all_extensions is None:
                all_extensions = _load_extensions()
            for extension in all_extensions:
                if message.event.HasExtension(extension):
                    message_type = extension.full_name
            logger.info('A message with type {0} was received. Skipped.'.format(message_type))
    elif queue == 'notifications.info':
        logger.info('Received notification from openstack...')
        try:
            body = json.loads(body)
            print(body)
            event_type = body['event_type']
            logger.info('Processing notification for event {0}'.format(event_type))
            if event_type == 'compute.instance.update':
                old_display_name = body['payload'].get('old_display_name')
                instance_id = body['payload']['instance_id']
                display_name = body['payload'].get('display_name')
                if old_display_name and old_display_name != display_name:
                    logger.info('Caught instance rename event')
                    VMachineController.update_vmachine_name.apply_async(kwargs={'old_name': old_display_name, 'new_name': display_name, 'instance_id': instance_id})
            elif event_type == 'volume.update.start':
                volume_id = body['payload']['volume_id']
                display_name = body['payload']['display_name']
                CINDER_VOLUME_UPDATE_CACHE[volume_id] = display_name
            elif event_type == 'volume.update.end':
                volume_id = body['payload']['volume_id']
                display_name = body['payload']['display_name']
                old_display_name = CINDER_VOLUME_UPDATE_CACHE.get(volume_id)
                if old_display_name and old_display_name != display_name:
                    logger.info('Caught volume rename event')
                    VDiskController.update_vdisk_name.apply_async(kwargs={'volume_id': volume_id, 'old_name': old_display_name, 'new_name': display_name})
                    del CINDER_VOLUME_UPDATE_CACHE[volume_id]
        except Exception as ex:
            logger.error('Processing notification failed {0}'.format(ex))
        logger.info('Processed notification from openstack.')
    else:
        raise NotImplementedError('Queue {0} is not yet implemented'.format(queue))


def _load_extensions():
    """
    Loads all possible extensions
    """
    extensions = []
    for member in inspect.getmembers(VolumeDriverEvents) + inspect.getmembers(FileSystemEvents):
        if isinstance(member[1], FieldDescriptor):
            extensions.append(member[1])
    return extensions


def _log(task, kwargs, storagedriver_id):
    """
    Log an event
    """
    metadata = {'storagedriver': StorageDriverList.get_by_storagedriver_id(storagedriver_id).guid}
    _logger = LogHandler.get('log', name='volumedriver_event')
    _logger.info('[{0}.{1}] - {2} - {3}'.format(
        task.__class__.__module__,
        task.__class__.__name__,
        json.dumps(kwargs),
        json.dumps(metadata)
    ))

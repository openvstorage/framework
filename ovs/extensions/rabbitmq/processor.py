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

"""
Contains the process method for processing rabbitmq messages
"""

from celery.task.control import revoke
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.plugin.provider.configuration import Configuration
from ovs.log.logHandler import LogHandler

logger = LogHandler('extensions', name='processor')


def process(queue, body, mapping):
    """
    Processes the actual received body
    """
    if queue == Configuration.get('ovs.core.broker.volumerouter.queue'):
        import json
        import volumedriver.storagerouter.EventMessages_pb2 as EventMessages
        cache = VolatileFactory.get_client()

        data = EventMessages.EventMessage().FromString(body)

        # Possible special tags used as `arguments` key:
        # - [NODE_ID]: Replaced by the vsrid as reported by the event
        # - [CLUSTER_ID]: Replaced by the clusterid as reported by the event
        # Possible deduping key tags:
        # - [EVENT_NAME]: The name of the eventmessage type
        # - [TASK_NAME]: Task method name
        # - [<argument value>]: Any value of the `arguments` dictionary.

        if data.type in mapping:
            for current_map in mapping[data.type]:
                task = current_map['task']
                data_container = getattr(data, current_map['property'])
                kwargs = {}
                delay = 0
                routing_key = 'generic'
                for field, target in current_map['arguments'].iteritems():
                    if field == '[NODE_ID]':
                        kwargs[target] = data.node_id
                    elif field == '[CLUSTER_ID]':
                        kwargs[target] = data.cluster_id
                    else:
                        kwargs[target] = getattr(data_container, field)
                if 'options' in current_map:
                    options = current_map['options']
                    if options.get('execonstorageappliance', False):
                        vsr = VolumeStorageRouterList.get_by_vsrid(data.node_id)
                        if vsr is not None:
                            routing_key = 'sa.{0}'.format(vsr.storageappliance.machineid)
                    delay = options.get('delay', 0)
                    dedupe = options.get('dedupe', False)
                    dedupe_key = options.get('dedupe_key', None)
                    if dedupe is True and dedupe_key is not None:  # We can't dedupe without a key
                        key = dedupe_key
                        key = key.replace('[EVENT_NAME]', data.type.__class__.__name__)
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
                        async_result = task.s(**kwargs).apply_async(
                            countdown=delay,
                            routing_key=routing_key
                        )
                        cache.set(key, async_result.id, 600)  # Store the task id
                        new_task_id = async_result.id
                    else:
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
        else:
            logger.info('Message type {0} was received. Skipped.'.format(str(data.type)))
    else:
        raise NotImplementedError('Queue {} is not yet implemented'.format(queue))

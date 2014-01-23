#!/usr/bin/env python
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
Consumes messages from rabbitmq, dispatching them to the process method and acknowledges them
"""
import pika
import logging
import sys
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.rabbitmq.processor import process

logging.basicConfig(level='ERROR')


def callback(ch, method, properties, body):
    """
    Handles the message, making sure it gets acknowledged once processed
    """
    _ = properties
    try:
        process(queue, body)
    except Exception as e:
        print 'Error processing message: %s' % e
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = Configuration.get('ovs.grid.ip'),
                                                                   port = int(Configuration.get('ovs.core.broker.port')),
                                                                   credentials = pika.PlainCredentials(Configuration.get('ovs.core.broker.login'),
                                                                                                       Configuration.get('ovs.core.broker.password'))))
    channel = connection.channel()

    queue = sys.argv[1] if len(sys.argv) == 2 else 'default'
    channel.queue_declare(queue=queue, durable=True)
    print 'Waiting for messages on %s. To exit press CTRL+C' % queue

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=queue)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        sys.exit('Exiting consumption of rabbitmq queue {}'.format(queue))

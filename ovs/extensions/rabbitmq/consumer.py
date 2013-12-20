#!/usr/bin/env python
# license see http://www.openvstorage.com/licenses/opensource/
"""
Consumes messages from rabbitmq, dispatching them to the process method and acknowledges them
"""
import pika
import logging
import sys
from JumpScale import j
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
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = j.application.config.get('ovs.grid.ip'),
                                                                   port = int(j.application.config.get('ovs.core.broker.port')),
                                                                   credentials = pika.PlainCredentials(j.application.config.get('ovs.core.broker.login'),
                                                                                                       j.application.config.get('ovs.core.broker.password'))))
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

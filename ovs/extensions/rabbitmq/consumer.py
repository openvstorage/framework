#!/usr/bin/env python
"""
Consumes messages from rabbitmq, dispatching them to the process method and acknowledges them
"""
import pika
import logging
import sys
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

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queue = sys.argv[1] if len(sys.argv) == 2 else 'default'
channel.queue_declare(queue=queue, durable=True)
print 'Waiting for messages on %s. To exit press CTRL+C' % queue

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue)

channel.start_consuming()

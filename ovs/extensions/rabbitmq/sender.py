#!/usr/bin/env python
"""
This script can be used for testing purposes, adding data passed in as the only argument to the
body of a new entry on the queue.
"""
import sys
import pika
import logging

logging.basicConfig(level='ERROR')

data = sys.argv[1] if len(sys.argv) >= 2 else '{}'
queue = sys.argv[2] if len(sys.argv) >= 3 else 'default'

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue=queue, durable=True)

print 'Sending to %s: %s' % (queue, data)
channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=data,
                      properties=pika.BasicProperties(delivery_mode=2))
channel.close()
connection.close()

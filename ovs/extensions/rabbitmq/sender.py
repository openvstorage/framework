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
This script can be used for testing purposes, adding data passed in as the only argument to the
body of a new entry on the queue.
"""
import sys
import pika
import logging

logging.basicConfig(level='ERROR')

if __name__ == '__main__':
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

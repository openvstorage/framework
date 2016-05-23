#!/usr/bin/env python2
# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
This script can be used for testing purposes, adding data passed in as the only argument to the
body of a new entry on the queue.
"""

import sys
import pika

from ovs.log.log_handler import LogHandler


if __name__ == '__main__':
    logger = LogHandler.get('extensions', name='sender')

    data = sys.argv[1] if len(sys.argv) >= 2 else '{}'
    queue = sys.argv[2] if len(sys.argv) >= 3 else 'default'

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=queue, durable=True)

    logger.debug('Sending to {0}: {1}'.format(queue, data))
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=data,
                          properties=pika.BasicProperties(delivery_mode=2))
    channel.close()
    connection.close()

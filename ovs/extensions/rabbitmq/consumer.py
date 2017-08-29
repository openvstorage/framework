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
Consumes messages from rabbitmq, dispatching them to the process method and acknowledges them
"""

import os
import imp
import sys
import pika
import inspect
import logging
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.system import System
from ovs.extensions.rabbitmq.processor import process

mapping = {}


if __name__ == '__main__':
    def callback(ch, method, properties, body):
        """
        Handles the message, making sure it gets acknowledged once processed
        """
        _ = properties
        try:
            if type(body) == unicode:
                data = bytearray(body, 'utf-8')
                body = bytes(data)
            process(queue, body, mapping)
        except Exception as e:
            logger.exception('Error processing message: {0}'.format(e))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    import argparse
    parser = argparse.ArgumentParser(description='Rabbitmq Event Processor for OVS',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('rabbitmq_queue', type=str,
                        help='Rabbitmq queue name')
    parser.add_argument('--durable', dest='queue_durable', action='store_const', default=False, const=True,
                        help='Declare queue as durable')

    logger = Logger('extensions-rabbitmq')

    args = parser.parse_args()
    try:
        run_event_consumer = False
        my_ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(System.get_my_machine_id()))
        for endpoint in Configuration.get('/ovs/framework/messagequeue|endpoints'):
            if endpoint.startswith(my_ip):
                run_event_consumer = True

        if run_event_consumer is True:
            # Load mapping
            mapping = {}
            path = '/'.join([os.path.dirname(__file__), 'mappings'])
            for filename in os.listdir(path):
                if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                    name = filename.replace('.py', '')
                    mod = imp.load_source(name, '/'.join([path, filename]))
                    for member in inspect.getmembers(mod):
                        if inspect.isclass(member[1]) \
                                and member[1].__module__ == name \
                                and 'object' in [base.__name__ for base in member[1].__bases__]:
                            this_mapping = member[1].mapping
                            for key in this_mapping.keys():
                                if key not in mapping:
                                    mapping[key] = []
                                mapping[key] += this_mapping[key]
            logger.debug('Event map:')
            for key in mapping:
                logger.debug('{0}: {1}'.format(key.name, [current_map['task'].__name__ for current_map in mapping[key]]))

            # Starting connection and handling
            rmq_servers = Configuration.get('/ovs/framework/messagequeue|endpoints')
            channel = None
            server = ''
            loglevel = logging.root.manager.disable  # Workaround for disabling logging
            logging.disable('WARNING')
            for server in rmq_servers:
                try:
                    connection = pika.BlockingConnection(
                        pika.ConnectionParameters(host=server.split(':')[0],
                                                  port=int(server.split(':')[1]),
                                                  credentials=pika.PlainCredentials(
                                                      Configuration.get('/ovs/framework/messagequeue|user'),
                                                      Configuration.get('/ovs/framework/messagequeue|password'))
                                                  )
                    )
                    channel = connection.channel()
                    break
                except:
                    pass
            logging.disable(loglevel)  # Restore workaround
            if channel is None:
                raise RuntimeError('Could not connect to any available RabbitMQ endpoint.')
            logger.debug('Connected to: {0}'.format(server))

            queue = args.rabbitmq_queue
            durable = args.queue_durable
            channel.queue_declare(queue=queue, durable=durable)
            logger.info('Waiting for messages on {0}...'.format(queue))
            logger.info('To exit press CTRL+C', extra={'print_msg': True})

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(callback, queue=queue)
            channel.start_consuming()
        else:
            logger.info('Nothing to do here, kthxbai',)

    except KeyboardInterrupt:
        pass
    except Exception as exception:
        logger.error('Unexpected exception: {0}'.format(str(exception)))
        sys.exit(1)

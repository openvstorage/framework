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
import os
import time
import sys
import logging
from configobj import ConfigObj
import pyinotify
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.rabbitmq.processor import process
from ovs.extensions.generic.system import Ovs
from ovs.plugin.provider.configuration import Configuration
from ovs.log.logHandler import LogHandler

logger = LogHandler('ovs.extensions', name='consumer')
logging.basicConfig()
KVM_ETC = '/etc/libvirt/qemu/'


def run_kvm_watcher():
    """
    Check whether to run the KVM file watcher
    """
    return Ovs.get_my_vsa().pmachine.hvtype == 'KVM'


def run_event_consumer():
    """
    Check whether to run the event consumer
    """
    rmq_ini = ConfigObj(os.path.join(Configuration.get('ovs.core.cfgdir'), 'rabbitmqclient.cfg'))
    rmq_nodes = rmq_ini.get('main')['nodes'] if type(rmq_ini.get('main')['nodes']) == list else [rmq_ini.get('main')['nodes'], ]
    machine_id = Ovs.get_my_machine_id()
    return machine_id in rmq_nodes


def callback(ch, method, properties, body):
    """
    Handles the message, making sure it gets acknowledged once processed
    """
    _ = properties
    try:
        process(queue, body)
    except Exception as e:
        logger.exception('Error processing message: {0}'.format(e))
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    notifier = None
    try:
        if run_kvm_watcher():
            from ovs.extensions.rabbitmq.kvm_xml_processor import Kxp

            wm = pyinotify.WatchManager()

            MASK_EVENTS_TO_WATCH = pyinotify.IN_CLOSE_WRITE | \
                pyinotify.IN_CREATE | \
                pyinotify.IN_DELETE | \
                pyinotify.IN_MODIFY | \
                pyinotify.IN_MOVED_FROM | \
                pyinotify.IN_MOVED_TO | \
                pyinotify.IN_UNMOUNT

            notifier = pyinotify.ThreadedNotifier(wm, Kxp())
            notifier.start()

            wdd = wm.add_watch(KVM_ETC, MASK_EVENTS_TO_WATCH, rec=True)
            logger.info('Watching {0}...'.format(KVM_ETC), print_msg=True)

            vpool_mountpoints = set()
            for vpool in VPoolList().get_vpools():
                for vsr in vpool.vsrs:
                    vsrid = Ovs.get_my_vsr_id(vpool.name)
                    if vsrid == vsr.vsrid:
                        vpool_mountpoints.add(vsr.mountpoint)
            logger.info('KVM xml processor active...', print_msg=True)

        if run_event_consumer():
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=Configuration.get('ovs.grid.ip'),
                                          port=int(Configuration.get('ovs.core.broker.port')),
                                          credentials=pika.PlainCredentials(
                                              Configuration.get('ovs.core.broker.login'),
                                              Configuration.get('ovs.core.broker.password')
                                          )))
            channel = connection.channel()

            queue = sys.argv[1] if len(sys.argv) == 2 else 'default'
            channel.queue_declare(queue=queue, durable=True)
            logger.info('Waiting for messages on {0}...'.format(queue), print_msg=True)
            logger.info('To exit press CTRL+C', print_msg=True)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(callback, queue=queue)
            channel.start_consuming()
        else:
            # We need to keep the process running
            logger.info('To exit press CTRL+C', print_msg=True)
            while True:
                time.sleep(3600)

    except KeyboardInterrupt:
        if run_kvm_watcher() and notifier is not None:
            notifier.stop()
    except Exception as exception:
        logger.error('Unexpected exception: {0}'.format(str(exception)), print_msg=True)
        if run_kvm_watcher() and notifier is not None:
            notifier.stop()
        sys.exit(1)

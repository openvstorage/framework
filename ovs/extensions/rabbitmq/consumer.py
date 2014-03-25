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
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.rabbitmq.processor import process
from ovs.extensions.generic.system import Ovs
from ovs.plugin.provider.configuration import Configuration
import pyinotify

logging.basicConfig(level='ERROR')

KVM_ETC = '/etc/libvirt/qemu/'


def is_kvm_available():
    return Ovs.get_my_vsa().pmachine.hvtype == 'KVM'


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
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=Configuration.get('ovs.grid.ip'),
                                  port=int(
                                      Configuration.get(
                                          'ovs.core.broker.port')),
                                  credentials=pika.PlainCredentials(
                                      Configuration.get(
                                          'ovs.core.broker.login'),
                                      Configuration.get('ovs.core.broker.password'))))
    channel = connection.channel()

    queue = sys.argv[1] if len(sys.argv) == 2 else 'default'
    channel.queue_declare(queue=queue, durable=True)
    print 'Waiting for messages on %s. To exit press CTRL+C' % queue

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=queue)

    if is_kvm_available():
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
        print "Watching {0}...".format(KVM_ETC)

        vpool_mountpoints = set()
        for vpool in VPoolList().get_vpools():
            for vsr in vpool.vsrs:
                vsrid = Ovs.get_my_vsr_id(vpool.name)
                if vsrid == vsr.vsrid:
                    vpool_mountpoints.add(vsr.mountpoint)
        print "kvm xml processor active..."

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        if is_kvm_available():
            notifier.stop()
        sys.exit('Exiting consumption of rabbitmq queue {}'.format(queue))

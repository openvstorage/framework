from __future__ import absolute_import

import time
import logging
import sys
sys.path.append('/opt/openvStorage')

from celery import Celery
from ovs import celeryconfig
from ovs.logging.logHandler import LogHandler

celery = Celery('ovs',
                include=['ovs.lib.dummy',
                         'ovs.lib.vdisk',
                         'ovs.lib.vmachine',
                         'ovs.lib.user',
                         'ovs.lib.messaging',
                         'ovs.hypervisor.hypervisors.vmware'])
celery.config_from_object(celeryconfig)

loghandler = LogHandler('celery.log')

if __name__ == '__main__':
    celery.start()
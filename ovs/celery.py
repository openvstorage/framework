from __future__ import absolute_import

import time
import logging
import sys
sys.path.append('/opt/openvStorage')

from celery import Celery
from ovs import celeryconfig

celery = Celery('ovs',
                include=['ovs.lib.dummy',
                         'ovs.lib.vdisk',
                         'ovs.lib.machine'
                         'ovs.lib.user',
                         'ovs.hypervisor.hypervisors.vmware'])
celery.config_from_object(celeryconfig)

if __name__ == '__main__':
    celery.start()
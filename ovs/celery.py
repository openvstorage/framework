from __future__ import absolute_import

import time
import logging
import sys
sys.path.append('/opt/openvStorage')

from celery import Celery
from ovs import celeryconfig

celery = Celery('ovs',
                broker=celeryconfig.BROKER_URL,
                backend=celeryconfig.CELERY_RESULT_BACKEND,
                include=['ovs.app.dummy',
                         'ovs.app.disk', 'ovs.app.user',
                         'ovs.app.user'])

if __name__ == '__main__':
    celery.start()
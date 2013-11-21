"""
Celery entry point module
"""
from __future__ import absolute_import

import sys
sys.path.append('/opt/OpenvStorage')

from celery import Celery
from celery.schedules import crontab
from ovs.logging.logHandler import LogHandler

celery = Celery('ovs',
                include=['ovs.lib.dummy',
                         'ovs.lib.vdisk',
                         'ovs.lib.vmachine',
                         'ovs.lib.user',
                         'ovs.lib.messaging',
                         'ovs.lib.scheduledtask',
                         'ovs.hypervisor.hypervisors.vmware'])

celery.conf.CELERY_RESULT_BACKEND = "cache"
celery.conf.CELERY_CACHE_BACKEND = 'memcached://127.0.0.1:11211/'
celery.conf.BROKER_URL = 'amqp://guest:guest@127.0.0.1:5672//'
celery.conf.CELERYBEAT_SCHEDULE = {
    # Snapshot policy
    # > Executes every weekday between 2 and 22 hour, every 15 minutes
    'take-snapshots': {
        'task': 'ovs.scheduled.snapshotall',
        'schedule': crontab(minute='0', hour='2-22', day_of_week='mon,tue,wed,thu,fri'),
        'args': [],
    },
}

loghandler = LogHandler('celery.log')

if __name__ == '__main__':
    celery.start()

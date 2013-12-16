# license see http://www.openvstorage.com/licenses/opensource/
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
                include=['ovs.lib.vdisk',
                         'ovs.lib.vmachine',
                         'ovs.lib.messaging',
                         'ovs.lib.scheduledtask',
                         'ovs.extensions.hypervisor.hypervisors.vmware'])

celery.conf.CELERY_TASK_SERIALIZER = 'json'
celery.conf.CELERY_ACCEPT_CONTENT = ['json']
celery.conf.CELERY_RESULT_BACKEND = "cache"
celery.conf.CELERY_CACHE_BACKEND = 'memcached://127.0.0.1:11211/'
celery.conf.BROKER_URL = 'amqp://guest:guest@127.0.0.1:5672//'
celery.conf.CELERYBEAT_SCHEDULE = {
    # Snapshot policy
    # > Executes every day, hourly between 02:00 and 22:00 hour
    'take-snapshots': {
        'task': 'ovs.scheduled.snapshotall',
        'schedule': crontab(minute='0', hour='2-22'),
        'args': []
    },
    # Delete snapshot policy
    # > Excutes every day at 00:30
    #'delete-snapshots': {
    #    'task': 'ovs.scheduled.deletesnapshots',
    #    'schedule': crontab(minute='30', hour='0'),
    #    'args': []
    #}
}

loghandler = LogHandler('celery.log')

if __name__ == '__main__':
    celery.start()

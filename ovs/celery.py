# license see http://www.openvstorage.com/licenses/opensource/
"""
Celery entry point module
"""
from __future__ import absolute_import

import sys
sys.path.append('/opt/OpenvStorage')

import os
from celery import Celery
from celery.schedules import crontab
from ovs.logging.logHandler import LogHandler
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.tools import Tools

memcache_ini = Tools.inifile.open(os.path.join(Configuration.get('ovs.core.cfgdir'), 'memcacheclient.cfg'))
nodes = memcache_ini.getValue('main', 'nodes').split(',')
memcache_servers = map(lambda m: memcache_ini.getValue(m, 'location'), nodes)

celery = Celery('ovs',
                include=['ovs.lib.vdisk',
                         'ovs.lib.vmachine',
                         'ovs.lib.messaging',
                         'ovs.lib.scheduledtask',
                         'ovs.extensions.hypervisor.hypervisors.vmware'])

celery.conf.CELERY_RESULT_BACKEND = "cache"
celery.conf.CELERY_CACHE_BACKEND = 'memcached://{}/'.format(';'.join(memcache_servers))
celery.conf.BROKER_URL = '{}://{}:{}@{}:{}//'.format(Configuration.get('ovs.core.broker.protocol'),
                                                     Configuration.get('ovs.core.broker.login'),
                                                     Configuration.get('ovs.core.broker.password'),
                                                     Configuration.get('ovs.grid.ip'),
                                                     Configuration.get('ovs.core.broker.port'))
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
    'delete-scrub-snapshots': {
        'task': 'ovs.scheduled.deletescrubsnapshots',
        'schedule': crontab(minute='30', hour='0'),
        'args': []
    },
    # Collapse arakoon tlogs
    # > Executes every day at 00:30
    'arakoon-collapse': {
        'task': 'ovs.scheduled.collapse_arakoon',
        'schedule': crontab(minute='30', hour='0'),
        'args': []
    }
}

loghandler = LogHandler('celery.log')

if __name__ == '__main__':
    celery.start()

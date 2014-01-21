# license see http://www.openvstorage.com/licenses/opensource/
"""
Celery entry point module
"""
from __future__ import absolute_import

import sys
sys.path.append('/opt/OpenvStorage')

import os
from kombu import Queue
from celery import Celery
from celery.schedules import crontab
from ovs.logging.logHandler import LogHandler
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.tools import Tools
from ovs.dal.lists.vmachinelist import VMachineList

memcache_ini = Tools.inifile.open(os.path.join(Configuration.get('ovs.core.cfgdir'), 'memcacheclient.cfg'))
nodes = memcache_ini.getValue('main', 'nodes').split(',')
memcache_servers = map(lambda m: memcache_ini.getValue(m, 'location'), nodes)

rmq_ini = Tools.inifile.open(os.path.join(Configuration.get('ovs.core.cfgdir'), 'rabbitmqclient.cfg'))
nodes = rmq_ini.getValue('main', 'nodes').split(',')
rmq_servers = map(lambda m: rmq_ini.getValue(m, 'location'), nodes)

vsas = VMachineList.get_vsas()

celery = Celery('ovs',
                include=['ovs.lib.vdisk',
                         'ovs.lib.vmachine',
                         'ovs.lib.vpool',
                         'ovs.lib.messaging',
                         'ovs.lib.scheduledtask',
                         'ovs.extensions.hypervisor.hypervisors.vmware'])

celery.conf.CELERY_RESULT_BACKEND = "cache"
celery.conf.CELERY_CACHE_BACKEND = 'memcached://{0}/'.format(';'.join(memcache_servers))
celery.conf.BROKER_URL = ';'.join(['{0}://{1}:{2}@{3}//'.format(Configuration.get('ovs.core.broker.protocol'),
                                                                Configuration.get('ovs.core.broker.login'),
                                                                Configuration.get('ovs.core.broker.password'),
                                                                server)
                                   for server in rmq_servers])
celery.conf.CELERY_DEFAULT_QUEUE = 'ovs_generic'
queues = [Queue('ovs_generic', routing_key='generic.#')]
for vsa in vsas:
    queues.append(Queue('ovs_{0}'.format(vsa.machineid), routing_key='vsa.{0}.#'.format(vsa.machineid)))
celery.conf.CELERY_QUEUES = tuple(queues)
celery.conf.CELERY_DEFAULT_EXCHANGE = 'generic'
celery.conf.CELERY_DEFAULT_EXCHANGE_TYPE = 'topic'
celery.conf.CELERY_DEFAULT_ROUTING_KEY = 'generic.default'

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

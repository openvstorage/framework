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
Celery entry point module
"""
from __future__ import absolute_import

import sys
sys.path.append('/opt/OpenvStorage')

import os
from kombu import Queue
from celery import Celery
from celery.beat import Scheduler, ScheduleEntry  # Do not remove, need these in celery_beat.py
from celery import current_app  # Do not remove, need these in celery_beat.py
from celery.schedules import crontab
from celery.signals import task_postrun, worker_process_init
from ovs.lib.messaging import MessageController
from ovs.log.logHandler import LogHandler
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.plugin.provider.configuration import Configuration
from configobj import ConfigObj
from subprocess import check_output

memcache_ini = ConfigObj(os.path.join(Configuration.get('ovs.core.cfgdir'), 'memcacheclient.cfg'))
memcache_nodes = memcache_ini.get('main')['nodes'] if type(memcache_ini.get('main')['nodes']) == list else [memcache_ini.get('main')['nodes'], ]
memcache_servers = map(lambda m: memcache_ini.get(m)['location'], memcache_nodes)

rmq_ini = ConfigObj(os.path.join(Configuration.get('ovs.core.cfgdir'), 'rabbitmqclient.cfg'))
rmq_nodes = rmq_ini.get('main')['nodes'] if type(rmq_ini.get('main')['nodes']) == list else [rmq_ini.get('main')['nodes'], ]
rmq_servers = map(lambda m: rmq_ini.get(m)['location'], rmq_nodes)

unique_id = sorted(check_output("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'", shell=True).strip().split('\n'))[0]

include = []
path = os.path.join(os.path.dirname(__file__), 'lib')
for filename in os.listdir(path):
    if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py') and filename != '__init__.py':
        name = filename.replace('.py', '')
        include.append('ovs.lib.{0}'.format(name))

celery = Celery('ovs', include=include)

celery.conf.CELERY_RESULT_BACKEND = "cache"
celery.conf.CELERY_CACHE_BACKEND = 'memcached://{0}/'.format(';'.join(memcache_servers))
celery.conf.BROKER_URL = ';'.join(['{0}://{1}:{2}@{3}//'.format(Configuration.get('ovs.core.broker.protocol'),
                                                                Configuration.get('ovs.core.broker.login'),
                                                                Configuration.get('ovs.core.broker.password'),
                                                                server)
                                   for server in rmq_servers])
celery.conf.CELERY_DEFAULT_QUEUE = 'ovs_generic'
celery.conf.CELERY_QUEUES = tuple([Queue('ovs_generic', routing_key='generic.#'),
                                   Queue('ovs_{0}'.format(unique_id), routing_key='sr.{0}.#'.format(unique_id))])
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

loghandler = LogHandler('celery', name='celery')


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """
    Hook for celery postrun event
    """
    _ = sender, task, args, kwargs, kwds
    MessageController.fire(MessageController.Type.TASK_COMPLETE, task_id)


@worker_process_init.connect
def worker_process_init_handler(args=None, kwargs=None, **kwds):
    """
    Hook for process init
    """
    _ = args, kwargs, kwds
    VolatileFactory.store = None
    PersistentFactory.store = None


if __name__ == '__main__':
    celery.start()

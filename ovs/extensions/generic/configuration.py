# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Generic module for managing configuration somewhere
"""

import copy
import json
import random
import string
# noinspection PyUnresolvedReferences
from ovs_extensions.generic.configuration import Configuration as _Configuration, ConnectionException, NotFoundException


class Configuration(_Configuration):
    """
    Extends the 'default' configuration class
    """
    CACC_LOCATION = '/opt/OpenvStorage/config/arakoon_cacc.ini'
    CONFIG_STORE_LOCATION = '/opt/OpenvStorage/config/framework.json'

    base_config = {'cluster_id': None,
                   'external_config': None,
                   'plugins/installed': {'backends': [],
                                         'generic': []},
                   'paths': {'basedir': '/opt/OpenvStorage',
                             'ovsdb': '/opt/OpenvStorage/db'},
                   'support': {'enablesupport': False,
                               'enabled': True,
                               'interval': 60},
                   'webapps': {'html_endpoint': '/',
                               'oauth2': {'mode': 'local'}}}

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @classmethod
    def initialize_host(cls, host_id, port_info=None):
        """
        Initialize keys when setting up a host
        :param host_id: ID of the host
        :type host_id: str
        :param port_info: Information about ports to be used
        :type port_info: dict
        :return: None
        """
        if cls.exists('/ovs/framework/hosts/{0}/setupcompleted'.format(host_id)):
            return
        if port_info is None:
            port_info = {}

        mds_port_range = port_info.get('mds', [26300, 26399])
        arakoon_start_port = port_info.get('arakoon', 26400)
        storagedriver_port_range = port_info.get('storagedriver', [26200, 26299])

        host_config = {'ports': {'storagedriver': [storagedriver_port_range],
                                 'mds': [mds_port_range],
                                 'arakoon': [arakoon_start_port]},
                       'setupcompleted': False,
                       'versions': {'ovs': 9},
                       'type': 'UNCONFIGURED'}
        for key, value in host_config.iteritems():
            cls.set('/ovs/framework/hosts/{0}/{1}'.format(host_id, key), value, raw=False)

    @classmethod
    def initialize(cls, external_config=None, logging_target=None):
        """
        Initialize general keys for all hosts in cluster
        :param external_config: The configuration store runs on another host outside the cluster
        :param logging_target: Configures (overwrites) logging configuration
        """
        cluster_id = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
        if cls.exists('/ovs/framework/cluster_id'):
            return

        messagequeue_cfg = {'endpoints': [],
                            'metadata': {'internal': True},
                            'protocol': 'amqp',
                            'user': 'ovs',
                            'password': '0penv5tor4ge',
                            'queues': {'storagedriver': 'volumerouter'}}

        base_cfg = copy.deepcopy(cls.base_config)
        base_cfg.update({'cluster_id': cluster_id,
                         'external_config': external_config,
                         'arakoon_clusters': {},
                         'stores': {'persistent': 'pyrakoon',
                                    'volatile': 'memcache'},
                         'logging': {'type': 'console', 'level': 'DEBUG'},
                         'scheduling/celery': {'ovs.statsmonkey.run_all': None,  # Disable statsmonkey scheduled task by default
                                               'alba.statsmonkey.run_all': None}})
        if logging_target is not None:
            base_cfg['logging'] = logging_target
        if cls.exists('/ovs/framework/memcache') is False:
            base_cfg['memcache'] = {'endpoints': [],
                                    'metadata': {'internal': True}}
        if cls.exists('/ovs/framework/messagequeue') is False:
            base_cfg['messagequeue'] = messagequeue_cfg
        else:
            messagequeue_info = cls.get('/ovs/framework/messagequeue')
            for key, value in messagequeue_cfg.iteritems():
                if key not in messagequeue_info:
                    base_cfg['messagequeue'][key] = value
        for key, value in base_cfg.iteritems():
            cls.set('/ovs/framework/{0}'.format(key), value, raw=False)

    @classmethod
    def get_store_info(cls):
        """
        Retrieve the configuration store method. Currently this can only be 'arakoon'
        :return: The store method
        :rtype: str
        """
        with open(cls.CONFIG_STORE_LOCATION) as config_file:
            contents = json.load(config_file)
            return contents['configuration_store']

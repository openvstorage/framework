# Copyright (C) 2018 iNuron NV
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

from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.configuration.exceptions import ConfigurationNotFoundException as NotFoundException
from ovs_extensions.generic.graphiteclient import GraphiteClient as _graphite_client
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class GraphiteClient(_graphite_client):
    """
    Make a Graphite client, which allows data to be sent to Graphite
    """
    CONFIG_PATH = '/ovs/framework/monitoring/stats_monkey'

    def __init__(self, ip=None, port=None, database=None):
        # type: (str, int, str) -> None
        """
        Create client instance for graphite and validate parameters
        :param ip: IP address of the client to send graphite data towards
        :type ip: str
        :param port: port of the UDP listening socket
        :type port: int
        :param database: name of the database
        :type database: str
        """
        graphite_data = {}
        if all(p is None for p in [ip, port]):
            # Nothing specified
            graphite_data = self.get_graphite_config()
            if not graphite_data:
                raise RuntimeError('No graphite data found in config path `{0}`'.format(self.CONFIG_PATH))

        ip = ip or graphite_data['ip']
        port = port or graphite_data.get('port', 2003)

        ExtensionsToolbox.verify_required_params(verify_keys=True,
                                                 actual_params={'host': ip,
                                                                'port': port},
                                                 required_params={'host': (str, ExtensionsToolbox.regex_ip, True),
                                                                  'port': (int, {'min': 1025, 'max': 65535}, True)})

        super(GraphiteClient, self).__init__(ip=ip, port=port, database=database)

    def send_statsmonkey_data(self, sm_data, function_name):
        # type: (List, str) -> None
        """
        Sends statsmonkey formatted data to graphite
        :param sm_data: list
        Example format:
             [{'fields': {'capacity': 100,
                          'load': 1.0,
                          'masters': 1,
                          'slaves': 0},
               'measurement': 'mds',
               'tags': {'environment': u'simon_stats_test',
                        'mds_number': 0,
                        'storagerouter_name': u'svdb_01',
                        'vpool_name': u'vp1'}}]
        :param function_name: Name of the statsmonkey being executed
        :type function_name: str
        :return None
        """
        for datapointset in sm_data:
            path = '.'.join([datapointset['tags'].get('environment'),
                             function_name,
                             datapointset['tags'].get('vpool_name') or '__',  # If the key is not present, place a __ to make later removal possible
                             datapointset['tags'].get('storagerouter_name') or '__',
                             str(datapointset['tags'].get('mds_number') or '__')])
            path = path.replace('.__', '', 4)
            for fieldkey, fieldvalue in datapointset['fields'].items():
                tmp_path = '{0}.{1}'.format(path, fieldkey)
                self.send(tmp_path, fieldvalue)

    @classmethod
    def get_graphite_config(cls):
        # type: () -> Dict[str, Union[str, int]]
        """
        Retrieve the graphite config (if any)
        :return:
        """
        try:
            graphite_data = Configuration.get(cls.CONFIG_PATH)
            return {'ip': graphite_data['host'],
                    'port': graphite_data.get('port', 2003)}
        except NotFoundException:
            return {}

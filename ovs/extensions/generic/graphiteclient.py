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

import time
import socket
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.configuration.exceptions import ConfigurationNotFoundException as NotFoundException
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.generic.logger import Logger


class GraphiteClient(object):
    """
    Make a statistics object, which allows it to be sent to Graphite
    """

    def __init__(self, ip=None, port=None, database=None):
        # type: (str, int) -> None
        """
        Create client instance for graphite and validate parameters
        :param ip: IP address of the client to send graphite data towards
        :type ip: str
        :param port: port of the UDP listening socket
        :type port: int
        :param database: name of the database
        :type database: str
        ":param env:
        """
        config_path = '/ovs/framework/monitoring/stats_monkey'
        self.logger = Logger(name='lib')

        precursor = 'openvstorage.fwk'
        if database is not None and not database.startswith(precursor):
            precursor = '.'.join([precursor, database])
        self.precursor = precursor + '.{0} {1} {2}'   # format: precusor.env.x.y.z value timestamp

        if all(p is None for p in [ip, port]):
            # Nothing specified
            try:
                graphite_data = Configuration.get(config_path)
            except NotFoundException:
                raise RuntimeError('No graphite data found in config path `{0}`'.format(config_path))

        ip = ip or graphite_data['host']
        port = port or graphite_data.get('port', 2003)

        ExtensionsToolbox.verify_required_params(verify_keys=True,
                                                 actual_params={'host': ip,
                                                                'port': port},
                                                 required_params={'host': (str, ExtensionsToolbox.regex_ip, True),
                                                                  'port': (int, {'min': 1025, 'max': 65535}, True)})
        self.ip = ip
        self.port = port

    def __str__(self):
        return 'Graphite client: ({0}:{1})'.format(self.ip, self.port)

    def __repr__(self):
        return str(self)

    def send(self, path, data):
        # type: (str, Any) -> None
        """
        Send the statistics with client
        :param path: path in graphite to send the data to
        :param data: data to send
        :return: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            datastring = self.precursor.format(path, data, int(time.time()))  # Carbon timestamp in integers
            sock.sendto(datastring, (self.ip, self.port))
        finally:
            sock.close()

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

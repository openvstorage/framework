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


class GraphiteClient(object):
    """
    Make a statistics object, which allows it to be sent to Graphite
    """

    def __init__(self, ip=None, port=None):
        # type: (str, int) -> None
        """
        Create client instance for graphite and validate parameters
        :param ip: IP address of the client to send graphite data towards
        :type ip: str
        :param port: port of the UDP listening socket
        :type port: int
        """
        config_path = 'ovs/framework/graphite'
        self.precursor = 'openvstorage.fwk.{0} {1} {2}'

        try:
            graphite_data = Configuration.get(config_path)
        except NotFoundException:
            raise RuntimeError('No graphite data found in config path `{0}`'.format(config_path))

        if ip is None:
            ip = graphite_data['ip']
        if port is None:
            port = int(graphite_data.get('port', 2003))

        ExtensionsToolbox.verify_required_params(verify_keys=True,
                                                 actual_params={'ip': ip,
                                                                'port': port},
                                                 required_params={'ip': (str, ExtensionsToolbox.regex_ip, True),
                                                                  'port': (int, {'min': 1025, 'max': 65535}, True)})
        self.ip = ip
        self.port = port

    def __str__(self):
        return 'Graphite client: ({0}:{1})'.format(self.ip, self.port)

    def __repr__(self):
        return str(self)

    def send(self, path, data):
        # type: (str, float) -> None
        """
        Send the statistics with client
        :param path: path in graphite to send the data to
        :param data: data to send
        :return: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        datastring = self.precursor.format(path, int(data), int(time.time()))  # Carbon timestamp in integers
        sock.sendto(datastring, (self.ip, self.port))
        sock.close()

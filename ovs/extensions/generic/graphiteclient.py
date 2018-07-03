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


class GraphiteClient(object):
    """
    Make a statistics object, which allows it to be sent to Graphite
    """

    def __init__(self, ip=None, port=None):
        config_path = 'ovs/framework/graphite'
        self.precursor = 'openvstorage.fwk:{0}'

        if ip is not None:
            self.ip = ip
        if port is not None:
            self.port = port

        else:
            try:
                graphite_data = Configuration.get(config_path)
                if 'ip' not in graphite_data:
                    raise RuntimeError('IP needs to be specified in config path `{0}`'.format(config_path))
            except NotFoundException:
                raise RuntimeError('No graphite data found in config path `{0}`'.format(config_path))

            self.port = graphite_data.get('port', 2003)
            self.ip = graphite_data['ip']

    def __str__(self):
        return 'Graphite client: ({0}:{1})'.format(self.ip, self.port)

    def send(self, data):
        """
        Send the statistics object to
        :param data: data to send
        :return:
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(self.precursor.format(bytes(data)), (self.ip, self.port))
        sock.close()

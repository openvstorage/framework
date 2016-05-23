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
OS manager Factory module
"""

from subprocess import check_output
from ovs.extensions.os.ubuntu import Ubuntu
from ovs.extensions.os.centos import Centos
from ovs.log.log_handler import LogHandler


class OSManager(object):
    """
    Factory class returning specialized classes
    """
    ImplementationClass = None
    _logger = LogHandler.get('extensions', name='osmanager')

    class MetaClass(type):
        """
        Metaclass
        """

        def __getattr__(cls, item):
            """
            Returns the appropriate class
            """
            _ = cls
            if OSManager.ImplementationClass is None:
                try:
                    dist_info = check_output('cat /etc/os-release', shell=True)
                    # All OS distribution classes used in below code should share the exact same interface!
                    if 'Ubuntu' in dist_info:
                        OSManager.ImplementationClass = Ubuntu
                    elif 'CentOS Linux' in dist_info:
                        OSManager.ImplementationClass = Centos
                    else:
                        raise RuntimeError('There was no known OSManager detected')
                except Exception as ex:
                    OSManager._logger.exception('Error loading OSManager: {0}'.format(ex))
                    raise
            return getattr(OSManager.ImplementationClass, item)

    __metaclass__ = MetaClass

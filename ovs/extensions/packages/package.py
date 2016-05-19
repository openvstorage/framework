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
Package Factory module
"""

from subprocess import check_output
from ovs.extensions.packages.debian import DebianPackage
from ovs.extensions.packages.rpm import RpmPackage
from ovs.log.log_handler import LogHandler


class PackageManager(object):
    """
    Factory class returning specialized classes
    """
    _logger = LogHandler.get('extensions', name='packagemanager')
    ImplementationClass = None

    class MetaClass(type):
        """
        Metaclass
        """

        def __getattr__(cls, item):
            """
            Returns the appropriate class
            """
            _ = cls
            if PackageManager.ImplementationClass is None:
                try:
                    distributor = check_output('lsb_release -i', shell=True)
                    distributor = distributor.replace('Distributor ID:', '').strip()
                    # All *Package classes used in below code should share the exact same interface!
                    if distributor in ['Ubuntu']:
                        PackageManager.ImplementationClass = DebianPackage
                    elif distributor in ['CentOS']:
                        PackageManager.ImplementationClass = RpmPackage
                    else:
                        raise RuntimeError('There is no handler for Distributor ID: {0}'.format(distributor))
                except Exception as ex:
                    PackageManager._logger.exception('Error loading Distributor ID: {0}'.format(ex))
                    raise
            return getattr(PackageManager.ImplementationClass, item)

    __metaclass__ = MetaClass

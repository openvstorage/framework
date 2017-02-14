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
Service Factory module
"""
import os
from subprocess import check_output
from ovs.log.log_handler import LogHandler
if os.environ.get('RUNNING_UNITTESTS') == 'True':
    from ovs.extensions.services.tests.systemd import Systemd
else:
    from ovs.extensions.services.upstart import Upstart
    from ovs.extensions.services.systemd import Systemd


class ServiceManager(object):
    """
    Factory class returning specialized classes
    """
    _logger = LogHandler.get('extensions', name='service-manager')
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
            if ServiceManager.ImplementationClass is None:
                if os.environ.get('RUNNING_UNITTESTS') == 'True':
                    ServiceManager.ImplementationClass = Systemd
                else:
                    try:
                        init_info = check_output('cat /proc/1/comm', shell=True)
                        # All service classes used in below code should share the exact same interface!
                        if 'init' in init_info:
                            version_info = check_output('init --version', shell=True)
                            if 'upstart' in version_info:
                                ServiceManager.ImplementationClass = Upstart
                            else:
                                raise RuntimeError('The ServiceManager is unrecognizable')
                        elif 'systemd' in init_info:
                            ServiceManager.ImplementationClass = Systemd
                        else:
                            raise RuntimeError('There was no known ServiceManager detected')
                    except Exception as ex:
                        ServiceManager._logger.exception('Error loading ServiceManager: {0}'.format(ex))
                        raise
            return getattr(ServiceManager.ImplementationClass, item)

    __metaclass__ = MetaClass

    @staticmethod
    def reload():
        """
        Reset the ImplementationClass
        :return: None
        """
        ServiceManager.ImplementationClass = None

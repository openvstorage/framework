# Copyright 2015 CloudFounders NV
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
Service Factory module
"""

from subprocess import check_output
from ovs.extensions.services.upstart import Upstart
# from ovs.extensions.services.systemd import SystemD
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='servicemanager')


class ServiceManager(object):
    """
    Factory class returning specialized classes
    """
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
                try:
                    init_info = check_output('init --version', shell=True)
                    # All service classes used in below code should share the exact same interface!
                    if 'upstart' in init_info:
                        ServiceManager.ImplementationClass = Upstart
                    # elif 'systemd' in init_info:
                    #     ServiceManager.ImplementationClass = SystemD
                    else:
                        raise RuntimeError('There was no known ServiceManager detected')
                except Exception as ex:
                    logger.exception('Error loading ServiceManager: {0}'.format(ex))
                    raise
            return getattr(ServiceManager.ImplementationClass, item)

    __metaclass__ = MetaClass

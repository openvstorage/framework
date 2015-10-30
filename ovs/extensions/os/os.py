# Copyright 2015 CloudFounders NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OS manager Factory module
"""

from subprocess import check_output
from ovs.extensions.os.ubuntu import Ubuntu
from ovs.extensions.os.centos import Centos
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='osmanager')


class OSManager(object):
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
                    logger.exception('Error loading OSManager: {0}'.format(ex))
                    raise
            return getattr(OSManager.ImplementationClass, item)

    __metaclass__ = MetaClass

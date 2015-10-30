# Copyright 2015 iNuron NV
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
Package Factory module
"""

from subprocess import check_output
from ovs.extensions.packages.debian import DebianPackage
from ovs.extensions.packages.rpm import RpmPackage
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='packagemanager')


class PackageManager(object):
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
                    logger.exception('Error loading Distributor ID: {0}'.format(ex))
                    raise
            return getattr(PackageManager.ImplementationClass, item)

    __metaclass__ = MetaClass

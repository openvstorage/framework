# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Service Factory module
"""
import time

from subprocess import check_output, CalledProcessError
from ovs.extensions.services.upstart import Upstart
from ovs.extensions.services.systemd import Systemd
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='servicemanager')
try:
    from ovs.extensions.services.fleetctl import FleetCtl
    HAS_FLEET_CLIENT = True
except ImportError as ie:
    logger.info('No fleet client detected {0}'.format(ie))
    HAS_FLEET_CLIENT = False

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
                    init_info = check_output('cat /proc/1/comm', shell=True)
                    # All service classes used in below code should share the exact same interface!
                    if 'init' in init_info:
                        version_info = check_output('init --version', shell=True)
                        if 'upstart' in version_info:
                            ServiceManager.ImplementationClass = Upstart
                        else:
                            raise RuntimeError('The ServiceManager is unrecognizable')
                    elif 'systemd' in init_info:
                        if HAS_FLEET_CLIENT is True:
                            try:
                                if ServiceManager.has_fleet():
                                    if ServiceManager._is_fleet_running_and_usable():
                                        pass
                                    else:
                                        ServiceManager.setup_fleet()
                                    ServiceManager.ImplementationClass = FleetCtl
                                else:
                                    ServiceManager.ImplementationClass = Systemd
                            except CalledProcessError as cpe:
                                logger.warning('Could not determine if fleet can be used. {0}'.format(cpe))
                                ServiceManager.ImplementationClass = Systemd
                        else:
                            ServiceManager.ImplementationClass = Systemd
                    else:
                        raise RuntimeError('There was no known ServiceManager detected')
                except Exception as ex:
                    logger.exception('Error loading ServiceManager: {0}'.format(ex))
                    raise
            return getattr(ServiceManager.ImplementationClass, item)

    __metaclass__ = MetaClass

    @staticmethod
    def reload():
        ServiceManager.ImplementationClass = None

    @staticmethod
    def setup_fleet():
        if ServiceManager.has_fleet():
            if ServiceManager._is_fleet_running_and_usable():
                logger.info('Fleet service is running')
                ServiceManager.reload()
                return True
            else:
                check_output('systemctl start fleet', shell=True)
                start = time.time()
                while time.time() - start < 15:
                    if ServiceManager._is_fleet_running_and_usable():
                        logger.info('Fleet service is running and usable')
                        ServiceManager.reload()
                        return True
                    time.sleep(1)
                raise RuntimeError('Can not use fleet to manage services.')

    @staticmethod
    def has_fleet():
        try:
            has_fleetctl_bin = 'fleetctl' in check_output('which fleetctl', shell=True)
            has_fleetd_bin = 'fleetd' in check_output('which fleetd', shell=True)
            return has_fleetctl_bin and has_fleetd_bin
        except CalledProcessError as cpe:
            logger.warning('Could not determine if fleet can be used. {0}'.format(cpe))
            return False

    @staticmethod
    def _is_fleet_running_and_usable():
        try:
            is_fleetd_running = 'active' in check_output('systemctl is-active fleet || true', shell=True).strip()
            if is_fleetd_running:
                is_fleetd_usable = "Error" not in check_output('fleetctl list-machines 2>&1 || true', shell=True).strip()
                return is_fleetd_usable
            return is_fleetd_running
        except CalledProcessError as cpe:
            logger.warning('Could not determine if fleetd is running. {0}'.format(cpe))
            return False
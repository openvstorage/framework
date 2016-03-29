# Copyright 2016 iNuron NV
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
import time

from subprocess import check_output, CalledProcessError
from ovs.extensions.services.upstart import Upstart
from ovs.extensions.services.systemd import Systemd
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
                        if ServiceManager.has_fleet_client() is True and ServiceManager.has_fleet() and \
                                ServiceManager._is_fleet_running_and_usable():
                            from ovs.extensions.services.fleetctl import FleetCtl
                            ServiceManager.ImplementationClass = FleetCtl
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
    def has_fleet_client():
        try:
            from ovs.extensions.services.fleetctl import FleetCtl
            return True
        except ImportError as ie:
            logger.info('No fleet client detected {0}'.format(ie))
            return False
        except ValueError as ve:
            logger.info('Flient client detected, fleet not running {0}'.format(ve))
            return True

    @staticmethod
    def setup_fleet():
        if ServiceManager.has_fleet_client() is False:
            logger.info('Cannot use fleet because the client is not installed')
            return
        if ServiceManager.has_fleet():
            if ServiceManager._is_fleet_running_and_usable():
                logger.info('Fleet service is running')
                ServiceManager.reload()
                return
            else:
                check_output('systemctl start fleet', shell=True)
                start = time.time()
                while time.time() - start < 15:
                    if ServiceManager._is_fleet_running_and_usable():
                        logger.info('Fleet service is running and usable')
                        ServiceManager.reload()
                        return
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
            is_fleetd_usable = "Error" not in check_output('fleetctl list-machines 2>&1 || true', shell=True).strip()
            return is_fleetd_usable
        except CalledProcessError as cpe:
            logger.warning('Could not determine if fleetd is running. {0}'.format(cpe))
            return False

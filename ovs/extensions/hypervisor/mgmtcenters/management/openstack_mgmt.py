# Copyright 2014 Open vStorage NV
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
This module contains OpenStack Cinder commands
"""
import os
import time
import datetime
from ConfigParser import RawConfigParser
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.os.os import OSManager
from ovs.extensions.services.service import ServiceManager
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='openstack_mgmt')


class OpenStackManagement(object):
    """
    Configure/manage openstack services
    """

    def __init__(self, cinder_client):
        self.client = SSHClient('127.0.0.1', username='root')
        self.cinder_client = cinder_client

        self._NOVA_CONF = '/etc/nova/nova.conf'
        self._CINDER_CONF = '/etc/cinder/cinder.conf'
        self._is_openstack = ServiceManager.has_service(OSManager.get_openstack_cinder_service_name(), self.client)
        self._nova_installed = self.client.file_exists(self._NOVA_CONF)
        self._cinder_installed = self.client.file_exists(self._CINDER_CONF)

        try:
            self._is_devstack = 'stack' in str(self.client.run('ps aux | grep SCREEN | grep stack | grep -v grep || true'))
        except SystemExit:  # ssh client raises system exit 1
            self._is_devstack = False
        except Exception:
            self._is_devstack = False

    def is_host_configured(self):
        if (self._is_devstack is False and self._is_openstack is False) or self._cinder_installed is False or self._nova_installed is False:
            logger.warning('No OpenStack nor DevStack installation detected or Cinder and Nova plugins are not installed')
            return False

        with Remote('127.0.0.1', [RawConfigParser], 'root') as remote:
            cfg = remote.RawConfigParser()
            cfg.read([self._CINDER_CONF])
            return cfg.has_option("DEFAULT", "notification_topics") and cfg.get("DEFAULT", "notification_topics") == "notifications"

    def configure_vpool(self, vpool_name):
        if (self._is_devstack is False and self._is_openstack is False) or self._cinder_installed is False or self._nova_installed is False:
            logger.warning('No OpenStack nor DevStack installation detected or Cinder and Nova plugins are not installed')
            return

        logger.debug('configure_vpool {0} started'.format(vpool_name))

        # 1. Configure Cinder driver
        with Remote('127.0.0.1', [RawConfigParser], 'root') as remote:
            changed = False
            cfg = remote.RawConfigParser()
            cfg.read([self._CINDER_CONF])
            if not cfg.has_section(vpool_name):
                changed = True
                cfg.add_section(vpool_name)
                cfg.set(vpool_name, "volume_driver", "cinder.volume.drivers.openvstorage.OVSVolumeDriver")
                cfg.set(vpool_name, "volume_backend_name", vpool_name)
                cfg.set(vpool_name, "vpool_name", vpool_name)
            enabled_backends = []
            if cfg.has_option("DEFAULT", "enabled_backends"):
                enabled_backends = cfg.get("DEFAULT", "enabled_backends").split(", ")
            if vpool_name not in enabled_backends:
                changed = True
                enabled_backends.append(vpool_name)
                cfg.set("DEFAULT", "enabled_backends", ", ".join(enabled_backends))
            if changed is True:
                with open(self._CINDER_CONF, "w") as fp:
                    cfg.write(fp)

        # 2. Create volume type
        if self.cinder_client and not [volume_type for volume_type in self.cinder_client.volume_types.list() if volume_type.name == vpool_name]:
            volume_type = self.cinder_client.volume_types.create(vpool_name)
            volume_type.set_keys(metadata={'volume_backend_name': vpool_name})

        # 3. Restart processes
        self._restart_processes()
        logger.debug('configure_vpool {0} completed'.format(vpool_name))

    def unconfigure_vpool(self, vpool_name, remove_volume_type):
        if self._is_devstack is False and self._is_openstack is False or self._cinder_installed is False:
            logger.warning('No OpenStack nor DevStack installation detected or Cinder plugin is not installed')
            return

        with Remote('127.0.0.1', [RawConfigParser], 'root') as remote:
            changed = False
            cfg = remote.RawConfigParser()
            cfg.read([self._CINDER_CONF])
            if cfg.has_section(vpool_name):
                changed = True
                cfg.remove_section(vpool_name)
            if cfg.has_option("DEFAULT", "enabled_backends"):
                enabled_backends = cfg.get("DEFAULT", "enabled_backends").split(", ")
                if vpool_name in enabled_backends:
                    changed = True
                    enabled_backends.remove(vpool_name)
                    cfg.set("DEFAULT", "enabled_backends", ", ".join(enabled_backends))
            if changed is True:
                with open(self._CINDER_CONF, "w") as fp:
                    cfg.write(fp)

        if remove_volume_type and self.cinder_client:
            for volume_type in self.cinder_client.volume_types.list():
                if volume_type.name == vpool_name:
                    try:
                        self.cinder_client.volume_types.delete(volume_type.id)
                    except Exception as ex:
                        logger.info('Removing volume type from cinder failed with error: {0}'.format(ex))
                        pass

        self._restart_processes()

    def configure_host(self):
        driver_location = OSManager.get_openstack_package_base_path()

        try:
            from cinder import version
            version_string = version.version_string()
            if version_string.startswith('2015.2') or version_string.startswith('2015.1') or version_string.startswith('7.0'):
                version = 'kilo'  # For the moment use K driver
            elif version_string.startswith('2014.2'):
                version = 'juno'
            else:
                raise ValueError('Unsupported cinder version: {0}'.format(version_string))
        except Exception as ex:
            raise ValueError('Cannot determine cinder version: {0}'.format(ex))

        # 1. Get Driver code
        remote_driver = "/opt/OpenvStorage/config/templates/cinder-volume-driver/{0}/openvstorage.py".format(version)
        remote_version = '0.0.0'
        existing_version = '0.0.0'
        try:
            from cinder.volume.drivers import openvstorage
            if hasattr(openvstorage, 'OVSVolumeDriver'):
                existing_version = getattr(openvstorage.OVSVolumeDriver, 'VERSION', '0.0.0')
        except ImportError:
            pass

        with open(remote_driver, 'r') as remote_driver_file:
            for line in remote_driver_file.readlines():
                if 'VERSION = ' in line:
                    remote_version = line.split('VERSION = ')[-1].strip().replace("'", "").replace('"', "")
                    break

        nova_base_path = self._get_base_path('nova')
        cinder_base_path = self._get_base_path('cinder')

        if self._is_devstack is True:
            local_driver = '{0}/volume/drivers/openvstorage.py'.format(cinder_base_path)
        else:
            local_driver = '{0}/cinder/volume/drivers/openvstorage.py'.format(driver_location)

        if remote_version > existing_version:
            logger.debug('Updating existing driver using {0} from version {1} to version {2}'.format(remote_driver, existing_version, remote_version))
            if self._is_devstack is True:
                self.client.run('cp -f {0} /opt/stack/cinder/cinder/volume/drivers'.format(remote_driver))
            else:
                self.client.run('cp -f {0} {1}'.format(remote_driver, local_driver))
        else:
            logger.debug('Using driver {0} version {1}'.format(local_driver, existing_version))

        # 2. Configure users and groups
        users = ['libvirt-qemu', 'stack'] if self._is_devstack is True else OSManager.get_openstack_users()
        for user in users:
            self.client.run('usermod -a -G ovs {0}'.format(user))

        # 3. Apply patches
        if self._is_devstack is True:
            nova_volume_file = '{0}/virt/libvirt/volume.py'.format(nova_base_path)
            nova_driver_file = '{0}/virt/libvirt/driver.py'.format(nova_base_path)
            cinder_brick_initiator_file = '{0}/brick/initiator/connector.py'.format(cinder_base_path)
        else:
            nova_volume_file = '{0}/nova/virt/libvirt/volume.py'.format(driver_location)
            nova_driver_file = '{0}/nova/virt/libvirt/driver.py'.format(driver_location)
            cinder_brick_initiator_file = '{0}/cinder/brick/initiator/connector.py'.format(driver_location)

        with open(nova_volume_file, 'r') as nova_vol_file:
            file_contents = nova_vol_file.readlines()
        if not [line for line in file_contents if line.startswith('class LibvirtFileVolumeDriver(LibvirtBaseVolumeDriver):')]:
            file_contents.extend(['\n', '\n'])
            file_contents.extend([line + '\n' for line in '''class LibvirtFileVolumeDriver(LibvirtBaseVolumeDriver):
    def __init__(self, connection):
        super(LibvirtFileVolumeDriver,
              self).__init__(connection, is_block_dev=False)

    def get_config(self, connection_info, disk_info):
        conf = super(LibvirtFileVolumeDriver,
                     self).get_config(connection_info, disk_info)
        conf.source_type = 'file'
        conf.source_path = connection_info['data']['device_path']
        return conf
'''.splitlines()])
            with open(nova_volume_file, 'w') as nova_vol_file:
                nova_vol_file.writelines(file_contents)

        with open(nova_driver_file, 'r') as nova_driv_file:
            file_contents = nova_driv_file.readlines()
        if not [line for line in file_contents if 'file=nova.virt.libvirt.volume.LibvirtFileVolumeDriver' in line]:
            for line in file_contents:
                if 'local=nova.virt.libvirt.volume.LibvirtVolumeDriver' in line:
                    stripped_line = line.rstrip()
                    whitespaces = len(stripped_line) - len(stripped_line.lstrip())
                    new_line = "{0}'file=nova.virt.libvirt.volume.LibvirtFileVolumeDriver',\n".format(' ' * whitespaces)
                    fc = file_contents[:file_contents.index(line)] + [new_line] + file_contents[file_contents.index(line):]
                    with open(nova_driver_file, 'w') as nova_driv_file:
                        nova_driv_file.writelines(fc)
                    break

        if os.path.exists(cinder_brick_initiator_file):
            # fix brick/upload to glance
            self.client.run("""sed -i 's/elif protocol == "LOCAL":/elif protocol in ["LOCAL", "FILE"]:/g' {0}""".format(cinder_brick_initiator_file))

        # 4. Configure messaging driver
        nova_messaging_driver = 'nova.openstack.common.notifier.rpc_notifier' if version == 'juno' else 'messaging'
        cinder_messaging_driver = 'cinder.openstack.common.notifier.rpc_notifier' if version == 'juno' else 'messaging'

        nova_conf = '/etc/nova/nova.conf'
        cinder_conf = '/etc/cinder/cinder.conf'

        with Remote('127.0.0.1', [RawConfigParser], 'root') as remote:
            for config_file, driver in {nova_conf: nova_messaging_driver,
                                        cinder_conf: cinder_messaging_driver}.iteritems():
                changed = False
                cfg = remote.RawConfigParser()
                cfg.read([config_file])
                if cfg.has_option("DEFAULT", "notification_driver"):
                    if cfg.get("DEFAULT", "notification_driver") != driver:
                        changed = True
                        cfg.set("DEFAULT", "notification_driver", driver)
                else:
                    changed = True
                    cfg.set("DEFAULT", "notification_driver", driver)
                if cfg.has_option("DEFAULT", "notification_topics"):
                    notification_topics = cfg.get("DEFAULT", "notification_topics").split(",")
                    if "notifications" not in notification_topics:
                        notification_topics.append("notifications")
                        changed = True
                        cfg.set("DEFAULT", "notification_topics", ",".join(notification_topics))
                else:
                    changed = True
                    cfg.set("DEFAULT", "notification_topics", "notifications")

                if config_file == nova_conf:
                    for param, value in {'notify_on_any_change': 'True',
                                         'notify_on_state_change': 'vm_and_task_state'}.iteritems():
                        if not cfg.has_option("DEFAULT", param):
                            changed = True
                            cfg.set("DEFAULT", param, value)

                if changed is True:
                    with open(config_file, "w") as fp:
                        cfg.write(fp)

        # 5.. Enable events consumer
        service_name = 'ovs-openstack-events-consumer'
        if not ServiceManager.has_service(service_name, self.client):
            ServiceManager.add_service(service_name, self.client)
            ServiceManager.enable_service(service_name, self.client)
            ServiceManager.start_service(service_name, self.client)

    def unconfigure_host(self):
        pass

    @staticmethod
    def _get_base_path(component):
        exec('import %s' % component, locals())
        module = locals().get(component)
        return os.path.dirname(os.path.abspath(module.__file__))

    def _restart_processes(self):
        """
        Restart the cinder process that uses the OVS volume driver
        - also restarts nova api and compute services
        """
        def stop_screen_process(process_name):
            out = self.client.run('''su stack -c 'screen -S {0} -p {1} -Q select 1>/dev/null; echo $?' '''.format(screen_name, process_name))
            process_screen_exists = out == '0'
            if process_screen_exists:
                self.client.run('''su stack -c 'screen -S {0} -p {1} -X stuff \n' '''.format(screen_name, process_name))
                self.client.run('''su stack -c 'screen -S {0} -p {1} -X kill' '''.format(screen_name, process_name))
            return process_screen_exists

        def start_screen_process(process_name, commands):
            logfile = '{0}/{1}.log.{2}'.format(logdir, process_name, datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d-%H%M%S'))
            logger.debug(self.client.run('''su stack -c 'touch {0}' '''.format(logfile)))
            logger.debug(self.client.run('''su stack -c 'screen -S {0} -X screen -t {1}' '''.format(screen_name, process_name)))
            logger.debug(self.client.run('''su stack -c 'screen -S {0} -p {1} -X logfile {2}' '''.format(screen_name, process_name, logfile)))
            logger.debug(self.client.run('''su stack -c 'screen -S {0} -p {1} -X log on' '''.format(screen_name, process_name)))
            time.sleep(1)
            logger.debug(self.client.run('rm {0}/{1}.log || true'.format(logdir, process_name)))
            logger.debug(self.client.run('ln -sf {0} {1}/{2}.log'.format(logfile, logdir, process_name)))
            for command in commands:
                cmd = '''su stack -c 'screen -S {0} -p {1} -X stuff "{2}\012"' '''.format(screen_name, process_name, command)
                logger.debug(cmd)
                logger.debug(self.client.run(cmd))

        logdir = '/opt/stack/logs'
        screen_name = 'stack'
        if self._is_devstack is True:
            try:
                c_vol_screen_exists = stop_screen_process('c-vol')
                n_cpu_screen_exists = stop_screen_process('n-cpu')
                n_api_screen_exists = stop_screen_process('n-api')
                c_api_screen_exists = stop_screen_process('c-api')

                self.client.run('''su stack -c 'mkdir -p /opt/stack/logs' ''')

                if c_vol_screen_exists:
                    start_screen_process('c-vol', ["export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\" ",
                                                   "newgrp ovs",
                                                   "newgrp stack",
                                                   "umask 0002",
                                                   "/usr/local/bin/cinder-volume --config-file /etc/cinder/cinder.conf & echo \$! >/opt/stack/status/stack/c-vol.pid; fg || echo  c-vol failed to start | tee \"/opt/stack/status/stack/c-vol.failure\" "])
                time.sleep(3)
                if n_cpu_screen_exists:
                    start_screen_process('n-cpu', ["newgrp ovs",
                                                   "newgrp stack",
                                                   "sg libvirtd /usr/local/bin/nova-compute --config-file /etc/nova/nova.conf & echo $! >/opt/stack/status/stack/n-cpu.pid; fg || echo n-cpu failed to start | tee \"/opt/stack/status/stack/n-cpu.failure\" "])
                time.sleep(3)
                if n_api_screen_exists:
                    start_screen_process('n-api', ["export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\" ",
                                                   "/usr/local/bin/nova-api & echo $! >/opt/stack/status/stack/n-api.pid; fg || echo n-api failed to start | tee \"/opt/stack/status/stack/n-api.failure\" "])
                time.sleep(3)
                if c_api_screen_exists:
                    start_screen_process('c-api', ["/usr/local/bin/cinder-api --config-file /etc/cinder/cinder.conf & echo $! >/opt/stack/status/stack/c-api.pid; fg || echo c-api failed to start | tee \"/opt/stack/status/stack/c-api.failure\" "])
                time.sleep(3)
            except SystemExit as se:  # failed command or non-zero exit codes raise SystemExit
                raise RuntimeError(str(se))

        else:
            for service_name in OSManager.get_openstack_services():
                if ServiceManager.has_service(service_name, self.client):
                    try:
                        ServiceManager.restart_service(service_name, self.client)
                    except SystemExit as sex:
                        logger.debug('Failed to restart service {0}. {1}'.format(service_name, sex))
            time.sleep(3)

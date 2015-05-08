# Copyright 2014 CloudFounders NV
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
import time, datetime
from ovs.extensions.generic.sshclient import SSHClient
from ovs.log.logHandler import LogHandler


logger = LogHandler('lib', name='openstack_mgmt')

CINDER_CONF = '/etc/cinder/cinder.conf'
CINDER_OPENSTACK_SERVICE = '/etc/init/cinder-volume.conf'
EXPORT = 'env PYTHONPATH="${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"'
EXPORT_ = 'env PYTHONPATH="\\\${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"'
NOVA_CONF = '/etc/nova/nova.conf'

def file_exists(ssh_client, location):
    """
    Cuisine's file_exists uses "run" which asks for password when run as user stack
    """
    return ssh_client.run_local('test -e "{0}" && echo OK ; true'.format(location)).endswith('OK')


class OpenStackManagement(object):
    """
    Configure/manage openstack services
    """

    def __init__(self, cinder_client):
        self.client = SSHClient('127.0.0.1', username='root')
        self.cinder_client = cinder_client
        self.is_devstack = self._is_devstack()
        self.is_openstack = self._is_openstack()

    @property
    def is_cinder_running(self):
        return self._is_cinder_running()

    @property
    def is_cinder_installed(self):
        return self._is_cinder_installed()

    def configure_vpool(self, vpool_name, mountpoint):
        if (self.is_devstack or self.is_openstack) and self._is_cinder_installed():
            self._get_driver_code()
            self._configure_user_groups()
            self._configure_cinder_driver(vpool_name)
            self._create_volume_type(vpool_name)
            self._patch_etc_init_cindervolume_conf()
            self._apply_patches()
            self._configure_messaging_driver()
            self._enable_openstack_events_consumer()
            self._restart_processes()

    def unconfigure_vpool(self, vpool_name, mountpoint, remove_volume_type):
        if self.is_devstack or self.is_openstack:
            self._unconfigure_cinder_driver(vpool_name)
            if remove_volume_type:
                self._delete_volume_type(vpool_name)
            self._unpatch_etc_init_cindervolume_conf()
            self._restart_processes()

    def _get_version(self):
        """
        Get openstack cinder version
        """
        try:
            from cinder import version
            version = version.version_string()
            if version.startswith('2015.2'):
                return 'kilo'  # For the moment use K driver
            elif version.startswith('2015.1'):
                return 'kilo'
            elif version.startswith('2014.2'):
                return 'juno'
            else:
                raise ValueError('Unknown cinder version: %s' % version)
        except Exception as ex:
            raise ValueError('Cannot determine cinder version: %s' % str(ex))

    def _get_existing_driver_version(self):
        """
        Get VERSION string from existing driver
        """
        try:
            from cinder.volume.drivers import openvstorage
        except ImportError:
            pass
        else:
            if hasattr(openvstorage, 'OVSVolumeDriver'):
                return getattr(openvstorage.OVSVolumeDriver, 'VERSION', '0.0.0')
        return '0.0.0'

    def _get_remote_driver_version(self, location):
        """
        Get VERSION string from downloaded driver
        """
        with open(location, 'r') as f:
            for line in f.readlines():
                if 'VERSION = ' in line:
                    return line.split('VERSION = ')[-1].strip().replace("'", "").replace('"', "")
        return '0.0.0'

    def _get_driver_code(self):
        """
        WGET driver, compare versions, allow local code to be updated from OVS repo until driver is patched upstream
        """
        version = self._get_version()
        remote_driver = "https://bitbucket.org/openvstorage/openvstorage/raw/default/openstack/cinder-volume-driver/%s/openvstorage.py" % version
        temp_location = "/tmp/openvstorage.py"
        self.client.run('wget {0} -P /tmp'.format(remote_driver))

        existing_version = self._get_existing_driver_version()
        remote_version = self._get_remote_driver_version(temp_location)
        if self.is_devstack:
            cinder_base_path = self._get_base_path('cinder')
            local_driver = '{0}/volume/drivers/openvstorage.py'.format(cinder_base_path)
        elif self.is_openstack:
            local_driver = '/usr/lib/python2.7/dist-packages/cinder/volume/drivers/openvstorage.py'
        if remote_version > existing_version:
            logger.debug('Updating existing driver using {0} from version {1} to version {2}'.format(remote_driver, existing_version, remote_version))
            if self.is_devstack:
                self.client.run('cp {0} /opt/stack/cinder/cinder/volume/drivers'.format(temp_location))
            elif self.is_openstack:
                self.client.run('cp {0} /usr/lib/python2.7/dist-packages/cinder/volume/drivers'.format(temp_location))
        else:
            logger.debug('Using driver {0} version {1}'.format(local_driver, existing_version))
        self.client.run('rm {0}'.format(temp_location))

    def _is_devstack(self):
        try:
            return 'stack' in str(self.client.run_local('ps aux | grep SCREEN | grep stack | grep -v grep'))
        except SystemExit:  # ssh client raises system exit 1
            return False

    def _is_openstack(self):
        return os.path.exists(CINDER_OPENSTACK_SERVICE)

    def _is_cinder_running(self):
        if self.is_devstack:
            try:
                return 'cinder-volume' in str(self.client.run_local('ps aux | grep cinder-volume | grep -v grep'))
            except SystemExit:
                return False
        if self.is_openstack:
            try:
                return 'start/running' in str(self.client.run_local('service cinder-volume status'))
            except SystemExit:
                return False
        return False

    def _is_cinder_installed(self):
        try:
            return self.client.file_exists(CINDER_CONF)
        except EOFError:
            return file_exists(self.client, CINDER_CONF)

    def _configure_user_groups(self):
        # Vpool owned by stack / cinder
        # Give access to libvirt-qemu and ovs
        if self.is_devstack:
            self.client.run('usermod -a -G ovs libvirt-qemu')
            self.client.run('usermod -a -G ovs stack')
        elif self.is_openstack:
            self.client.run('usermod -a -G ovs libvirt-qemu')
            self.client.run('usermod -a -G ovs cinder')

    def _unchown_mountpoint(self, mountpoint):
        pass

    def _enable_openstack_events_consumer(self):
        """
        Enable service ovs-openstack-events-consumer
        """
        from ovs.lib.setup import SetupController
        service_name = 'ovs-openstack-events-consumer'
        if not SetupController._has_service(self.client, service_name):
            SetupController._add_service(self.client, service_name)
            SetupController._enable_service(self.client, service_name)
            SetupController._start_service(self.client, service_name)

    def _configure_messaging_driver(self):
        """
        Configure nova and cinder messaging driver
        Restart c-api and n-api
        """
        if not self.client.file_exists(CINDER_CONF):
            return False

        version = self._get_version()
        if version == 'juno':
            messaging_driver = 'nova.openstack.common.notifier.rpc_notifier'
        elif version in ['kilo']:
            messaging_driver = 'messaging'
        else:
            return False
        self.client.run("""python -c '''from ConfigParser import RawConfigParser
changed = False
CINDER_CONF = "{0}"
cfg = RawConfigParser()
cfg.read([CINDER_CONF]);
if cfg.has_option("DEFAULT", "notification_driver"):
    if cfg.get("DEFAULT", "notification_driver") != "{1}":
        changed = True
        cfg.set("DEFAULT", "notification_driver", "{1}")
else:
    changed = True
    cfg.set("DEFAULT", "notification_driver", "{1}")
if cfg.has_option("DEFAULT", "notification_topics"):
    notification_topics = cfg.get("DEFAULT", "notification_topics").split(",")
    if "notifications" not in notification_topics:
        notification_topics.append("notifications")
        changed = True
        cfg.set("DEFAULT", "notification_topics", ",".join(notification_topics))
else:
    changed = True
    cfg.set("DEFAULT", "notification_topics", "notifications")

if changed:
    with open(CINDER_CONF, "w") as fp:
       cfg.write(fp)
'''""".format(CINDER_CONF, messaging_driver))

        if not self.client.file_exists(NOVA_CONF):
            return False

        self.client.run("""python -c '''from ConfigParser import RawConfigParser
changed = False
NOVA_CONF = "{0}"
cfg = RawConfigParser()
cfg.read([NOVA_CONF]);
if cfg.has_option("DEFAULT", "notification_driver"):
    if cfg.get("DEFAULT", "notification_driver") != "{1}":
        changed = True
        cfg.set("DEFAULT", "notification_driver", "{1}")
else:
    changed = True
    cfg.set("DEFAULT", "notification_driver", "{1}")
if cfg.has_option("DEFAULT", "notification_topics"):
    notification_topics = cfg.get("DEFAULT", "notification_topics").split(",")
    if "notifications" not in notification_topics:
        notification_topics.append("notifications")
        changed = True
        cfg.set("DEFAULT", "notification_topics", ",".join(notification_topics))
else:
    changed = True
    cfg.set("DEFAULT", "notification_topics", "notifications")
if not cfg.has_option("DEFAULT", "notify_on_state_change"):
    changed = True
    cfg.set("DEFAULT", "notify_on_state_change", "vm_and_task_state")
if not cfg.has_option("DEFAULT", "notify_on_any_change"):
    changed = True
    cfg.set("DEFAULT", "notify_on_any_change", "True")
if changed:
    with open(NOVA_CONF, "w") as fp:
       cfg.write(fp)
'''""".format(NOVA_CONF, messaging_driver))

    def _configure_cinder_driver(self, vpool_name):
        """
        Adds a new cinder driver, multiple backends
        """
        if not self.client.file_exists(CINDER_CONF):
            return False

        self.client.run("""python -c '''from ConfigParser import RawConfigParser
changed = False
vpool_name = "%s"
CINDER_CONF = "%s"
cfg = RawConfigParser()
cfg.read([CINDER_CONF]);
if not cfg.has_section(vpool_name):
    changed = True
    cfg.add_section(vpool_name)
    cfg.set(vpool_name, "volume_driver", "cinder.volume.drivers.openvstorage.OVSVolumeDriver")
    cfg.set(vpool_name, "volume_backend_name", vpool_name)
    cfg.set(vpool_name, "vpool_name", vpool_name)
enabled_backends = []
if cfg.has_option("DEFAULT", "enabled_backends"):
    enabled_backends = cfg.get("DEFAULT", "enabled_backends").split(", ")
if not vpool_name in enabled_backends:
    changed = True
    enabled_backends.append(vpool_name)
    cfg.set("DEFAULT", "enabled_backends", ", ".join(enabled_backends))
    if changed:
        with open(CINDER_CONF, "w") as fp:
           cfg.write(fp)
'''""" % (vpool_name, CINDER_CONF))

    def _unconfigure_cinder_driver(self, vpool_name):
        """
        Removes a cinder driver, multiple backends
        """
        if not self.client.file_exists(CINDER_CONF):
            return False

        self.client.run("""python -c '''from ConfigParser import RawConfigParser
changed = False
vpool_name = "%s"
CINDER_CONF = "%s"
cfg = RawConfigParser()
cfg.read([CINDER_CONF]);
if cfg.has_section(vpool_name):
    changed = True
    cfg.remove_section(vpool_name)
enabled_backends = []
if cfg.has_option("DEFAULT", "enabled_backends"):
    enabled_backends = cfg.get("DEFAULT", "enabled_backends").split(", ")
if vpool_name in enabled_backends:
    changed = True
    enabled_backends.remove(vpool_name)
    cfg.set("DEFAULT", "enabled_backends", ", ".join(enabled_backends))
    if changed:
        with open(CINDER_CONF, "w") as fp:
           cfg.write(fp)
'''""" % (vpool_name, CINDER_CONF))

    def _restart_processes(self):
        """
        Restart the cinder process that uses the OVS volume driver
        - also restarts nova api and compute services
        """
        if self.is_devstack:
            self._restart_devstack_screen()
        else:
            self._restart_openstack_services()

    def _get_devstack_log_name(self, service, logdir='/opt/stack/logs'):
        """
        Construct a log name in format /opt/stack/logs/h-api-cw.log.2015-04-01-123300
        """
        now_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d-%H%M%S')
        return '{0}/{1}.log.{2}'.format(logdir, service, now_time)

    def _stop_screen_process(self, process_name, screen_name='stack'):
        out = self.client.run('''su stack -c 'screen -S {0} -p {1} -Q select 1>/dev/null; echo $?' '''.format(screen_name, process_name))
        process_screen_exists = out == '0'
        if process_screen_exists:
            self.client.run('''su stack -c 'screen -S {0} -p {1} -X stuff \n' '''.format(screen_name, process_name))
            self.client.run('''su stack -c 'screen -S {0} -p {1} -X kill' '''.format(screen_name, process_name))
        return process_screen_exists

    def _start_screen_process(self, process_name, commands, screen_name='stack', logdir='/opt/stack/logs'):
        logfile = self._get_devstack_log_name(process_name)
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

    def _restart_devstack_screen(self):
        """
        Restart screen processes on devstack
        """
        try:
            c_vol_screen_exists = self._stop_screen_process('c-vol')
            n_cpu_screen_exists = self._stop_screen_process('n-cpu')
            n_api_screen_exists = self._stop_screen_process('n-api')
            c_api_screen_exists = self._stop_screen_process('c-api')

            self.client.run('''su stack -c 'mkdir -p /opt/stack/logs' ''')

            if c_vol_screen_exists:
                self._start_screen_process('c-vol', ["export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\" ",
                                                     "newgrp ovs",
                                                     "newgrp stack",
                                                     "umask 0002",
                                                     "/usr/local/bin/cinder-volume --config-file /etc/cinder/cinder.conf & echo \$! >/opt/stack/status/stack/c-vol.pid; fg || echo  c-vol failed to start | tee \"/opt/stack/status/stack/c-vol.failure\" "])
            time.sleep(3)
            if n_cpu_screen_exists:
                self._start_screen_process('n-cpu', ["newgrp ovs",
                                                     "newgrp stack",
                                                     "sg libvirtd /usr/local/bin/nova-compute --config-file /etc/nova/nova.conf & echo $! >/opt/stack/status/stack/n-cpu.pid; fg || echo n-cpu failed to start | tee \"/opt/stack/status/stack/n-cpu.failure\" "])
            time.sleep(3)
            if n_api_screen_exists:
                self._start_screen_process('n-api', ["export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\" ",
                                                     "/usr/local/bin/nova-api & echo $! >/opt/stack/status/stack/n-api.pid; fg || echo n-api failed to start | tee \"/opt/stack/status/stack/n-api.failure\" "])
            time.sleep(3)
            if c_api_screen_exists:
                self._start_screen_process('c-api', ["/usr/local/bin/cinder-api --config-file /etc/cinder/cinder.conf & echo $! >/opt/stack/status/stack/c-api.pid; fg || echo c-api failed to start | tee \"/opt/stack/status/stack/c-api.failure\" "])
            time.sleep(3)
        except SystemExit as se:  # failed command or non-zero exit codes raise SystemExit
            raise RuntimeError(str(se))
        return self._is_cinder_running()

    def _patch_etc_init_cindervolume_conf(self):
        """
        export PYTHONPATH in the upstart service conf file
        """
        if self.is_openstack and os.path.exists(CINDER_OPENSTACK_SERVICE):
            with open(CINDER_OPENSTACK_SERVICE, 'r') as cinder_file:
                contents = cinder_file.read()
            if EXPORT in contents:
                return True
            contents = contents.replace('\nexec start-stop-daemon', '\n\n{}\nexec start-stop-daemon'.format(EXPORT_))
            logger.debug('changing contents of cinder-volume service conf... %s' % (EXPORT_ in contents))
            self.client.run('cat >%s <<EOF \n%s' % (CINDER_OPENSTACK_SERVICE, contents))

    def _unpatch_etc_init_cindervolume_conf(self):
        """
        remove export PYTHONPATH from the upstart service conf file
        """
        if self.is_openstack and os.path.exists(CINDER_OPENSTACK_SERVICE):
            with open(CINDER_OPENSTACK_SERVICE, 'r') as cinder_file:
                contents = cinder_file.read()
            if not EXPORT in contents:
                return True
            contents = contents.replace(EXPORT_, '')
            logger.debug('fixed contents of cinder-volume service conf... %s' % (EXPORT_ in contents))
            self.client.run('cat >%s <<EOF \n%s' % (CINDER_OPENSTACK_SERVICE, contents))

    def _restart_openstack_services(self):
        """
        Restart services on openstack
        """
        for service_name in ('nova-compute', 'nova-api-os-compute', 'cinder-volume', 'cinder-api'):
            if os.path.exists('/etc/init/{0}.conf'.format(service_name)):
                try:
                    self.client.run('service {0} restart'.format(service_name))
                except SystemExit as sex:
                    logger.debug('Failed to restart service {0}. {1}'.format(service_name, sex))
        time.sleep(3)
        return self._is_cinder_running()

    def _create_volume_type(self, volume_type_name):
        """
        Create a cinder volume type, based on vpool name
        """
        if self.cinder_client:
            volume_types = self.cinder_client.volume_types.list()
            for v in volume_types:
                if v.name == volume_type_name:
                    return False
            volume_type = self.cinder_client.volume_types.create(volume_type_name)
            volume_type.set_keys(metadata={'volume_backend_name': volume_type_name})

    def _apply_patches(self):
        nova_base_path = self._get_base_path('nova')
        cinder_base_path = self._get_base_path('cinder')

        version = self._get_version()
        # fix "blockdev" issue
        if self.is_devstack:
            nova_volume_file = '{0}/virt/libvirt/volume.py'.format(nova_base_path)
            nova_driver_file = '{0}/virt/libvirt/driver.py'.format(nova_base_path)
            cinder_brick_initiator_file = '{0}/brick/initiator/connector.py'.format(cinder_base_path)
        elif self.is_openstack:
            nova_volume_file = '/usr/lib/python2.7/dist-packages/nova/virt/libvirt/volume.py'
            nova_driver_file = '/usr/lib/python2.7/dist-packages/nova/virt/libvirt/driver.py'
            cinder_brick_initiator_file = '/usr/lib/python2.7/dist-packages/cinder/brick/initiator/connector.py'

        self.client.run("""python -c "
import os
version = '%s'
nova_volume_file = '%s'
nova_driver_file = '%s'

with open(nova_volume_file, 'r') as f:
    file_contents = f.readlines()
new_class = '''
class LibvirtFileVolumeDriver(LibvirtBaseVolumeDriver):
    def __init__(self, connection):
        super(LibvirtFileVolumeDriver,
              self).__init__(connection, is_block_dev=False)

    def get_config(self, connection_info, disk_info):
        conf = super(LibvirtFileVolumeDriver,
                     self).get_config(connection_info, disk_info)
        conf.source_type = 'file'
        conf.source_path = connection_info['data']['device_path']
        return conf
'''

patched = False
for line in file_contents:
    if 'class LibvirtFileVolumeDriver(LibvirtBaseVolumeDriver):' in line:
        patched = True
        break

if not patched:
    fc = None
    for line in file_contents[:]:
        if line.startswith('class LibvirtVolumeDriver(LibvirtBaseVolumeDriver):'):
            fc = file_contents[:file_contents.index(line)] + [l+'\\n' for l in new_class.split('\\n')] + file_contents[file_contents.index(line):]
            break
    if fc is not None:
        with open(nova_volume_file, 'w') as f:
            f.writelines(fc)
with open(nova_driver_file, 'r') as f:
    file_contents = f.readlines()
patched = False
for line in file_contents:
    if 'file=nova.virt.libvirt.volume.LibvirtFileVolumeDriver' in line:
        patched = True
        break
if not patched:
    fc = None
    for line in file_contents[:]:
        if 'local=nova.virt.libvirt.volume.LibvirtVolumeDriver' in line:
            fc = file_contents[:file_contents.index(line)] + ['''                  'file=nova.virt.libvirt.volume.LibvirtFileVolumeDriver',\\n'''] + file_contents[file_contents.index(line):]
            break
    if fc is not None:
        with open(nova_driver_file, 'w') as f:
            f.writelines(fc)

" """ % (version, nova_volume_file, nova_driver_file))

        # fix brick/upload to glance
        if os.path.exists(cinder_brick_initiator_file):
            self.client.run("""sed -i 's/elif protocol == "LOCAL":/elif protocol in ["LOCAL", "FILE"]:/g' %s""" % cinder_brick_initiator_file)

    def _delete_volume_type(self, volume_type_name):
        """
        Delete a cinder volume type, based on vpool name
        """
        if self.cinder_client:
            volume_types = self.cinder_client.volume_types.list()
            for v in volume_types:
                if v.name == volume_type_name:
                    try:
                        self.cinder_client.volume_types.delete(v.id)
                    except Exception:
                        pass

    def _get_base_path(self, component):
        exec('import %s' % component, locals())
        module = locals().get(component)
        return os.path.dirname(os.path.abspath(module.__file__))

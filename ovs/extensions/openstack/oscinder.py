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
import time
from ovs.extensions.generic.sshclient import SSHClient

CINDER_CONF = '/etc/cinder/cinder.conf'
CINDER_OPENSTACK_SERVICE = '/etc/init/cinder-volume.conf'
EXPORT = 'env PYTHONPATH="${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"'
EXPORT_ = 'env PYTHONPATH="\\\${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"'


def file_exists(ssh_client, location):
    """
    Cuisine's file_exists uses "run" which asks for password when run as user stack
    """
    return ssh_client.run_local('test -e "{0}" && echo OK ; true'.format(location)).endswith('OK')


class OpenStackCinder(object):
    """
    Represent the Cinder service
    """

    def __init__(self, cinder_password=None, cinder_user='admin', tenant_name='admin', controller_ip='127.0.0.1'):
        self.client = SSHClient.load('127.0.0.1')
        auth_url = 'http://{}:35357/v2.0'.format(controller_ip)
        self.cinder_client = None

        if cinder_password:
            try:
                from cinderclient.v1 import client as cinder_client
            except ImportError:
                pass
            else:
                self.cinder_client = cinder_client.Client(cinder_user, cinder_password, tenant_name, auth_url)
        self.is_devstack = self._is_devstack()
        self.is_openstack = self._is_openstack()

    @property
    def is_cinder_running(self):
        return self._is_cinder_running()

    @property
    def is_cinder_installed(self):
        return self._is_cinder_installed()

    def valid_credentials(self, cinder_password, cinder_user, tenant_name, controller_ip):
        """
        Validate credentials
        """
        try:
            from cinderclient.v1 import client as cinder_client
        except ImportError:
            return False
        else:
            try:
                auth_url = 'http://{}:35357/v2.0'.format(controller_ip)
                cinder_client = cinder_client.Client(cinder_user, cinder_password, tenant_name, auth_url)
                cinder_client.authenticate()
                return True
            except:
                return False

    def _get_version(self):
        """
        Get openstack cinder version
        """
        try:
            from cinder import version
            version = version.version_string()
            if version.startswith('2015.1'):
                return 'kilo'
            elif version.startswith('2014.2'):
                return 'juno'
            else:
                raise ValueError('Unknown cinder version: %s' % version)
        except Exception as ex:
            raise ValueError('Cannot determine cinder version: %s' % str(ex))

    def _get_driver_code(self):
        """
        WGET driver, temporary, until driver is included in openstack
        """
        version = self._get_version()
        driver = "https://bitbucket.org/openvstorage/openvstorage/raw/default/openstack/cinder-volume-driver/%s/openvstorage.py" % version
        print('Using driver %s' % driver)
        if self.is_devstack:
            if os.path.exists('/opt/stack/devstack/cinder'):
                if not os.path.exists('/opt/stack/devstack/cinder/cinder/volume/drivers/openvstorage.py'):
                    self.client.run('wget %s -P /opt/stack/devstack/cinder/cinder/volume/drivers' % driver)
            elif os.path.exists('/opt/stack/cinder'):
                if not os.path.exists('/opt/stack/cinder/cinder/volume/drivers/openvstorage.py'):
                    self.client.run('wget %s -P /opt/stack/cinder/cinder/volume/drivers' % driver)
        elif self.is_openstack:
            if not os.path.exists('/usr/lib/python2.7/dist-packages/cinder/volume/drivers/openvstorage.py'):
                self.client.run('wget %s -P /usr/lib/python2.7/dist-packages/cinder/volume/drivers' % driver)

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

    def configure_vpool(self, vpool_name, mountpoint):
        if self.is_devstack or self.is_openstack:
            self._get_driver_code()
            self._chown_mountpoint(mountpoint)
            self._configure_cinder_driver(vpool_name)
            self._create_volume_type(vpool_name)
            self._patch_etc_init_cindervolume_conf()
            self._apply_patches()
            self._restart_processes()

    def unconfigure_vpool(self, vpool_name, mountpoint, remove_volume_type):
        if self.is_devstack or self.is_openstack:
            self._unchown_mountpoint(mountpoint)
            self._unconfigure_cinder_driver(vpool_name)
            if remove_volume_type:
                self._delete_volume_type(vpool_name)
            self._unpatch_etc_init_cindervolume_conf()
            self._restart_processes()

    def _chown_mountpoint(self, mountpoint):
        if self.is_devstack:
            self.client.run('chown stack "{0}"'.format(mountpoint))
            self.client.run('usermod -a -G stack libvirt-qemu')
        elif self.is_openstack:
            self.client.run('chown cinder "{0}"'.format(mountpoint))
            self.client.run('usermod -a -G cinder libvirt-qemu')

    def _unchown_mountpoint(self, mountpoint):
        self.client.run('chown root "{0}"'.format(mountpoint))

    def _configure_cinder_driver(self, vpool_name):
        """
        Adds a new cinder driver, multiple backends
        """
        if not self.client.file_exists(CINDER_CONF):
            return False

        self.client.run("""python -c '''from ConfigParser import ConfigParser
changed = False
vpool_name = "%s"
CINDER_CONF = "%s"
cfg = ConfigParser()
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

        self.client.run("""python -c '''from ConfigParser import ConfigParser
changed = False
vpool_name = "%s"
CINDER_CONF = "%s"
cfg = ConfigParser()
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

    def _restart_devstack_screen(self):
        """
        Restart c-vol on devstack
        """
        try:
            self.client.run('''su stack -c 'screen -S stack -p c-vol -X kill' ''')
            self.client.run('''su stack -c 'screen -S stack -X screen -t c-vol' ''')
            time.sleep(3)
            self.client.run('''su stack -c 'screen -S stack -p n-api -X kill' ''')
            self.client.run('''su stack -c 'screen -S stack -X screen -t n-api' ''')
            time.sleep(3)
            self.client.run('''su stack -c 'screen -S stack -p n-cpu -X kill' ''')
            self.client.run('''su stack -c 'screen -S stack -X screen -t n-cpu' ''')
            time.sleep(3)
            self.client.run('''su stack -c 'screen -S stack -p c-vol -X stuff "export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\"\012"' ''')
            self.client.run('''su stack -c 'screen -S stack -p c-vol -X stuff "/usr/local/bin/cinder-volume --config-file /etc/cinder/cinder.conf & echo \$! >/opt/stack/status/stack/c-vol.pid; fg || echo  c-vol failed to start | tee \"/opt/stack/status/stack/c-vol.failure\"\012"' ''')
            time.sleep(3)
            self.client.run('''su stack -c 'screen -S stack -p n-cpu -X stuff "sg libvirtd /usr/local/bin/nova-compute --config-file /etc/nova/nova.conf & echo $! >/opt/stack/status/stack/n-cpu.pid; fg || echo n-cpu failed to start | tee \"/opt/stack/status/stack/n-cpu.failure\"\012"' ''')
            time.sleep(3)
            self.client.run('''su stack -c 'screen -S stack -p n-api -X stuff "/usr/local/bin/nova-api & echo $! >/opt/stack/status/stack/n-api.pid; fg || echo n-api failed to start | tee \"/opt/stack/status/stack/n-api.failure\"\012"' ''')
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
            print('changing contents of cinder-volume service conf... %s' % (EXPORT_ in contents))
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
            print('fixed contents of cinder-volume service conf... %s' % (EXPORT_ in contents))
            self.client.run('cat >%s <<EOF \n%s' % (CINDER_OPENSTACK_SERVICE, contents))

    def _restart_openstack_services(self):
        """
        Restart service on openstack
        """
        self.client.run('service nova-compute restart')
        self.client.run('service nova-api-os-compute restart')
        self.client.run('service cinder-volume restart')
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
        # fix run_as_root issue
        if self.is_devstack:
            if os.path.exists('/opt/stack/devstack/cinder'):
                self.client.run('''sed -i 's/run_as_root=True/run_as_root=False/g' /opt/stack/devstack/cinder/cinder/image/image_utils.py''')
            elif os.path.exists('/opt/stack/cinder'):
                self.client.run('''sed -i 's/run_as_root=True/run_as_root=False/g' /opt/stack/cinder/cinder/image/image_utils.py''')
        elif self.is_openstack:
            self.client.run('''sed -i 's/run_as_root=True/run_as_root=False/g' /usr/lib/python2.7/dist-packages/cinder/image/image_utils.py''')

        # fix "blockdev" issue
        if self.is_devstack:
            nova_volume_file = '/opt/stack/nova/nova/virt/libvirt/volume.py'
            nova_driver_file = '/opt/stack/nova/nova/virt/libvirt/driver.py'
        elif self.is_openstack:
            nova_volume_file = '/usr/lib/python2.7/dist-packages/nova/virt/libvirt/volume.py'
            nova_driver_file = '/usr/lib/python2.7/dist-packages/nova/virt/libvirt/driver.py'

        self.client.run("""python -c "
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
    for line in file_contents[:]:
        if line.startswith('class LibvirtVolumeDriver(LibvirtBaseVolumeDriver):'):
            fc = file_contents[:file_contents.index(line)] + [l+'\\n' for l in new_class.split('\\n')] + file_contents[file_contents.index(line):]
            break
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
    for line in file_contents[:]:
        if 'local=nova.virt.libvirt.volume.LibvirtVolumeDriver' in line:
            fc = file_contents[:file_contents.index(line)] + ['''                  'file=nova.virt.libvirt.volume.LibvirtFileVolumeDriver',\\n'''] + file_contents[file_contents.index(line):]
            break
    with open(nova_driver_file, 'w') as f:
        f.writelines(fc)
" """ % (nova_volume_file, nova_driver_file))

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

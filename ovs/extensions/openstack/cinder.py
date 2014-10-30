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
import os, time
from ovs.extensions.generic.sshclient import SSHClient

CINDER_CONF = '/etc/cinder/cinder.conf'
CINDER_OPENSTACK_SERVICE = '/etc/init/cinder-volume.conf'
EXPORT = 'env PYTHONPATH="${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"'
EXPORT_ = 'env PYTHONPATH="\\\${PYTHONPATH}:/opt/OpenvStorage:/opt/OpenvStorage/webapps"'


class OpenStackCinder(object):
    """
    Represent the Cinder service
    """

    def __init__(self, cinder_password = None, cinder_user = 'admin', tenant_name = 'admin', controller_ip = '127.0.0.1'):
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
        self.is_cinder_running = self._is_cinder_running()
        self.is_cinder_installed = self._is_cinder_installed()

    def _get_driver_code(self):
        """
        WGET driver, temporary, until driver is included in openstack
        """
        if self.is_devstack:
            if not os.path.exists('/opt/stack/devstack/cinder/cinder/volume/drivers/ovs_volume_driver.py'):
                self.client.run('wget https://bitbucket.org/openvstorage/openvstorage/raw/tip/openstack/cinder-volume-driver/ovs_volume_driver.py -P /opt/stack/devstack/cinder/cinder/volume/drivers')
        elif self.is_openstack:
            if not os.path.exists('/usr/lib/python2.7/dist-packages/cinder/volume/drivers/ovs_volume_driver.py'):
                self.client.run('wget https://bitbucket.org/openvstorage/openvstorage/raw/tip/openstack/cinder-volume-driver/ovs_volume_driver.py -P /usr/lib/python2.7/dist-packages/cinder/volume/drivers')

    def _is_devstack(self):
        try:
            return 'stack' in str(self.client.run('ps aux | grep SCREEN | grep stack | grep -v grep'))
        except SystemExit: #ssh client raises system exit 1
            return False

    def _is_openstack(self):
        return os.path.exists(CINDER_OPENSTACK_SERVICE)

    def _is_cinder_running(self):
        if self.is_devstack:
            try:
                return 'cinder-volume' in str(self.client.run('ps aux | grep cinder-volume | grep -v grep'))
            except SystemExit:
                return False
        if self.is_openstack:
            try:
                return 'start/running' in str(self.client.run('service cinder-volume status'))
            except SystemExit:
                return False
        raise RuntimeError('Neither devstack nor OpenStack processes detected!')

    def _is_cinder_installed(self):
        try:
            out = self.client.run('ls %s' % CINDER_CONF)
            return 'cannot access' not in out
        except SystemExit:
            return False

    def configure_vpool(self, vpool_name, mountpoint):
        if self.is_devstack or self.is_openstack:
            self._get_driver_code()
            self._chown_mountpoint(mountpoint)
            self._configure_cinder_driver(vpool_name)
            self._create_volume_type(vpool_name)
            self._patch_etc_init_cindervolume_conf()
            self._restart_cinder_process()

    def unconfigure_vpool(self, vpool_name, mountpoint, remove_volume_type):
        if self.is_devstack or self.is_openstack:
            self._unchown_mountpoint(mountpoint)
            self._unconfigure_cinder_driver(vpool_name)
            if remove_volume_type:
                self._delete_volume_type(vpool_name)
            self._unpatch_etc_init_cindervolume_conf()
            self._restart_cinder_process()

    def _chown_mountpoint(self, mountpoint):
        if self.is_devstack:
            self.client.run('chown stack %s' % mountpoint)
        elif self.is_openstack:
            self.client.run('chown cinder %s' % mountpoint)

    def _unchown_mountpoint(self, mountpoint):
        self.client.run('chown root %s' % mountpoint)

    def _configure_cinder_driver(self, vpool_name):
        """
        Adds a new cinder driver, multiple backends
        """
        try:
            self.client.run('ls %s' % CINDER_CONF)
        except SystemExit:
            return False  # no such file or directory

        self.client.run("""python -c '''from ConfigParser import ConfigParser
changed = False
vpool_name = "%s"
CINDER_CONF = "%s"
cfg = ConfigParser()
cfg.read([CINDER_CONF]);
if not cfg.has_section(vpool_name):
    changed = True
    cfg.add_section(vpool_name)
    cfg.set(vpool_name, "volume_driver", "cinder.volume.drivers.ovs_volume_driver.OVSVolumeDriver")
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
        try:
            self.client.run('ls %s' % CINDER_CONF)
        except SystemExit:
            return False  # no such file or directory

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

    def _restart_cinder_process(self):
        """
        Restart the cinder process that uses the OVS volume driver
        """
        if self.is_devstack:
            self._restart_devstack_screen()
        else:
            self._restart_openstack_service()

    def _restart_devstack_screen(self):
        """
        Restart c-vol on devstack
        """
        try:
            self.client.run('''su stack -c 'screen -S stack -p c-vol -X kill' ''')
            self.client.run('''su stack -c 'screen -S stack -X screen -t c-vol' ''')
            time.sleep(3)
            self.client.run('''su stack -c 'screen -S stack -p c-vol -X stuff "export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\"\012"' ''')
            self.client.run('''su stack -c 'screen -S stack -p c-vol -X stuff "cd /opt/stack/cinder && /opt/stack/cinder/bin/cinder-volume --config-file /etc/cinder/cinder.conf & echo \$! >/opt/stack/status/stack/c-vol.pid; fg || echo  c-vol failed to start | tee \"/opt/stack/status/stack/c-vol.failure\"\012"' ''')
            time.sleep(3)
        except SystemExit as sex: # failed command or non-zero exit codes raise SystemExit
            raise RuntimeError(str(sex))
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

    def _restart_openstack_service(self):
        """
        Restart service on openstack
        """
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
            volume_type.set_keys(metadata = {'volume_backend_name': volume_type_name})

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
                    except Exception as ex:
                        pass


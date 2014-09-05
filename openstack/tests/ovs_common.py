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


import os, uuid, time, shutil, subprocess
import inspect #for profiling

#OPENSTACK
from cinder import test
from cinderclient.v1 import client as cinder_client
from glanceclient.v1 import client as glance_client
from keystoneclient.v2_0 import client as keystone_client
from cinderclient import exceptions as cinder_client_exceptions

#OVS
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.lib.storagerouter import StorageRouterController

#CONFIG
from ovs_config import *
from ConfigParser import ConfigParser

class OVSPluginTestCase(test.TestCase):
    """
    Base Class for OVS Test cases
    We need to make sure the OVS model is synced before we can assert
    - a small delay may occur depending on rabbitmq, celery, arakoon and workers
    do not use time.sleep, implement wait_until or retry_until ...
    """
    cleanup = {}
    cinder_client = None
    glance_client = None
    keystone_client = None

    shell_client = None
    current_user_id = os.getuid()

    _profiled = False

    def _debug(self, message):
        """
        Internal method used when profiling code
        """
        if self._profiled:
            stack = inspect.stack()
            caller = stack[1][3]
            print('[TRACE] [%s] (%s) %s' % (caller, time.time(), message))

    def set_profiled(self):
        self._profiled = True

    def __init__(self, *args, **kwargs):
        super(OVSPluginTestCase, self).__init__(*args, **kwargs)
        self._get_cinder_client()

    @classmethod
    def setUpClass(cls):
        cls.runTest = None
        cls()._prepare()
    @classmethod
    def tearDownClass(cls):
        cls.runTest = None
        cls()._cleanup()

    def setUp(self):
        super(OVSPluginTestCase, self).setUp()
        self._get_vpool()

    def _prepare(self):
        restart = False
        if not self._vpool_exists():
            self._create_vpool()
            restart = True
        if self._set_cinder_driver_config() or self._set_cinder_volume_type() or restart:
            self._restart_cinder()

        self._debug('setUp complete')

    def _cleanup(self):
        for cleanup_item in self.cleanup:
            self._debug('tearDown cleanup %s' % str(cleanup_item))
            method, kwargs = self.cleanup[cleanup_item]
            method(**kwargs)

        if VPOOL_CLEANUP:
            self._revert_cinder_config()
            self._remove_cinder_volume_type()
            self._restart_cinder()
            if self._vpool_exists():
                self._remove_vpool()

        self._debug('tearDown complete')

    def register_tearDown(self, key, method, kwargs):
        if key in self.cleanup:
            self._debug('duplicate key %s' % key)
        self.cleanup[key] = (method, kwargs)

    def unregister_tearDown(self, key):
        try:
            del self.cleanup[key]
        except KeyError:
            pass

    # INTERNAL
    def _get_shell_client(self):
        if not self.shell_client:
            def _shell_client(command):
                proc = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
                out, err = proc.communicate()
                self._debug('Command %s Out %s Err %s' % (command, out, err))
                return out, err
            self.shell_client = _shell_client

    def _get_vpool(self):
        self.vpool = VPoolList.get_vpool_by_name(VPOOL_NAME)

    def _vpool_exists(self):
        return VPoolList.get_vpool_by_name(VPOOL_NAME) is not None

    def _mount_volume_by_filename(self, file_name):
        """
        Mount the file_name (.raw) as MOUNT_LOCATION
        Volume must be from an image (a.k.a. not empty, else this fails !)
        https://en.wikibooks.org/wiki/QEMU/Images#Mounting_an_image_on_the_host
        """
        self._debug('mount file %s' % file_name)
        self._get_shell_client()
        self.shell_client('sudo mkdir -p %s' % MOUNT_LOCATION)
        self.shell_client('sudo chown -R stack %s' % MOUNT_LOCATION)
        self.shell_client('sudo modprobe nbd max_part=16')
        self.shell_client('sudo qemu-nbd -c /dev/nbd0 %s/%s' % (VPOOL_MOUNTPOINT, file_name))
        self.shell_client('sudo partprobe /dev/nbd0')
        self.shell_client('sudo mount /dev/nbd0p1 %s' % MOUNT_LOCATION)
        self.register_tearDown('mount%s' % file_name, self._umount_volume, {'file_name': file_name})
        self._debug('mounted %s as %s' % (file_name, MOUNT_LOCATION))

    def _umount_volume(self, file_name):
        """
        Umount MOUNT_LOCATION
        """
        self._debug('umounting %s' % MOUNT_LOCATION)
        self.unregister_tearDown('mount%s' % file_name)
        self._get_shell_client()
        self.shell_client('sudo umount %s' % MOUNT_LOCATION)
        self.shell_client('sudo qemu-nbd -d /dev/nbd0')
        self.shell_client('sudo rm -r %s' % MOUNT_LOCATION)
        self._debug('umounted %s' % MOUNT_LOCATION)

    def _create_vpool(self):
        """
        Needed to actually run tests on
        This is not actually a test of "Add Vpool to OVS",
        so any failure here will be reported as a setUp error and no tests will run
        """
        self._debug('Creating vpool')
        backend_type = 'local'
        fields = ['storage_ip', 'vrouter_port']
        parameters = {'storagerouter_ip': IP,
                      'vpool_name': VPOOL_NAME,
                      'type': 'LOCAL',
                      'mountpoint_bfs': VPOOL_BFS,
                      'mountpoint_temp': VPOOL_TEMP,
                      'mountpoint_md': VPOOL_MD,
                      'mountpoint_cache': VPOOL_CACHE,
                      'storage_ip': '127.0.0.1', #KVM
                      'vrouter_port': VPOOL_PORT
                      }
        StorageRouterController.add_vpool(parameters)
        attempt = 0
        while attempt < 10:
            vpool = VPoolList.get_vpool_by_name(VPOOL_NAME)
            if vpool is not None:
                self._debug('vpool %s created' % VPOOL_NAME)
                try:
                    self._get_shell_client()
                    self.shell_client('chown %s %s' % (self.current_user_id, VPOOL_MOUNTPOINT))
                    os.listdir(VPOOL_MOUNTPOINT)
                    return vpool
                except Exception as ex:
                    #either it doesn't exist, or we don't have permission
                    self._debug('vpool not ready yet %s' % (str(ex)))
                    pass
            attempt += 1
            time.sleep(1)
        raise RuntimeError('Vpool %s was not modeled correctly or did not start.' % VPOOL_NAME)

    def _remove_vpool(self):
        """
        Clean up
        This is not actually a test of "Remove Vpool from OVS",
        so any failure here will be reported as a tearDown error and no cleanup will occur
        """
        self._debug('Removing vpool')
        vpool = VPoolList.get_vpool_by_name(VPOOL_NAME)
        if vpool is None:
            self._debug('already removed')
            return
        for storagedriver_guid in vpool.storagedrivers_guids:
            self._debug('removing storagedriver %s' % storagedriver_guid)
            StorageRouterController.remove_storagedriver(storagedriver_guid)
        attempt = 0
        while attempt < 10:
            vpool = VPoolList.get_vpool_by_name(VPOOL_NAME)
            if vpool is None:
                self._debug('vpool %s deleted' % VPOOL_NAME)
                return
            attempt += 1
            time.sleep(1)
        raise RuntimeError('Vpool %s was not removed correctly.' % VPOOL_NAME)


    def _ovs_devicename_in_vdisklist(self, devicename, exists = True, retry=10):
        if devicename is None:
            raise RuntimeError('Devicename is None, expecting a string.')
        self._debug('find device %s in ovs model' % devicename)
        attempt = 0
        while attempt <= int(retry):
            self._get_vpool()
            vdisk = VDiskList.get_by_devicename_and_vpool(devicename, self.vpool)
            if exists:
                if vdisk is not None:
                    return True
            else:
                if vdisk is None:
                    return True
            self._debug('not found, sleep 1')
            attempt += 1
            time.sleep(1)
        self._debug('still not found, return')
        return False

    def _get_ovs_vdisk_by_devicename(self, devicename):
        if self._ovs_devicename_in_vdisklist(devicename):
            return VDiskList.get_by_devicename_and_vpool(devicename, self.vpool)
        raise ValueError('No such devicename %s in OVS model' % devicename)

    def _ovs_snapshot_id_in_vdisklist_snapshots(self, snapshot_id, retry=10):
        attempt = 0
        while attempt <= int(retry):
            snap_map = dict((vd.guid, vd.snapshots) for vd in VDiskList.get_vdisks())
            for guid, snapshots in snap_map.items():
                snaps = [snap['guid'] for snap in snapshots if snap['guid'] == snapshot_id]
                if len(snaps) == 1:
                    return True
            attempt += 1
            time.sleep(1)
        return False

    def _random_volume_name(self):
        return VOLUME_NAME % str(uuid.uuid4())

    def _random_snapshot_name(self):
        return SNAP_NAME % str(uuid.uuid4())

    def _random_clone_name(self):
        return CLONE_NAME % str(uuid.uuid4())

    def _create_file(self, file_name, contents=''):
        self._debug('create file %s' % file_name)
        self._get_shell_client()
        self.shell_client('sudo touch %s' % file_name)
        self.shell_client('sudo chown stack %s' % file_name)
        with open(file_name, 'w') as fd:
            fd.write(contents)
        self._debug('created file %s' % file_name)

    def _file_exists_on_mountpoint(self, file_name, mountpoint = None):
        self._debug('check if file %s exists' % file_name)
        if not mountpoint:
            mountpoint = VPOOL_MOUNTPOINT
        if not os.path.exists(mountpoint):
            self._debug('location not found %s' % mountpoint)
            return False
        list_dir = os.listdir(mountpoint)
        return file_name in list_dir

    # KEYSTONE
    def _get_keystone_client(self):
        if not self.keystone_client:
            self.keystone_client = keystone_client.Client(username = CINDER_USER,
                                                          password = CINDER_PASS,
                                                          tenant_name = TENANT_NAME,
                                                          auth_url = AUTH_URL)
    # GLANCE
    def _get_glance_client(self):
        if not self.glance_client:
            self._get_keystone_client()
            glance_endpoint = self.keystone_client.service_catalog.url_for(service_type='image',
                                                                            endpoint_type='publicURL')
            self.glance_client = glance_client.Client(glance_endpoint,
                                                      token=self.keystone_client.auth_token)

    def _glance_get_test_image(self):
        self._get_glance_client()
        for image in self.glance_client.images.list():
            if image.name == IMAGE_NAME:
                return image
        raise ValueError('Default test image %s not found' % IMAGE_NAME)

    # CINDER
    def _get_cinder_config(self):
        cinder_conf = '/etc/cinder/cinder.conf'
        cinder_backup = '/etc/cinder/cinder.conf.backup%i'
        i = 0
        while True:
            if os.path.exists(cinder_backup % i):
                i += 1
            else:
                break
        cinder_backup = cinder_backup % i
        shutil.copy(cinder_conf, cinder_backup)
        self._debug('backup cinder.conf to %s' % cinder_backup)
        cfg = ConfigParser()
        cfg.read(cinder_conf)
        return cfg

    def _revert_cinder_config(self):
        cinder_conf = '/etc/cinder/cinder.conf'
        cinder_backup = '/etc/cinder/cinder.conf.backup%i'
        i = 0
        while True:
            if os.path.exists(cinder_backup % i):
                i += 1
            else:
                break
        cinder_backup = cinder_backup % (i-1)
        shutil.move(cinder_backup, cinder_conf)
        self._debug('restore cinder.conf from %s' % cinder_backup)

    def _set_cinder_driver_config(self):
        cfg = self._get_cinder_config()
        changed = False
        config_map = {'volume_driver': 'cinder.volume.drivers.ovs_volume_driver.OVSVolumeDriver',
                      'volume_backend_name': VPOOL_NAME,
                      'vpool_name': VPOOL_NAME,
                      'default_volume_type': 'ovs'}
        for key, value in config_map.items():
            if not cfg.has_option('DEFAULT', key) or cfg.get('DEFAULT', key) != value:
                cfg.set('DEFAULT', key, value)
                changed = True
        if cfg.has_option('DEFAULT', 'enabled_backends'):
            cfg.remove_option('DEFAULT', 'enabled_backends')
            changed = True
        if changed:
            with open('/etc/cinder/cinder.conf', 'w') as fp:
                cfg.write(fp)
        return changed

    def _restart_cinder(self):
        self._get_shell_client()
        if PROCESS == 'screen':
            #restart in screen
            self._debug('stopping cinder screen process')
            #stop
            self.shell_client('''screen -S stack -p c-vol -X kill''')
            self._debug('starting cinder screen process')
            #start
            self.shell_client('''screen -S stack -X screen -t c-vol''')
            time.sleep(3)
            self.shell_client('''screen -S stack -p c-vol -X stuff "export PYTHONPATH=\"${PYTHONPATH}:/opt/OpenvStorage\"\012"''')
            self.shell_client('''screen -S stack -p c-vol -X stuff "cd /opt/stack/cinder && /opt/stack/cinder/bin/cinder-volume --config-file /etc/cinder/cinder.conf & echo \$! >/opt/stack/status/stack/c-vol.pid; fg || echo  c-vol failed to start | tee \"/opt/stack/status/stack/c-vol.failure\"\012"''')
            time.sleep(3)
        elif PROCESS == 'service':
            # restart service
            pass

    def _set_cinder_volume_type(self):
        self._get_cinder_client()
        volume_types = self.cinder_client.volume_types.list()
        for v in volume_types:
            if v.name == VOLUME_TYPE:
                return False
        volume_type = self.cinder_client.volume_types.create(VOLUME_TYPE)
        volume_type.set_keys(metadata = {'volume_backend_name': VPOOL_NAME})
        return True

    def _remove_cinder_volume_type(self):
        self._get_cinder_client()
        volume_types = self.cinder_client.volume_types.list()
        for v in volume_types:
            if v.name == VOLUME_TYPE:
                try:
                    self.cinder_client.volume_types.delete(v.id)
                except Exception as ex:
                    self._debug('cannot delete volume type, reason: %s' % str(ex))

    def _get_cinder_client(self):
        if not self.cinder_client:
            self.cinder_client = cinder_client.Client(CINDER_USER, CINDER_PASS, TENANT_NAME, AUTH_URL)

    def _cinder_get_volume_by_id(self, volume_id):
        """
        Return volume object by id
        """
        self._get_cinder_client()
        return self.cinder_client.volumes.get(volume_id)

    def _cinder_get_snapshot_by_id(self, snapshot_id):
        """
        Return snapshot object by id
        """
        self._get_cinder_client()
        return self.cinder_client.volume_snapshots.get(snapshot_id)

    def _cinder_create_volume(self, name, snapshot_id = None, volume_id = None, image_id = None):
        """
        Creates a volume based partially on DEFAULT values
        - can be created from snapshot or another volume or an image
        """
        self._debug('new volume %s %s %s %s' % (name, snapshot_id, volume_id, image_id))
        self._get_cinder_client()
        volume = self.cinder_client.volumes.create(size = VOLUME_SIZE,
                                                   display_name = name,
                                                   volume_type = VOLUME_TYPE,
                                                   snapshot_id = snapshot_id,
                                                   source_volid = volume_id,
                                                   imageRef = image_id)
        self._cinder_wait_until_volume_state(volume.id, 'available') #allow changes to propagate, model to update
        self._debug('volume %s is available' % name)
        return volume

    def _cinder_delete_volume(self, volume, timeout = 300):
        """
        Delete volume, wait(volume might not be yet in state to be deleted)
        If volume is in-use we will raise error immediately
        """
        self._debug('delete volume %s' % volume.id)
        self._get_cinder_client()
        try:
            volume = self._cinder_get_volume_by_id(volume.id)
        except cinder_client_exceptions.NotFound:
            self._debug('Volume %s not found, already deleted' % volume.id)
            return
        if volume.status == 'in-use':
            raise RuntimeError('Cannot delete volume %s while it is in use' % volume.id)

        self._cinder_wait_until_volume_state(volume.id, status='available')
        self._debug('volume is now available')
        volume = self._cinder_get_volume_by_id(volume.id)
        self.cinder_client.volumes.delete(volume)
        self._cinder_wait_until_volume_not_found(volume.id, timeout)
        self._debug('deleted volume %s' % volume.id)

    def _cinder_create_snapshot(self, volume, snap_name):
        """
        Create default snapshot, wait, parent volume might not be yet available
        """
        self._get_cinder_client()
        self._cinder_wait_until_volume_state(volume.id, status='available')
        snapshot =  self.cinder_client.volume_snapshots.create(volume.id, display_name = snap_name)
        self._cinder_wait_until_snapshot_state(snapshot.id, status='available')
        self._debug('created snapshot')
        return snapshot

    def _cinder_list_snapshots(self):
        self._get_cinder_client()
        return  dict((s.id, s.display_name) for s in self.cinder_client.volume_snapshots.list())

    def _cinder_delete_snapshot(self, snapshot, timeout=300):
        self._get_cinder_client()
        self.cinder_client.volume_snapshots.delete(snapshot)
        self._cinder_wait_until_snapshot_not_found(snapshot.id, timeout)

    def _cinder_reset_volume_state(self, volume, status='available'):
        """
        During some tests volume becomes error_deleting, so it cannot be deleted anymore
        """
        self._debug('reset volume %s to state %s' % (volume.id, status))
        self._get_cinder_client()
        volume = self._cinder_get_volume_by_id(volume.id)
        self.cinder_client.volumes.reset_state(volume, status)
        self._cinder_wait_until_volume_state(volume.id, status)
        self._debug('volume %s is now in state %s' % (volume.id, status))

    def _cinder_reset_snapshot_state(self, snapshot, status='available'):
        """
        During some tests snapshot becomes error_deleting, so it cannot be deleted anymore
        """
        self._debug('reset snapshot %s to state %s' % (snapshot.id, status))
        self._get_cinder_client()
        snapshot = self._cinder_get_snapshot_by_id(snapshot.id)
        self.cinder_client.volume_snapshots.reset_state(snapshot, status)
        self._cinder_wait_until_snapshot_state(snapshot.id, status)
        self._debug('snapshot %s is now in state %s' % (snapshot.id, status))


    # CINDER WAIT
    def _cinder_wait_until_volume_state(self, volume_id, status, timeout_sec=300):
        """
        Wait until volume has status, timeout after X sec
        Expects volume to exist
        """
        self._debug('wait until volume %s is %s' % (volume_id, status))
        start = time.time()
        initial_state = None
        while time.time() < start + timeout_sec:
            volume = self._cinder_get_volume_by_id(volume_id)
            if volume.status == 'error':
                raise RuntimeError('Volume %s in error state' % volume_id)
            if volume.status == status:
                self._debug('volume entered expected state')
                return
            if initial_state is None:
                initial_state = volume.status
            if volume.status != initial_state:
                self._debug('volume %s changed state from %s to %s' % (volume_id, initial_state, volume.status))
                initial_state = volume.status
            time.sleep(1)
        raise RuntimeError('Volume %s is not in state %s after %i seconds, current status %s' % (volume_id, status, timeout_sec, volume.status))

    def _cinder_wait_until_snapshot_state(self, snapshot_id, status, timeout_sec=300):
        """
        Wait until snapshot has status, timeout after X sec
        Expects snapshot to exist
        """
        self._debug('wait until volume %s is %s' % (snapshot_id, status))
        start = time.time()
        while time.time() < start + timeout_sec:
            snapshot = self._cinder_get_snapshot_by_id(snapshot_id)
            if snapshot.status == status:
                return
            self._debug('sleep 1')
            time.sleep(1)
        raise RuntimeError('Snapshot %s is not in state %s after %i seconds, current status %s' % (snapshot_id, status, timeout_sec, snapshot.status))

    def _cinder_wait_until_snapshot_not_found(self, snapshot_id, timeout_sec = 300):
        """
        Wait until snapshot.get returns 404
        Other errors are raised, timeout after X sec
        """
        self._debug('wait until snapshot %s is gone' % (snapshot_id))
        start = time.time()
        while time.time() < start + timeout_sec:
            try:
                snapshot = self._cinder_get_snapshot_by_id(snapshot_id)
            except Exception as ex:
                if hasattr(ex, 'code') and ex.code == 404:
                    return
                else:
                    raise
            self._debug('sleep 1')
            time.sleep(1)
        raise RuntimeError('Snapshot %s is still modeled after %i seconds, current status %s' % (snapshot_id, timeout_sec, snapshot.status))

    def _cinder_wait_until_volume_not_found(self, volume_id, timeout_sec=300):
        """
        Wait until volume.get returns 404
        Other errors are raised, timeout after X sec
        """
        self._debug('wait until volume %s is gone' % (volume_id))
        start = time.time()
        while time.time() < start + timeout_sec:
            try:
                volume = self._cinder_get_volume_by_id(volume_id)
            except Exception as ex:
                if hasattr(ex, 'code') and ex.code == 404:
                    return
                else:
                    raise
            self._debug('sleep 1')
            time.sleep(1)
        raise RuntimeError('Volume %s is still modeled after %i seconds, current status %s' % (volume_id, timeout_sec, volume.status))


    # WRAPPERS
    def _new_volume(self):
        self._debug('create new volume')
        volume_name = self._random_volume_name()
        file_name = '%s.%s' % (volume_name, FILE_TYPE)
        volume = self._cinder_create_volume(volume_name)
        self._debug('created new volume %s' % volume_name)
        self.register_tearDown(volume_name, self._cinder_delete_volume, {'volume': volume})
        self._debug('volume %s created' % volume_name)
        return volume, volume_name, file_name

    def _remove_volume(self, volume, volume_name, timeout=300):
        self._debug('remove volume %s' % volume_name)
        self._cinder_delete_volume(volume, timeout)
        self.unregister_tearDown(volume_name)
        self._cinder_wait_until_volume_not_found(volume.id, timeout)
        self._debug('volume %s removed' % volume_name)

    def _new_snapshot(self, volume):
        self._debug('new snapshot for %s' % volume.id)
        snap_name = self._random_snapshot_name()
        snapshot = self._cinder_create_snapshot(volume, snap_name)
        self.register_tearDown(snap_name, self._cinder_delete_snapshot, {'snapshot': snapshot})
        self._debug('snapshot %s created' % snap_name)
        return snapshot, snap_name

    def _remove_snapshot(self, snap_name, snapshot, timeout=300):
        self._debug('delete snapshot %s' % snap_name)
        self._cinder_delete_snapshot(snapshot, timeout)
        self.unregister_tearDown(snap_name)
        self._cinder_wait_until_snapshot_not_found(snapshot.id, timeout)
        self._debug('snapshot %s deleted' % snap_name)

    def _new_volume_from_snapshot(self, snapshot):
        self._debug('new volume from snapshot %s' % snapshot.id)
        clone_name = self._random_clone_name()
        file_name = '%s.%s' % (clone_name, FILE_TYPE)
        clone_volume = self._cinder_create_volume(clone_name, snapshot_id = snapshot.id)
        self._debug('created new volume %s' % clone_name)
        self.register_tearDown(clone_name, self._cinder_delete_volume, {'volume': clone_volume})
        self._debug('volume %s created' % clone_volume.display_name)
        return clone_volume, clone_name, file_name

    def _new_volume_from_volume(self, volume):
        self._debug('new volume from volume %s' % volume.id)
        clone_name = self._random_clone_name()
        file_name = '%s.%s' % (clone_name, FILE_TYPE)
        clone_volume = self._cinder_create_volume(clone_name, volume_id = volume.id)
        self._debug('created new volume %s' % clone_name)
        self.register_tearDown(clone_name, self._cinder_delete_volume, {'volume': clone_volume})
        self._debug('volume %s created' % clone_volume.display_name)
        return clone_volume, clone_name, file_name

    def _new_volume_from_default_image(self):
        image = self._glance_get_test_image()
        self._debug('new volume from image %s' % image)
        volume_name = self._random_volume_name()
        file_name = '%s.%s' % (volume_name, FILE_TYPE)
        volume = self._cinder_create_volume(volume_name, image_id = image.id)
        self._debug('created new volume %s' % volume_name)
        self.register_tearDown(volume_name, self._cinder_delete_volume, {'volume': volume})
        self._debug('volume %s created' % volume_name)
        return volume, volume_name, file_name



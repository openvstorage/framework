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
OpenStack Cinder driver - interface to OVS api
- uses volumedriver filesystem API (filesystem calls)
- uses Cinder logging (if configured, logging goes to syslog, else it goes to screen)
"""

import os, time

# OVS
from ovs.dal.lists.vpoollist import VPoolList
from ovs.log.logHandler import LogHandler
from ovs.lib.vdisk import VDiskController
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.hybrids.vdisk import VDisk


# Cinder
from oslo.config import cfg
from cinder.openstack.common import log as logging
from cinder.volume import driver
from cinder import utils

VERSION = '1.0.0'
LOG = logging.getLogger(__name__)

ovs_opts = [
    cfg.StrOpt('vpool_name',
               default='',
               help='Vpool to use for volumes - backend is defined by vpool not by us.')
            ]

CONF = cfg.CONF
CONF.register_opts(ovs_opts)

class OVSVolumeDriver(driver.VolumeDriver):
    """
    OVS Volume Driver interface
    Configuration file: /etc/cinder/cinder.conf
    Required parameters in config file:
    - volume_driver = cinder.volume.drivers.ovs_volume_driver.OVSVolumeDriver
    - volume_backend_name = vpoolsaio
    - vpool_name = vpoolsaio
    Required configuration:
    cinder type-create ovs
    cinder type-key ovs set volume_backend_name=vpoolsaio
    """
    VERSION = "1.0.0"

    def __init__(self, *args, **kwargs):
        """
        Init: args, kwargs pass through;
        Options come from CONF
        """
        super(OVSVolumeDriver, self).__init__(*args, **kwargs)
        LOG.info('INIT %s %s %s ' % (CONF.vpool_name, str(args), str(kwargs)))
        self.vpool_name = CONF.vpool_name
        self.vp = VPoolList.get_vpool_by_name(self.vpool_name)
        self.context = None
        self.db = kwargs.get('db', None)

    def _debug_vol_info(self, call, volume):
        """
        Debug print volume info
        """
        vol_info = {}
        for item in dir(volume):
            if not item.startswith('__'):
                try:
                    vol_info[item] = getattr(volume, item)
                except Exception as ex:
                    LOG.info('DEBUG failed %s' % str(ex))
        LOG.debug('[%s] %s' % (call, str(vol_info)))


    # Volume operations

    def initialize_connection(self, volume, connector):
        """
        Allow connection to connector and return connection info.
        """
        self._debug_vol_info("INIT_CONN", volume)

        return {'driver_volume_type': 'local',
                'data': {'vpoolname': self.vpool_name,
                         'device_path': volume.provider_location}}

    def create_volume(self, volume):
        """
        Creates a volume.
        Called on "cinder create ..." or "nova volume-create ..."
        :param volume: volume reference (sqlalchemy Model)
        """
        self._debug_vol_info("CREATE", volume)

        hostname = str(volume.host)
        name = str(volume.display_name)
        mountpoint = self._get_hostname_mountpoint(hostname)
        location = '{}/{}.raw'.format(mountpoint, name)
        size = volume.size
        volume_type = self._get_volume_type_name(volume.volume_type_id)
        if volume_type == 'ovs':
            self._do_create_volume(location, size)
            volume['provider_location'] = location
            return {'provider_location': volume['provider_location']}
        else:
            raise RuntimeError('Cannot create volume of type %s using this driver' % volume_type)

    def delete_volume(self, volume):
        """
        Deletes a logical volume.
        Called on "cinder delete ... "
        :param volume: volume reference (sqlalchemy Model)
        """
        self._debug_vol_info("DELETE", volume)

        location = volume.provider_location
        if location is not None:
            self._do_delete_volume(location)

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """
        Copy image to volume
        Called on "nova volume-create --image-id ..."
        Downloads image from glance server into local .raw
        """
        self._debug_vol_info("CP_IMG_TO_VOL", volume)
        LOG.info("CP_IMG_TO_VOL %s %s" % (image_service, image_id))

        destination_path = volume.provider_location
        fd = None
        if destination_path:
            try:
                fd = open(destination_path, 'rw+')
                LOG.info('CP_IMG_TO_VOL Downloading image to %s' % destination_path)
                image_service.download(context, image_id, fd)
                LOG.info('CP_IMG_TO_VOL Download successful %s' % destination_path)
            except Exception as ex:
                LOG.error('CP_IMG_TO_VOL Internal error %s ' % str(ex))
                self._do_delete_volume(destination_path)
                raise ex
            finally:
                if fd:
                    fd.close()

    # Volume stats

    def get_volume_stats(self, refresh=False):
        """
        Get volume stats
        If 'refresh' is True, update the stats first.
        """
        LOG.info('STATS')

        data = {}
        data['volume_backend_name'] = self.vpool_name
        data['vendor_name'] = 'Open vStorage'
        data['driver_version'] = self.VERSION
        data['storage_protocol'] = 'OVS'

        data['total_capacity_gb'] = 'infinite'
        data['free_capacity_gb'] = 'infinite'
        data['reserved_percentage'] = 0
        data['QoS_support'] = False
        return data

    # Snapshots operations

    def create_snapshot(self, snapshot):
        """
        Creates a snapshot.
        :param snapshot: snapshot reference (sqlalchemy Model)
        """
        self._debug_vol_info('CREATE_SNAP', snapshot)
        volume = snapshot.volume # Model object
        self._debug_vol_info('CREATE_SNAP_VOL', volume)

        hostname = volume.host
        location = volume.provider_location
        volume_type = self._get_volume_type_name(volume.volume_type_id)
        if volume_type == 'ovs':
            ovs_disk = self._find_ovs_model_disk_by_location(location, hostname)
            metadata = {'label': "Cinder snapshot {0}".format(snapshot.display_name),
                        'is_consistent': False,
                        'timestamp': time.time(),
                        'machineguid': ovs_disk.vmachine_guid,
                        'is_automatic': False}

            LOG.debug('CREATE_SNAP %s %s' % (snapshot.display_name, str(metadata)))
            VDiskController.create_snapshot(diskguid = ovs_disk.guid,
                                            metadata = metadata,
                                            snapshotid = str(snapshot.id))
            LOG.debug('CREATE_SNAP OK')
        else:
            raise RuntimeError('Cannot create snapshot for %s volume type using this driver' % volume_type)

    def delete_snapshot(self, snapshot):
        """
        Deletes a snapshot.
        :param snapshot: snapshot reference (sqlalchemy Model)
        """
        self._debug_vol_info('DELETE_SNAP', snapshot)
        volume = snapshot.volume # Model object
        hostname = volume.host
        location = volume.provider_location

        ovs_disk = self._find_ovs_model_disk_by_location(location, hostname)
        LOG.debug('DELETE_SNAP %s' % snapshot.id)
        try:
            VDiskController.delete_snapshot(diskguid = ovs_disk.guid,
                                            snapshotid = str(snapshot.id))
            LOG.debug('DELETE_SNAP OK')
        except Exception as ex:
            LOG.error('DELETE_SNAP Fail %s' % (str(ex)))

    def create_volume_from_snapshot(self, volume, snapshot):
        """
        Creates a volume from a snapshot.
        :param snapshot: snapshot reference (sqlalchemy Model)
        :param volume: volume reference (sqlalchemy Model)
        """
        self._debug_vol_info('CREATE_FROM_SNAP', snapshot)
        self._debug_vol_info('CREATE_FROM_SNAP_V', volume)

        hostname = str(volume.host)
        name = str(volume.display_name)
        mountpoint = self._get_hostname_mountpoint(hostname)
        location = '{}/{}.raw'.format(mountpoint, name)
        size = volume.size

        LOG.debug('CREATE_FROM_SNAP %s %s %s %s' % (name, location, size, snapshot.id))

    # Attach/detach

    def attach_volume(self, context, volume, instance_uuid, host_name,
                      mountpoint):
        """
        Callback for volume attached to instance or host.
        """
        self._debug_vol_info('ATTACH_VOL', volume)
        LOG.info('ATTACH_VOL %s %s %s' % (instance_uuid, host_name, mountpoint))

    def detach_volume(self, context, volume):
        """
        Callback for volume detached.
        """
        self._debug_vol_info('DETACH_VOL', volume)

    # Override parent behavior (NotImplementedError)
    # Not actually implemented

    def create_export(self, context, volume):
        """
        Just to override parent behavior
        """
        self._debug_vol_info("CREATE_EXP", volume)

    def remove_export(self, context, volume):
        """
        Just to override parent behavior.
        """
        self._debug_vol_info("RM_EXP", volume)

    def ensure_export(self, context, volume):
        """
        Just to override parent behavior.
        """
        self._debug_vol_info("ENS_EXP", volume)

    def terminate_connection(self, volume, connector, force):
        """
        Just to override parent behavior.
        """
        self._debug_vol_info("TERM_CONN", volume)
        LOG.info('TERM_CONN %s %s ' % (str(connector), force))

    def check_for_setup_error(self):
        """
        Just to override parent behavior.
        """
        LOG.info('CHECK FOR SETUP ERROR')

    def do_setup(self, context):
        """
        Any initialization the volume driver does while starting
        """
        self._debug_vol_info('SETUP', context)
        self.context = context

    # Internal
    def _get_volume_type_name(self, type_id):
        """
        Get SA volume type name from type_id
        :return name: string, type name
        """
        volume_type = 'UNDEFINED'
        if self.db and self.context:
            volume_type_obj = self.db.volume_type_get(self.context, type_id)
            LOG.debug(str(volume_type_obj))
            volume_type = volume_type_obj.get('name', 'UNKNOWN')
        LOG.info('volume type %s' % volume_type)
        return volume_type

    def _get_hostname_mountpoint(self, hostname):
        """
        Find OVS vsr mountpoint for self.vp and hostname
        :return mountpoint: string, mountpoint
        """
        LOG.debug('[_GET HOSTNAME MOUNTPOINT] Hostname %s' % hostname)
        vsrs = [vsr for vsr in self.vp.vsrs if vsr.serving_vmachine.name == hostname]
        if len(vsrs) == 1:
            LOG.debug('[_GET HOSTNAME MOUNTPOINT] Mountpoint %s' % vsr.mountpoint)
            return vsr.mountpoint
        elif not vsrs:
            raise RuntimeError('Not vsr mountpoint found for Vpool %s and hostname %s' % (self.vpool_name, hostname))

    def _find_ovs_model_disk_by_location(self, location, hostname):
        """
        Find OVS disk object based on location and hostname
        :return disk: OVS DAL model object
        """
        LOG.debug('[_FIND OVS DISK] Location %s, hostname %s' % (location, hostname))
        model_disks = [(vd.guid, "{0}/{1}".format([vsr for vsr in vd.vpool.vsrs if vsr.serving_vmachine.name == hostname][0].mountpoint, vd.devicename)) for vd in VDiskList.get_vdisks()]
        for model_disk in model_disks:
            if model_disk[1] == location:
                LOG.info('[_FIND OVS DISK] Location %s Disk found %s' % (location, model_disk[0]))
                disk = VDisk(model_disk[0])
                return disk
        raise RuntimeError('No disk found for location %s' % location)

    def _do_create_volume(self, location, size):
        """
        Actually create volume
        Calls "truncate" to create sparse raw file
        :param location: string, filepath
        :param size: int(long), size (GB)
        """

        LOG.info('DO_CREATE_VOLUME %s %s' % (location, size))
        if os.path.exists(location):
            msg = 'file already exists at %s' % location
            LOG.error(msg)
            raise RuntimeError(msg)

        utils.execute('truncate', '-s', '%sG' % size, location, run_as_root=True)

    def _do_delete_volume(self, location):
        """
        Actually delete volume
        Calls "rm" to delete raw file
        :param location: string, filepath
        """
        LOG.info('DO_DELETE_VOLUME %s' % (location))
        if not os.path.exists(location):
            msg = 'file already deleted at %s' % location
            LOG.error(msg)
        else:
            utils.execute('rm', location, run_as_root=True)


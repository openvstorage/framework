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
- uses Cinder logging (if configured, logging goes to syslog,
                       else it goes to screen)
"""

import os, time

# OVS
from ovs.dal.lists.vpoollist import VPoolList #pylint: disable=F0401
from ovs.dal.lists.vdisklist import VDiskList #pylint: disable=F0401
from ovs.dal.lists.pmachinelist import PMachineList #pylint: disable=F0401
from ovs.dal.hybrids.vdisk import VDisk #pylint: disable=F0401
from ovs.lib.vdisk import VDiskController #pylint: disable=F0401


# Cinder
from oslo.config import cfg #pylint: disable=F0401
from cinder.openstack.common import log as logging #pylint: disable=F0401
from cinder.volume import driver #pylint: disable=F0401
from cinder import utils #pylint: disable=F0401

VERSION = '1.0.0'
LOG = logging.getLogger(__name__)

OPTS = [
        cfg.StrOpt('vpool_name',
                   default='',
                   help=
        'Vpool to use for volumes - backend is defined by vpool not by us.')
            ]

CONF = cfg.CONF
CONF.register_opts(OPTS)

# Utils
def _debug_vol_info(call, volume):
    """
    Debug print volume info
    """
    vol_info = []
    for item in sorted(dir(volume)):
        if not item.startswith('__'):
            try:
                vol_info.append("%s: %s" % (item, getattr(volume, item)))
            except Exception as ex: #pylint: disable=W0703
                LOG.info('DEBUG failed %s' % str(ex))
    LOG.debug('[%s] %s' % (call, str(vol_info)))

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

    def __init__(self, *args, **kwargs): #pylint: disable=E1002
        """
        Init: args, kwargs pass through;
        Options come from CONF
        """
        super(OVSVolumeDriver, self).__init__(*args, **kwargs)
        LOG.info('INIT %s %s %s ' % (CONF.vpool_name, str(args), str(kwargs)))
        self._vpool_name = CONF.vpool_name
        self._vp = VPoolList.get_vpool_by_name(self._vpool_name)
        self._context = None
        self._db = kwargs.get('db', None)

    # Volume operations

    def initialize_connection(self, volume, connector):
        """
        Allow connection to connector and return connection info.
        """
        _ = connector
        _debug_vol_info("INIT_CONN", volume)

        return {'driver_volume_type': 'local',
                'data': {'vpoolname': self._vpool_name,
                         'device_path': volume.provider_location}}

    def create_volume(self, volume):
        """
        Creates a volume.
        Called on "cinder create ..." or "nova volume-create ..."
        :param volume: volume reference (sqlalchemy Model)
        """
        _debug_vol_info("CREATE", volume)

        hostname = str(volume.host)
        name = volume.display_name
        if not name:
            name = volume.name # volume-de7a8801-864c-4099-84eb-caf965cb173a
        mountpoint = self._get_hostname_mountpoint(hostname)
        location = '{}/{}.raw'.format(mountpoint, name)
        size = volume.size
        volume_type = self._get_volume_type_name(volume.volume_type_id)
        if volume_type == 'ovs':
            LOG.info('DO_CREATE_VOLUME %s %s' % (location, size))
            VDiskController.create_volume(location = location,
                                          size = size)
            volume['provider_location'] = location
            ovs_disk = self._find_ovs_model_disk_by_location(location, hostname)
            ovs_disk.cinder_id = volume.id
            ovs_disk.save()
            return {'provider_location': volume['provider_location']}
        else:
            raise RuntimeError('Cannot create volume of type %s using this driver' % volume_type) #pylint: disable=C0301

    def delete_volume(self, volume):
        """
        Deletes a logical volume.
        Called on "cinder delete ... "
        :param volume: volume reference (sqlalchemy Model)
        """
        _debug_vol_info("DELETE", volume)

        location = volume.provider_location
        if location is not None:
            LOG.info('DO_DELETE_VOLUME %s' % (location))
            VDiskController.delete_volume(location = location)

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """
        Copy image to volume
        Called on "nova volume-create --image-id ..."
        Downloads image from glance server into local .raw
        :param volume: volume reference (sqlalchemy Model)
        """
        _debug_vol_info("CP_IMG_TO_VOL", volume)
        LOG.info("CP_IMG_TO_VOL %s %s" % (image_service, image_id))

        destination_path = volume.provider_location
        if destination_path:
            try:
                with open(destination_path, 'rw+') as _fd:
                    LOG.info('CP_IMG_TO_VOL Downloading image to %s' % destination_path) #pylint: disable=C0301
                    image_service.download(context, image_id, _fd)
                    LOG.info('CP_IMG_TO_VOL Download successful %s' % destination_path) #pylint: disable=C0301
            except Exception as ex:
                LOG.error('CP_IMG_TO_VOL Internal error %s ' % str(ex))
                self._do_delete_volume(destination_path)
                raise ex

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """
        Copy the volume to the specified image.
        Called on "cinder upload-to-image ...volume... ...image-name..."
        Actually exports the image (complete data)
        :param volume: volume reference (sqlalchemy Model)
        """
        _ = context
        _debug_vol_info("CP_VOL_TO_IMG", volume)
        LOG.info("CP_VOL_TO_IMG %s %s" % (image_service, image_meta))

        source_path = volume.provider_location
        if source_path:
            try:
                with open(sourcepath, 'r') as _fd:
                    LOG.info('CP_VOL_TO_IMG Uploading image from %s' % source_path) #pylint: disable=C0301
                    image_service.update(context, image_id, {}, _fd)
                    LOG.info('CP_VOL_TO_IMG Upload successful %s' % source_path) #pylint: disable=C0301
            except Exception as ex:
                LOG.error('CP_VOL_TO_IMG Internal error %s ' % str(ex))
                raise ex

    def create_cloned_volume(self, volume, src_vref):
        """
        Create a cloned volume from another volume.
        Called on "cinder create --source-volid ... "

        Implemented as clone from template

        :param volume: volume reference - target volume (sqlalchemy Model)
        :param src_vref: volume reference - source volume (sqlalchemy Model)
        """
        _debug_vol_info("CLONE_VOL", volume)
        _debug_vol_info("CLONE_VOL", src_vref)

        # source
        hostname = src_vref.host
        location = src_vref.provider_location
        ovs_disk = self._find_ovs_model_disk_by_location(location, hostname)
        pmachineguid = self._find_ovs_model_pmachine_guid_by_hostname(hostname)
        mountpoint = self._get_hostname_mountpoint(hostname)

        # validate is vtemplate
        if ovs_disk.vmachine:
            if not ovs_disk.vmachine.is_vtemplate:
                raise ValueError('Source Volume %s does not belong to an OVS vtemplate. Cannot clone' % location)
        else:
            raise ValueError('Source Volume %s does not belong to an OVS vmachine. Cannot clone' % location)

        # target
        name = volume.display_name
        if not name:
            name = volume.name # volume-de7a8801-864c-4099-84eb-caf965cb173a
            volume.display_name = volume.name

        machinename = ""
        machineguid = None
        if ovs_disk.vmachine:
            machinename = "{}_clone".format(ovs_disk.vmachine.name)
            machineguid = ovs_disk.vmachine_guid

        disk_meta = VDiskController.create_from_template(diskguid = ovs_disk.guid,
                                                         machinename = machinename,
                                                         devicename = str(name),
                                                         pmachineguid = pmachineguid,
                                                         machineguid = machineguid,
                                                         storagedriver_guid=None)
        volume['provider_location'] = '{}/{}'.format(mountpoint,
                                                     disk_meta['backingdevice'])
        vdisk = VDisk(disk_meta['diskguid'])
        vdisk.cinder_id = volume.id
        vdisk.save()
        return {'provider_location': volume['provider_location'],
                'display_name': volume['display_name']}

    # Volumedriver stats

    def get_volume_stats(self, refresh=False):
        """
        Get volumedriver stats
        If 'refresh' is True, update the stats first.
        """
        LOG.info('VOLUMEDRIVER STATS')
        _ = refresh
        data = {}
        data['volume_backend_name'] = self._vpool_name
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
        Called on "nova image-create " or "cinder snapshot-create "
        :param snapshot: snapshot reference (sqlalchemy Model)
        """
        _debug_vol_info('CREATE_SNAP', snapshot)
        volume = snapshot.volume # Model object
        _debug_vol_info('CREATE_SNAP_VOL', volume)

        hostname = volume.host
        location = volume.provider_location
        volume_type = self._get_volume_type_name(volume.volume_type_id)
        if volume_type == 'ovs':
            ovs_disk = self._find_ovs_model_disk_by_location(location, hostname)
            metadata = {'label': "Cinder snapshot {0}".format(snapshot.display_name), #pylint: disable=C0301
                        'is_consistent': False,
                        'timestamp': time.time(),
                        'machineguid': ovs_disk.vmachine_guid,
                        'is_automatic': False}

            LOG.debug('CREATE_SNAP %s %s' % (snapshot.display_name, str(metadata))) #pylint: disable=C0301
            VDiskController.create_snapshot(diskguid = ovs_disk.guid,
                                            metadata = metadata,
                                            snapshotid = str(snapshot.id))
            LOG.debug('CREATE_SNAP OK')
        else:
            raise RuntimeError('Cannot create snapshot for %s volume type using this driver' % volume_type) #pylint: disable=C0301

    def delete_snapshot(self, snapshot):
        """
        Deletes a snapshot.
        :param snapshot: snapshot reference (sqlalchemy Model)
        """
        _debug_vol_info('DELETE_SNAP', snapshot)
        volume = snapshot.volume # Model object
        hostname = volume.host
        location = volume.provider_location

        ovs_disk = self._find_ovs_model_disk_by_location(location, hostname)
        LOG.debug('DELETE_SNAP %s' % snapshot.id)
        try:
            VDiskController.delete_snapshot(diskguid = ovs_disk.guid,
                                            snapshotid = str(snapshot.id))
            LOG.debug('DELETE_SNAP OK')
        except Exception as ex: #pylint: disable=W0703
            LOG.error('DELETE_SNAP Fail %s' % (str(ex)))

    def create_volume_from_snapshot(self, volume, snapshot):
        """
        Creates a volume from a snapshot.
        Called on "cinder create --snapshot-id ..."
        :param snapshot: snapshot reference (sqlalchemy Model)
        :param volume: volume reference (sqlalchemy Model)

        Volume here is just a ModelObject, it doesn't exist physically,
            it will be created by OVS.
        Diskguid to be passed to the clone method is the ovs diskguid of the
            parent of the snapshot with snapshot.id

        NOT SUPPORTED BY OVS VOLUMEDRIVER
        """
        raise NotImplementedError('Volumedriver does not implement Volume Clone from Snapshot')

        _debug_vol_info('CREATE_FROM_SNAP', snapshot)
        _debug_vol_info('CREATE_FROM_SNAP_V', volume)

        hostname = str(volume.host)
        name = volume.display_name
        mountpoint = self._get_hostname_mountpoint(hostname)
        if not name:
            name = volume.name # volume-de7a8801-864c-4099-84eb-caf965cb173a
        size = volume.size

        ovs_disk = self._find_ovs_model_disk_by_snapshot_id(snapshot.id)

        pmachineguid = self._find_ovs_model_pmachine_guid_by_hostname(hostname)
        machinename = ""
        machineguid = None
        if ovs_disk.vmachine:
            machinename = "{}_clone".format(ovs_disk.vmachine.name)
            machineguid = ovs_disk.vmachine_guid
        disk_meta = VDiskController.clone(diskguid = ovs_disk.guid,
                                          snapshotid = str(snapshot.id),
                                          devicename = "{}.raw".format(name),
                                          pmachineguid = pmachineguid,
                                          machinename = machinename,
                                          machineguid = machineguid)
        LOG.info('[CREATE_CLONE_FROM_SNAP] META %s' % str(disk_meta))
        volume['provider_location'] = '{}/{}'.format(mountpoint,
                                                     disk_meta['backingdevice'])
        return {'provider_location': volume['provider_location']}

    # Attach/detach volume to instance/host

    def attach_volume(self, context, volume, instance_uuid, host_name, #pylint: disable=R0913, R0201, C0301
                      mountpoint):
        """
        Callback for volume attached to instance or host.
        """
        _ = context
        _debug_vol_info('ATTACH_VOL', volume)
        LOG.info('ATTACH_VOL %s %s %s' % (instance_uuid, host_name, mountpoint))

    def detach_volume(self, context, volume): #pylint: disable=R0201
        """
        Callback for volume detached.
        """
        _ = context
        _debug_vol_info('DETACH_VOL', volume)

    # Extend

    def extend_volume(self, volume, size_gb):
        """
        Extend volume to new size size_gb
        """
        _debug_vol_info('EXTEND_VOL', volume)
        LOG.info('EXTEND_VOL Size %s' % size_gb)

        raise NotImplementedError('Volumedriver does not implement Volume Extend')

    # Override parent behavior (NotImplementedError)
    # Not actually implemented

    def create_export(self, context, volume): #pylint: disable=R0201
        """
        Just to override parent behavior
        """
        _ = context
        _debug_vol_info("CREATE_EXP", volume)

    def remove_export(self, context, volume): #pylint: disable=R0201
        """
        Just to override parent behavior.
        """
        _ = context
        _debug_vol_info("RM_EXP", volume)

    def ensure_export(self, context, volume): #pylint: disable=R0201
        """
        Just to override parent behavior.
        """
        _ = context
        _debug_vol_info("ENS_EXP", volume)

    def terminate_connection(self, volume, connector, force): #pylint: disable=R0201, C0301
        """
        Just to override parent behavior.
        """
        _debug_vol_info("TERM_CONN", volume)
        LOG.info('TERM_CONN %s %s ' % (str(connector), force))

    def check_for_setup_error(self): #pylint: disable=R0201
        """
        Just to override parent behavior.
        """
        LOG.info('CHECK FOR SETUP ERROR')

    def do_setup(self, context):
        """
        Any initialization the volume driver does while starting
        """
        _debug_vol_info('SETUP', context)
        self._context = context

    # Internal
    def _get_volume_type_name(self, type_id):
        """
        Get SA volume type name from type_id
        :return name: string, type name
        """
        volume_type = 'UNDEFINED'
        if self._db and self._context:
            volume_type_obj = self._db.volume_type_get(self._context, type_id)
            LOG.debug(str(volume_type_obj))
            volume_type = volume_type_obj.get('name', 'UNKNOWN')
        LOG.info('volume type %s' % volume_type)
        return volume_type

    def _get_hostname_mountpoint(self, hostname):
        """
        Find OVS vsr mountpoint for self._vp and hostname
        :return mountpoint: string, mountpoint
        """
        LOG.debug('[_GET HOSTNAME MOUNTPOINT] Hostname %s' % hostname)
        storagedrivers = [vsr for vsr in self._vp.storagedrivers
                          if str(vsr.storagerouter.name) == str(hostname)]
        if len(storagedrivers) == 1:
            LOG.debug('[_GET HOSTNAME MOUNTPOINT] Mountpoint %s' % storagedrivers[0].mountpoint) #pylint: disable=C0301
            return str(storagedrivers[0].mountpoint)
        elif not storagedrivers:
            raise RuntimeError('Not vsr mountpoint found for Vpool %s and hostname %s' % (self._vpool_name, hostname)) #pylint: disable=C0301

    def _find_ovs_model_disk_by_location(self, location, hostname): #pylint: disable=R0201, C0103, C0301
        """
        Find OVS disk object based on location and hostname
        :return disk: OVS DAL model object
        """
        LOG.debug('[_FIND OVS DISK] Location %s, hostname %s' % (location, hostname)) #pylint: disable=C0301
        model_disks = [(vd.guid,
                        "{0}/{1}".format([vsr for vsr in
                                          vd.vpool.storagedrivers
                    if vsr.storagerouter.name == hostname][0].mountpoint,
                        vd.devicename)) for vd in VDiskList.get_vdisks()]
        for model_disk in model_disks:
            if model_disk[1] == location:
                LOG.info('[_FIND OVS DISK] Location %s Disk found %s' % (location, model_disk[0])) #pylint: disable=C0301
                disk = VDisk(model_disk[0])
                return disk
        raise RuntimeError('No disk found for location %s' % location)

    def _find_ovs_model_pmachine_guid_by_hostname(self, hostname):
        """
        Find OVS pmachine guid based on storagerouter name
        :return guid: GUID
        """
        LOG.debug('[_FIND OVS PMACHINE] Hostname %s' % (hostname))
        mapping =  [(pm.guid, str(sr.name)) for pm in PMachineList.get_pmachines() for sr in pm.storagerouters]
        for item in mapping:
            if item[1] == str(hostname):
                LOG.info('[_FIND OVS PMACHINE] Found pmachineguid %s for Hostname %s' % (item[0], hostname)) #pylint: disable=C0301
                return item[0]
        raise RuntimeError('No PMachine guid found for Hostname %s' % hostname)

    def _find_ovs_model_disk_by_snapshot_id(self, snapshotid):
        """
        Find OVS disk object based on snapshot id
        :return disk: OVS DAL model object
        """
        LOG.debug('[_FIND OVS DISK] Snapshotid %s' % snapshotid)
        for disk in VDiskList.get_vdisks():
            snaps_guid = [s['guid'] for s in disk.snapshots]
            if str(snapshotid) in snaps_guid:
                LOG.info('[_FIND OVS DISK] Snapshot id %s Disk found %s' % (snapshotid, disk))
                return disk
        raise RuntimeError('No disk found for snapshotid %s' % snapshotid)


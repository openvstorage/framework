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

import os

# OVS
from ovs.dal.lists.vpoollist import VPoolList
from ovs.log.logHandler import LogHandler

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
        LOG.info('INIT %s' % CONF.vpool_name)
        self.vpool_name = CONF.vpool_name
        self.vp = VPoolList.get_vpool_by_name(self.vpool_name)
        
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
        
    def check_for_setup_error(self):
        """
        Just to override parent behavior.
        """
        pass

    def do_setup(self, context):
        """
        Any initialization the volume driver does while starting
        """
        LOG.info('SETUP %s' % str(context))

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
        
        vsr = [vsr for vsr in self.vp.vsrs if vsr.serving_vmachine.name == hostname][0]
        mountpoint = vsr.mountpoint
        
        location = '{}/{}.raw'.format(mountpoint, name)
        size = volume.size
        
        self._do_create_volume(location, size)
        
        volume['provider_location'] = location
        return {'provider_location': volume['provider_location']}

    def create_volume_from_snapshot(volume, snapshot):
        """
        Creates a volume from a snapshot.
        TODO
        """
        LOG.info('CREATE2 %s %s' % (str(volume), str(snapshot)))

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


    def _do_create_volume(self, location, size):
        """
        Actually create volume
        Calls "truncate" to create sparse raw file
        @param location: string, filepath
        @param size: int(long), size (GB)
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
        @param location: string, filepath
        """
        LOG.info('DO_DELETE_VOLUME %s' % (location))
        if not os.path.exists(location):
            msg = 'file already deleted at %s' % location
            LOG.error(msg)
        else:
            utils.execute('rm', location, run_as_root=True)
        

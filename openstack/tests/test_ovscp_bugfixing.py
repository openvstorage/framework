# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Bugfixing testing:
OVS-1330:
  Creating a volume (from image) with volume size smaller than the
 virtual_size of the image before conversion fails with error
 "Message objects do not support str() because they may contain non-ascii
 characters. Please use unicode() or translate() instead. " due to logging
 in OVSVolumeDriver doing str(ex)
 Test:
  - create volume (of size 1GB) from fedora image (virtual size = 2GB)
 Expect RuntimeError, investigate actual error.

"""
from ovs_common import OVSPluginTestCase, VolumeInErrorState


class OVSBugfixingTestCase(OVSPluginTestCase):
    """
    Image tests
    """

    # TESTS
    def test_ovs_1330(self):
        """
        Creating a volume (from image) with volume size smaller than the
         virtual_size of the image before conversion fails with UnicodeError in c-vol
         RuntimeError in client
        """
        image = self._glance_get_test_image()
        self._debug('new volume from image %s' % image)
        volume_name = self._random_volume_name()
        file_name = '%s.%s' % (volume_name, OVSPluginTestCase.FILE_TYPE)
        self.assertRaises(VolumeInErrorState, self._cinder_create_volume, volume_name, image_id = image.id, size = 1)
        # OVS Cinder driver catches the error and handles the cleanup
        # Not possible to catch the exact type of error, need to read the c-vol screen / cinder logs
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, OVSPluginTestCase.VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')

        volume = self._cinder_get_volume_by_display_name(volume_name)
        self._cinder_reset_volume_state(volume)
        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, OVSPluginTestCase.VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')

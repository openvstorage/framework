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
Image functionality testing for the cinder plugin
- create volume from image
-- downloaded, converted to .raw
-- resulting file is actually raw
- test volume clone (from volume created from image - thin clone)
- create volume from image
-- mount volume locally
-- put a file on it, unmount
-- clone volume from volume created from image (thin clone)
--- also raw file
-- mount clone
-- file exists on clone, unmount

- image upload
-- downloaded, converted to .raw
-- resulting file is actually raw
-- upload as image

* validate on OVS model
* validate on FS (volumedriver filesystem)
"""

from ovs_common import OVSPluginTestCase, VPOOL_MOUNTPOINT, IMAGE_NAME, MOUNT_LOCATION

class OVSPluginImageTestCase(OVSPluginTestCase):
    """
    Image tests
    """

    # TESTS
    def test_create_delete_volume_from_default_image(self):
        """
        Create a volume from image using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME --image-id IMAGE_ID VOLUME_SIZE
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         volume image metadata points to IMAGE_NAME
         vdisk modeled in OVS
         file removed from mountpoint
         vdisk removed from OVS
        CLEANUP:
         none
        """
        volume, volume_name, file_name = self._new_volume_from_default_image()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')
        self.assertTrue(volume.volume_image_metadata['image_name'] == IMAGE_NAME, 'Wrong volume image metadata %s' % volume.volume_image_metadata)

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')

    def test_create_delete_volume_from_default_image_upload_to_glance(self):
        """
        Create a volume from image using the cinder client, upload it to glance
        Delete the volume, delete the image
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME --image-id IMAGE_ID VOLUME_SIZE
         cinder upload-to-image <VOLID> <IMAGENAME>
         cinder delete <VOLID>
         glance image-delete
        ASSERTS:
         file exists on mountpoint
         volume image metadata points to IMAGE_NAME
         vdisk modeled in OVS
         file removed from mountpoint
         vdisk removed from OVS
        CLEANUP:
         none
        """

        volume, volume_name, file_name = self._new_volume_from_default_image()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')
        self.assertTrue(volume.volume_image_metadata['image_name'] == IMAGE_NAME, 'Wrong volume image metadata %s' % volume.volume_image_metadata)

        image, image_name = self._upload_volume_to_image(volume)
        self.assertTrue(image_name in self._glance_list_images_names(), 'Image not uploaded to glance')
        image_info = self._glance_get_image_by_name(image_name)
        self.assertFalse(image_info.status == 'error', 'Image uploaded with errors')

        self._remove_image(image_name)
        self.assertFalse(image_name in self._glance_list_images_names(), 'Image not removed from glance')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')

    def test_create_delete_volume_check_file_from_default_image(self):
        """
        Create a volume from image using the cinder client
         Mount the volume locally (loopback)
          Create a file
         Unmount the volume
         Clone the volume
          Mount the clone
           Check the file exists
          Unmount the volume
         Delete the clone
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME --image-id IMAGE_ID VOLUME_SIZE
          mount volume
          touch file
          unmount volume
         cinder create --source-volid <VOLUME_ID> --display-name CLONE_NAME VOLUME_SIZE
          mount clone
          cat file
          unmount clonse
         cinde delete <CLONE>
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         volume mounted and readable/writable at new mountpoint
         volume unmounted
         clone vdisk modeled in OVS
         clone file exists on mountpoint
         clone mounted and readable/writable at new mountpoint
         created file exists at new mountpoint
         clone unmounted
         clone vdisk deleted from OVS
         clone file removed from mountpoint
         original vdisk deleted from OVS
         file removed from mountpoint
        CLEANUP:
         -none
        """
        volume, volume_name, file_name = self._new_volume_from_default_image()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')
        self.assertTrue(volume.volume_image_metadata['image_name'] == IMAGE_NAME, 'Wrong volume image metadata %s' % volume.volume_image_metadata)

        self._mount_volume_by_filename(file_name)

        self._create_file('%s/autotest_generated_file' % MOUNT_LOCATION)
        self.assertTrue(self._file_exists_on_mountpoint('autotest_generated_file', MOUNT_LOCATION), 'Generated file is not present in %s' % MOUNT_LOCATION)

        self._umount_volume(file_name)
        self.assertFalse(self._file_exists_on_mountpoint('autotest_generated_file', MOUNT_LOCATION), 'Generated file is still present in %s' % MOUNT_LOCATION)

        clone, clone_name, clone_file_name = self._new_volume_from_volume(volume)
        self.assertTrue(self._file_exists_on_mountpoint(clone_file_name), 'File %s not created on mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name), 'Device not modeled in OVS')

        self._mount_volume_by_filename(clone_file_name)

        self.assertTrue(self._file_exists_on_mountpoint('autotest_generated_file', MOUNT_LOCATION), '(clone) Generated file is not present in %s' % MOUNT_LOCATION)

        self._umount_volume(clone_file_name)

        self._remove_volume(clone, clone_name)
        self.assertFalse(self._file_exists_on_mountpoint(clone_file_name), 'File %s not deleted from mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name, exists=False), 'Device still modeled in OVS')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')


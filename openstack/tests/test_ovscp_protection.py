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
Test protection features of the volumedriver
- not allowed to delete a volume if it has clones
* since this is a thin clone, atm the source volume cannot be deleted
* once the volumedriver will support full clone (emancipated) this test will be obsolete
-- via cinder plugin
-- via filesystem (not a test for cinder ?)
- not allowed to delete a snapshot if it has clones
-- via cinder plugin
- not allowed to delete a volume if it is in use (mounted or in use by qemu)
-- via cinder plugin
-- via filesystem (not a test for cinder ?)

* validate on OVS model
* validate on FS (volumedriver filesystem)
"""

from ovs_common import OVSPluginTestCase, VPOOL_MOUNTPOINT, cinder_client_exceptions

class OVSPluginProtectionTestCase(OVSPluginTestCase):
    """
    Protection features tests
    """

    #TESTS
    def test_not_allowed_to_delete_volume_with_clones(self):
        """
        Create a volume using the cinder client - THIN clone via snapshot
         Create a volume from that volume using the cinder client
         TRY to delete the original volume => expect failure
         Delete the cloned volume using the cinder client
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
         cinder create --source-volid <VOLUME_ID> --display-name CLONE_NAME VOLUME_SIZE
         cinder delete <VOLID>
         cinder delete <CLONEID>
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         clone vdisk modeled in OVS
         original file not removed from mountpoint
         vdisk still modeled in OVS
         clone vdisk deleted from OVS
         original vdisk deleted from OVS
         file removed from mountpoint
         vdisk no longer modeled in OVS
        CLEANUP:
         -none
        """
        self._debug('started test')
        volume, volume_name, file_name = self._new_volume()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')

        clone, clone_name, clone_file_name = self._new_volume_from_volume(volume)
        self.assertTrue(self._file_exists_on_mountpoint(clone_file_name), 'File %s not created on mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name), 'Device not modeled in OVS')


        self.assertRaises(RuntimeError, self._remove_volume, volume, volume_name, 5)
        self._cinder_reset_volume_state(volume)
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s accidentally removed from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device accidentally removed from OVS')


        self._remove_volume(clone, clone_name)
        self.assertFalse(self._file_exists_on_mountpoint(clone_file_name), 'File %s not deleted from mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name, exists=False), 'Device still modeled in OVS')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')
        self._debug('ended test')

    def test_not_allowed_to_delete_clone_of_snapshot(self):
        """
        Create a volume using the cinder client
         Create a snapshot using the cinder client
          Create a volume from that snapshot using the cinder client
          TRY to delete the original volume => expect failure
          TRY to delete the snapshot => expect failure
          Delete the cloned volume using the cinder client
         Delete the snapshot using the cinder client
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
         cinder snapshot-create --display-name SNAP_NAME <VOLID>
         cinder create --snapshot-id <SNAP_ID> --display-name CLONE_NAME VOLUME_SIZE
         cinder delete <VOLID>
         cinder snapshot-delete <SNAPID>
         cinder delete <CLONEID>
         cinder snapshot-delete <SNAPID>
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         snapshot exists in cinder DB
         clone vdisk modeled in OVS
         original file not removed from mountpoint
         snapshot not removed from OVS model
         clone vdisk deleted from OVS
         snapshot deleted from cinder DB
         snapshot removed from OVS
         file removed from mountpoint
         vdisk no longer modeled in OVS
        CLEANUP:
         -none
        """
        self._debug('started test')
        volume, volume_name, file_name = self._new_volume()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')

        snapshot, snap_name = self._new_snapshot(volume)
        cinder_snapshots = self._cinder_list_snapshots()
        self.assertTrue(snapshot.id in cinder_snapshots.keys(), 'Snapshot not modeled in Cinder')
        snapshot_name = cinder_snapshots[snapshot.id]
        self.assertTrue(snapshot_name == snap_name, 'Wrong name for snapshot %s' % snapshot_name)
        self.assertTrue(self._ovs_snapshot_id_in_vdisklist_snapshots(snapshot.id), 'Snapshot not modeled in OVS')

        clone, clone_name, clone_file_name = self._new_volume_from_snapshot(snapshot)
        self.assertTrue(self._file_exists_on_mountpoint(clone_file_name), 'File %s not created on mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name), 'Device not modeled in OVS')


        self.assertRaises(cinder_client_exceptions.BadRequest, self._remove_volume, volume, volume_name, 5)
        self._cinder_reset_volume_state(volume)
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s accidentally removed from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device accidentally removed from OVS')

        self.assertRaises(RuntimeError, self._remove_snapshot, snap_name, snapshot, 5)
        self._cinder_reset_snapshot_state(snapshot)
        cinder_snapshots = self._cinder_list_snapshots()
        self.assertTrue(snapshot.id in cinder_snapshots.keys(), 'Snapshot accidentally removed from Cinder')


        self._remove_volume(clone, clone_name)
        self.assertFalse(self._file_exists_on_mountpoint(clone_file_name), 'File %s not deleted from mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name, exists=False), 'Device still modeled in OVS')

        self._remove_snapshot(snap_name, snapshot, force = True)
        cinder_snapshots = self._cinder_list_snapshots()
        self.assertFalse(snapshot.id in cinder_snapshots.keys(), 'Snapshot still modeled in Cinder')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')
        self._debug('ended test')

    def test_not_allowed_to_delete_volume_local_mounted(self):
        """
        Create a volume from image using the cinder client
         Mount the volume locally (loopback)
         TRY to delete the original volume => expect failure
         Unmount the volume
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME --image-id IMAGE_ID VOLUME_SIZE
         mount
         cinder delete <VOLID>
         unmount
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         volume mounted and readable/writable at new mountpoint
         original file not removed from mountpoint
         vdisk still modeled in OVS
         volume unmounted
         original vdisk deleted from OVS
         file removed from mountpoint
         vdisk no longer modeled in OVS
        CLEANUP:
         -none
        """

        self._debug('started test')
        volume, volume_name, file_name = self._new_volume_from_default_image()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')

        self._mount_volume_by_filename(file_name)

        self.assertRaises(RuntimeError, self._remove_volume, volume, volume_name, 5)
        self._cinder_reset_volume_state(volume)
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s accidentally removed from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device accidentally removed from OVS')

        self._umount_volume(file_name)

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')
        self._debug('ended test')

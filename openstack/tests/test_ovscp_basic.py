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
Basic functionality unit tests for the OVS Cinder Plugin
- create volume
- delete volume
- create snapshot
- delete snapshot
- clone from existing snapshot
- clone from volume (via new snapshot)

* validate on OVS model
* validate on FS (volumedriver filesystem)
"""

from ovs_common import OVSPluginTestCase, VPOOL_MOUNTPOINT

class OVSPluginBasicTestCase(OVSPluginTestCase):
    """
    Basic tests - the real thing, takes some time
    """

    # TESTS
    def test_create_volume(self):
        """
        Create a volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
        ASSERTS:
         file exists on mountpoint
         vdisk modelled in OVS
        CLEANUP:
         delete volume
        """
        volume, volume_name, file_name = self._new_volume()

        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')

    def test_create_delete_volume(self):
        """
        Create a volume using the cinder client, delete it
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         file removed from mountpoint
         vdisk no longer modeled in OVS
        CLEANUP:
         -none
        """
        volume, volume_name, file_name = self._new_volume()
        self.assertTrue(self._file_exists_on_mountpoint(file_name), 'File %s not created on mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name), 'Device not modeled in OVS')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')

    def test_create_delete_volume_snapshots(self):
        """
        Create a volume using the cinder client
         Create a snapshot using the cinder client
         List the snapshot using the cinder client
         Delete the snapshot using the cinder client
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
         cinder snapshot-create --display-name SNAP_NAME <VOLID>
         cinder snapshot-list | grep SNAP_NAME
         cinder snapshot-delete <SNAPID>
         cinder snapshot-list | grep SNAP_NAME
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         snapshot exists in cinder DB
         snapshot modeled in OVS
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

        self._remove_snapshot(snap_name, snapshot, force = True)
        cinder_snapshots = self._cinder_list_snapshots()
        self.assertFalse(snapshot.id in cinder_snapshots.keys(), 'Snapshot still modeled in Cinder')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')
        self._debug('ended test')

    def test_create_delete_volume_clone_delete_from_snapshot(self):
        """
        Create a volume using the cinder client
         Create a snapshot using the cinder client
         List the snapshot using the cinder client
          Create a volume from that snapshot using the cinder client
          Delete the cloned volume using the cinder client
         Delete the snapshot using the cinder client
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
         cinder snapshot-create --display-name SNAP_NAME <VOLID>
         cinder snapshot-list | grep SNAP_NAME
         cinder create --snapshot-id <SNAP_ID> --display-name CLONE_NAME VOLUME_SIZE
         cinder delete <CLONEID>
         cinder snapshot-delete <SNAPID>
         cinder snapshot-list | grep SNAP_NAME
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         snapshot exists in cinder DB
         snapshot modeled in OVS
         clone vdisk modeled in OVS
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

    def test_create_delete_volume_clone_delete_from_volume(self):
        """
        Create a volume using the cinder client - THIN clone via snapshot
         Create a volume from that volume using the cinder client
         Delete the cloned volume using the cinder client
        Delete the volume using the cinder client
        COMMAND:
         cinder create --volume-type ovs --display-name VOLUME_NAME VOLUME_SIZE
         cinder create --source-volid <VOLUME_ID> --display-name CLONE_NAME VOLUME_SIZE
         cinder delete <CLONEID>
         cinder delete <VOLID>
        ASSERTS:
         file exists on mountpoint
         vdisk modeled in OVS
         clone vdisk modeled in OVS
         OVS snapshot created (since it's a new disk it has no default snapshot) - no cinder snapshot for this
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

        # assert snapshot created for volume
        vdisk = self._get_ovs_vdisk_by_devicename(file_name)
        self.assertTrue(len(vdisk.snapshots) > 0, 'No snapshots created for source disk, expected at least 1')

        self._remove_volume(clone, clone_name)
        self.assertFalse(self._file_exists_on_mountpoint(clone_file_name), 'File %s not deleted from mountpoint %s ' % (clone_file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(clone_file_name, exists=False), 'Device still modeled in OVS')

        self._remove_volume(volume, volume_name)
        self.assertFalse(self._file_exists_on_mountpoint(file_name), 'File %s not deleted from mountpoint %s ' % (file_name, VPOOL_MOUNTPOINT))
        self.assertTrue(self._ovs_devicename_in_vdisklist(file_name, exists=False), 'Device still modeled in OVS')
        self._debug('ended test')

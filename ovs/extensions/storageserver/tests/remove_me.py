import time

from ovs.lib.helpers.toolbox import Toolbox
from ovs_extensions.generic.toolbox import ExtensionsToolbox

from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.hybrids.vdisk import VDisk
from ovs.lib.vdisk import VDiskController
import unittest


vdisk_name = 'vd1'
clone1_name = vdisk_name+'_clone'
clone2_name = clone1_name + '_clone'
# create vdisks
VDiskController.create_new(volume_name='vd1', volume_size=1024 ** 3, storagedriver_guid='a4eb1334-cb97-464f-96d3-796a1d124ce8')
vd = VDiskList.get_vdisks()[0]
vd_guid = vd.guid
vol_id = vd.volume_id
vd_clone_guid = VDiskController.clone(vd.guid, clone1_name).get('vdisk_guid')
vol_id_clone = VDisk(vd_clone_guid).volume_id
vd_clone_clone_guid = VDiskController.clone(vd_clone_guid, clone2_name).get('vdisk_guid')
vol_id_clone_clone = VDisk(vd_clone_clone_guid).volume_id

# print 'testfile:                ',len(VDiskList.get_vdisks())
# for vd in VDiskList.get_vdisks():
#     try:
#         print 'testfile:parents       ',vd.parent_vdisk.name
#     except AttributeError:
#         pass
#clean vdisks
VDiskController.clean_vdisk_from_model(VDisk(vd_clone_clone_guid))
VDiskController.clean_vdisk_from_model(VDisk(vd_clone_guid))

#assert no vdisks in model
# print 'testfile:               ',len(VDiskList.get_vdisks())

# regenerate
VDiskController.sync_with_reality()

#assert vdisks in model
# print 'testfile:            ', len(VDiskList.get_vdisks())

#assert they have parents
for vd in VDiskList.get_vdisks():
    try:
        if vd.parent_vdisk.name:
            print 'testfile: parent ({0}) found for {1}'.format(vd.parent_vdisk.name, vd.name)
    except AttributeError:
        print 'testfile: parents: None detected for vdisk', vd.name
#remove
VDiskController.delete(VDiskList.get_vdisk_by_volume_id(vol_id_clone_clone).guid)
VDiskController.delete(VDiskList.get_vdisk_by_volume_id(vol_id_clone).guid)
VDiskController.delete(VDiskList.get_vdisk_by_volume_id(vol_id).guid)

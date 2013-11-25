# license see http://www.openvstorage.com/licenses/opensource/
"""
VDiskList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vdisk import VDisk


class VDiskList(object):
    """
    This VDiskList class contains various lists regarding to the VDisk class
    """

    @staticmethod
    def get_vdisks():
        """
        Returns a list of all VDisks
        """
        vdisks = DataList(key='vdisks',
                          query={'object': VDisk,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': []}}).data
        return DataObjectList(vdisks, VDisk)

    @staticmethod
    def get_vdisk_by_volumeid(volumeid):
        """
        Returns a list of all VDisks based on a given volumeid
        """
        # pylint: disable=line-too-long
        vdisks = DataList(key='vdisk_%s' % volumeid,
                          query={'object': VDisk,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': [('volumeid', DataList.operator.EQUALS, volumeid)]}}).data  # noqa
        # pylint: enable=line-too-long
        if vdisks:
            return DataObjectList(vdisks, VDisk)
        return None

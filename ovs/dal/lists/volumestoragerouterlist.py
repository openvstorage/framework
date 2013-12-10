# license see http://www.openvstorage.com/licenses/opensource/
"""
VDiskList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter


class VolumeStorageRouterList(object):
    """
    This VolumeStorageRouterList class contains various lists regarding to the VolumeStorageRouter class
    """

    @staticmethod
    def get_volumestoragerouters():
        """
        Returns a list of all VolumeStorageRouters
        """
        volumestoragerouters = DataList({'object': VolumeStorageRouter,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': []}}).data
        return DataObjectList(volumestoragerouters, VolumeStorageRouter)

    @staticmethod
    def get_volumestoragerouter_by_vsrid(vsrid):
        """
        Returns a list of all VolumeStorageRouters based on a given vsrid
        """
        # pylint: disable=line-too-long
        volumestoragerouters = DataList({'object': VolumeStorageRouter,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('vsrid', DataList.operator.EQUALS, vsrid)]}}).data
        # pylint: enable=line-too-long
        if volumestoragerouters:
            return DataObjectList(volumestoragerouters, VolumeStorageRouter)[0]
        return None

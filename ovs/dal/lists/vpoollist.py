# license see http://www.openvstorage.com/licenses/opensource/
"""
VPoolList
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vpool import VPool


class VPoolList(object):
    """
    This VPoolList class contains various lists regarding to the VPool class
    """

    @staticmethod
    def get_vpools():
        """
        Returns a list of all VPools
        """
        vpools = DataList({'object': VPool,
                           'data': DataList.select.DESCRIPTOR,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': []}}).data
        return DataObjectList(vpools, VPool)

    @staticmethod
    def get_vpool_by_name(vpool_name):
        """
        Returns all VPools which have a given name
        """
        vpools = DataList({'object': VPool,
                           'data': DataList.select.DESCRIPTOR,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('name', DataList.operator.EQUALS, vpool_name)]}}).data
        if vpools:
            return DataObjectList(vpools, VPool)
        return None

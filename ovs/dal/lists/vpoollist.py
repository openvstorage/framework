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
        vpools = DataList(key='vpools',
                          query={'object': VPool,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': []}}).data
        return DataObjectList(vpools, VPool)

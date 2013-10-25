from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vpool import vPool


class VPoolList(object):
    @staticmethod
    def get_vpools():
        vpools = DataList(key   = 'vpools',
                             query = {'object': vPool,
                                      'data': DataList.select.DESCRIPTOR,
                                      'query': {'type': DataList.where_operator.AND,
                                                'items': []}}).data
        return DataObjectList(vpools, vPool)

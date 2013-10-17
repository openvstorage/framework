from ovs.dal.datalist import DataList
from ovs.dal.hybrids.vdisk import vDisk
from ovs.dal.dataobjectlist import DataObjectList


class vDiskList(object):
    @staticmethod
    def get_vdisks():
        vdisks = DataList(key   = 'vdisks',
                          query = {'object': vDisk,
                                   'data': DataList.select.DESCRIPTOR,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': []}}).data
        return DataObjectList(vdisks, vDisk)

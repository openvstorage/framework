from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vdisk import vDisk


class vDiskList(object):
    @staticmethod
    def get_vdisks(template=False):
        vdisks = DataList(key   = 'vdisks',
                          query = {'object': vDisk,
                                   'data': DataList.select.DESCRIPTOR,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('template', DataList.operator.EQUALS, template)]}}).data
        return DataObjectList(vdisks, vDisk)

    @staticmethod
    def get_vdisk_by_volumeid(volumeid):
        vdisks = DataList(key   = 'vdisk_%s' % volumeid,
                          query = {'object': vDisk,
                                   'data'  : DataList.select.DESCRIPTOR,
                                   'query' : {'type' : DataList.where_operator.AND,
                                              'items': [('volumeid', DataList.operator.EQUALS, volumeid)]}}).data
        if vdisks:
            return DataObjectList(vdisks, vDisk)
        return None

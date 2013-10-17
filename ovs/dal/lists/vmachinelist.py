from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vmachine import vMachine


class VMachineList(object):
    @staticmethod
    def get_vmachines():
        vmachines = DataList(key   = 'vmachines',
                             query = {'object': vMachine,
                                      'data': DataList.select.DESCRIPTOR,
                                      'query': {'type': DataList.where_operator.AND,
                                                'items': []}}).data
        return DataObjectList(vmachines, vMachine)

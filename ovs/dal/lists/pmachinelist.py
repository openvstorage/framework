from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vmachine import pMachine


class PMachineList(object):
    @staticmethod
    def get_pmachines():
        pmachines = DataList(key   = 'pmachines',
                             query = {'object': pMachine,
                                      'data': DataList.select.DESCRIPTOR,
                                      'query': {'type': DataList.where_operator.AND,
                                                'items': []}}).data
        return DataObjectList(pmachines, pMachine)

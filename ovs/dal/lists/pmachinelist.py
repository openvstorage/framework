"""
PMachineList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vmachine import PMachine


class PMachineList(object):
    """
    This PMachineList class contains various lists regarding to the PMachine class
    """

    @staticmethod
    def get_pmachines():
        """
        Returns a list of all PMachines
        """
        pmachines = DataList(key='pmachines',
                             query={'object': PMachine,
                                    'data': DataList.select.DESCRIPTOR,
                                    'query': {'type': DataList.where_operator.AND,
                                              'items': []}}).data
        return DataObjectList(pmachines, PMachine)

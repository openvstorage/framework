# license see http://www.openvstorage.com/licenses/opensource/
"""
PMachineList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.pmachine import PMachine


class PMachineList(object):
    """
    This PMachineList class contains various lists regarding to the PMachine class
    """

    @staticmethod
    def get_pmachines():
        """
        Returns a list of all PMachines
        """
        pmachines = DataList({'object': PMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': []}}).data
        return DataObjectList(pmachines, PMachine)

    @staticmethod
    def get_by_ip(ip):
        """
        Gets a pmachine based on a given ip address
        """
        pmachines = DataList({'object': PMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('ip', DataList.operator.EQUALS, ip)]}}).data
        if pmachines:
            return DataObjectList(pmachines, PMachine)[0]
        return None

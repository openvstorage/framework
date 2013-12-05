# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachineList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vmachine import VMachine


class VMachineList(object):
    """
    This VMachineList class contains various lists regarding to the VMachine class
    """

    @staticmethod
    def get_vmachines():
        """
        Returns a list of all VMachines
        """
        vmachines = DataList({'object': VMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': []}}).data
        return DataObjectList(vmachines, VMachine)

    @staticmethod
    def get_vmachine_by_name(vmname):
        """
        Returns all VMachines which have a given name
        """
        # pylint: disable=line-too-long
        vmachines = DataList({'object': VMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('name', DataList.operator.EQUALS, vmname)]}}).data  # noqa
        # pylint: enable=line-too-long
        if vmachines:
            return DataObjectList(vmachines, VMachine)
        return None

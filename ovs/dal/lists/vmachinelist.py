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

    @staticmethod
    def get_by_devicename(devicename):
        """
        Returns a list of all VDisks based on a given volumeid
        """
        # pylint: disable=line-too-long
        vms = DataList({'object': VMachine,
                        'data': DataList.select.DESCRIPTOR,
                        'query': {'type': DataList.where_operator.AND,
                                  'items': [('devicename', DataList.operator.EQUALS, devicename)]}}).data  # noqa
        # pylint: enable=line-too-long
        if vms:
            return DataObjectList(vms, VMachine)[0]
        return None

    @staticmethod
    def get_customer_vmachines():
        """
        Returns "real" vmachines. No vTemplates or internal machines
        """
        vmachines = DataList({'object': VMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('is_vtemplate', DataList.operator.EQUALS, False),
                                                  ('is_internal', DataList.operator.EQUALS, False)]}}).data
        return DataObjectList(vmachines, VMachine)

    @staticmethod
    def get_vsas():
        """
        Returns VSA vMachines
        """
        vmachines = DataList({'object': VMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('is_vtemplate', DataList.operator.EQUALS, False),
                                                  ('is_internal', DataList.operator.EQUALS, True)]}}).data
        return DataObjectList(vmachines, VMachine)

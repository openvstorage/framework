# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    def get_by_devicename_and_vpool(devicename, vpool):
        """
        Returns a list of all VDisks based on a given volumeid
        """
        vpool_guid = None if vpool is None else vpool.guid
        vms = DataList({'object': VMachine,
                        'data': DataList.select.DESCRIPTOR,
                        'query': {'type': DataList.where_operator.AND,
                                  'items': [('devicename', DataList.operator.EQUALS, devicename),
                                            ('vpool_guid', DataList.operator.EQUALS, vpool_guid)]}}).data
        if vms:
            if len(vms) != 1:
                raise RuntimeError('Invalid amount of vMachines found: {0}'.format(len(vms)))
            return DataObjectList(vms, VMachine)[0]
        return None

    @staticmethod
    def get_customer_vmachines():
        """
        Returns "real" vmachines. No vTemplates
        """
        vmachines = DataList({'object': VMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('is_vtemplate', DataList.operator.EQUALS, False)]}}).data
        return DataObjectList(vmachines, VMachine)

    @staticmethod
    def get_vtemplates():
        """
        Returns vTemplates
        """
        vmachines = DataList({'object': VMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('is_vtemplate', DataList.operator.EQUALS, True)]}}).data
        return DataObjectList(vmachines, VMachine)

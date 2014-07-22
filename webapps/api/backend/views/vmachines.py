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
VMachine module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from django.http import Http404
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.lib.vmachine import VMachineController
from ovs.dal.exceptions import ObjectNotFoundException
from backend.decorators import required_roles, expose, discover, return_list, return_object, celery_task


class VMachineViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vmachines'
    base_name = 'vmachines'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @return_list(VMachine, 'name,vpool_guid')
    @discover()
    def list(self, vpoolguid=None):
        """
        Overview of all machines
        """
        if vpoolguid is not None:
            vpool = VPool(vpoolguid)
            vmachine_guids = []
            vmachines = []
            for vdisk in vpool.vdisks:
                if vdisk.vmachine_guid is not None and vdisk.vmachine_guid not in vmachine_guids:
                    vmachine_guids.append(vdisk.vmachine.guid)
                    if vdisk.vmachine.is_vtemplate is False:
                        vmachines.append(vdisk.vmachine)
        else:
            vmachines = VMachineList.get_vmachines()
        return vmachines

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @return_object(VMachine)
    @discover(VMachine)
    def retrieve(self, vmachine):
        """
        Load information about a given vMachine
        """
        return vmachine

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['delete'])
    @celery_task()
    @discover(VMachine)
    def destroy(self, vmachine):
        """
        Deletes a machine
        """
        if not vmachine.is_vtemplate:
            raise NotAcceptable('vMachine should be a vTemplate')
        return VMachineController.delete.delay(machineguid=vmachine.guid)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @celery_task()
    @discover(VMachine)
    def rollback(self, vmachine, timestamp):
        """
        Clones a machine
        """
        if vmachine.is_vtemplate:
            raise NotAcceptable('vMachine should not be a vTemplate')
        return VMachineController.rollback.delay(machineguid=vmachine.guid,
                                                 timestamp=timestamp)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @celery_task()
    @discover(VMachine)
    def snapshot(self, vmachine, name, consistent):
        """
        Snapshots a given machine
        """
        if vmachine.is_vtemplate:
            raise NotAcceptable('vMachine should not be a vTemplate')
        label = str(name)
        is_consistent = True if consistent else False  # Assure boolean type
        return VMachineController.snapshot.delay(machineguid=vmachine.guid,
                                                 label=label,
                                                 is_consistent=is_consistent,
                                                 is_automatic=False)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @return_list(VMachine)
    @discover(VMachine)
    def get_children(self, vmachine, hints):
        """
        Returns a list of vMachines guid(s) of children of a given vMachine
        """
        children_vmachine_guids = []
        children_vmachines = []
        if vmachine.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        for vdisk in vmachine.vdisks:
            for cdisk in vdisk.child_vdisks:
                if cdisk.vmachine_guid not in children_vmachine_guids:
                    children_vmachine_guids.append(cdisk.vmachine_guid)
                    if hints['full'] is True:
                        # Only load full object is required
                        children_vmachines.append(cdisk.vmachine)
        return children_vmachines if hints['full'] is True else children_vmachine_guids

    @expose(internal=True)
    @required_roles(['view'])
    @return_list(VMachine)
    @discover()
    def filter(self, query):
        """
        Filters vMachines based on a filter object
        """
        query_result = DataList({'object': VMachine,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': query}).data
        return DataObjectList(query_result, VMachine)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @celery_task()
    @discover(VMachine)
    def set_as_template(self, vmachine):
        """
        Sets a given machine as template
        """
        return VMachineController.set_as_template.delay(machineguid=vmachine.guid)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @celery_task()
    @discover(VMachine)
    def create_from_template(self, vmachine, pmachineguid, name, description):
        """
        Creates a vMachine based on a vTemplate
        """
        try:
            pmachine = PMachine(pmachineguid)
        except ObjectNotFoundException:
            raise Http404('pMachine could not be found')
        if vmachine.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        return VMachineController.create_from_template.delay(machineguid=vmachine.guid,
                                                             pmachineguid=pmachine.guid,
                                                             name=str(name),
                                                             description=str(description))

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @celery_task()
    @discover(VMachine)
    def create_multiple_from_template(self, vmachine, pmachineguids, amount, start, name, description):
        """
        Creates a certain amount of vMachines based on a vTemplate
        """
        if len(pmachineguids) == 0:
            raise NotAcceptable
        try:
            for pmachienguid in pmachineguids:
                _ = PMachine(pmachienguid)
        except ObjectNotFoundException:
            raise Http404('pMachine could not be found')
        if vmachine.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        if not isinstance(amount, int) or not isinstance(start, int):
            raise NotAcceptable('Fields amount and start should be numeric')
        amount = max(1, amount)
        start = max(0, start)
        return VMachineController.create_multiple_from_template.delay(machineguid=vmachine.guid,
                                                                      pmachineguids=pmachineguids,
                                                                      amount=amount,
                                                                      start=start,
                                                                      name=str(name),
                                                                      description=str(description))

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @return_list(PMachine)
    @discover(VMachine)
    def get_target_pmachines(self, vmachine, hints):
        """
        Gets all possible target pMachines for a given vMachine
        """
        if not vmachine.is_vtemplate:
            raise NotAcceptable('vMachine is not a vTemplate')
        # Collect all vPools used by the given template
        vpool_guids = []
        vpools = []
        if vmachine.vpool is not None:
            if vmachine.vpool_guid not in vpool_guids:
                vpools.append(vmachine.vpool)
                vpool_guids.append(vmachine.vpool_guid)
        for vdisk in vmachine.vdisks:
            if vdisk.vpool_guid not in vpool_guids:
                vpools.append(vdisk.vpool)
                vpool_guids.append(vdisk.vpool_guid)
        # Find pMachines which have all above vPools available.
        pmachine_guids = None
        pmachines = {}
        for vpool in vpools:
            this_pmachine_guids = set()
            for storagedriver in vpool.storagedrivers:
                this_pmachine_guids.add(storagedriver.storagerouter.pmachine_guid)
                if hints['full'] is True:
                    pmachines[storagedriver.storagerouter.pmachine_guid] = storagedriver.storagerouter.pmachine
            if pmachine_guids is None:
                pmachine_guids = list(this_pmachine_guids)
            else:
                pmachine_guids = list(this_pmachine_guids & set(pmachine_guids))
        return pmachine_guids if hints['full'] is False else [pmachines[guid] for guid in pmachine_guids]

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

import json
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
from backend.decorators import required_roles, load, return_list, return_object, return_task, log


class VMachineViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vmachines'
    base_name = 'vmachines'

    @log()
    @required_roles(['read'])
    @return_list(VMachine, 'name,vpool_guid')
    @load()
    def list(self, vpoolguid=None, query=None):
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
        elif query is not None:
            query = json.loads(query)
            query_result = DataList({'object': VMachine,
                                     'data': DataList.select.GUIDS,
                                     'query': query}).data
            vmachines = DataObjectList(query_result, VMachine)
        else:
            vmachines = VMachineList.get_vmachines()
        return vmachines

    @log()
    @required_roles(['read'])
    @return_object(VMachine)
    @load(VMachine)
    def retrieve(self, vmachine):
        """
        Load information about a given vMachine
        """
        return vmachine

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VMachine)
    def destroy(self, vmachine):
        """
        Deletes a machine
        """
        if not vmachine.is_vtemplate:
            raise NotAcceptable('vMachine should be a vTemplate')
        return VMachineController.delete.delay(machineguid=vmachine.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VMachine)
    def rollback(self, vmachine, timestamp):
        """
        Clones a machine
        """
        if vmachine.is_vtemplate:
            raise NotAcceptable('vMachine should not be a vTemplate')
        return VMachineController.rollback.delay(machineguid=vmachine.guid,
                                                 timestamp=timestamp)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VMachine)
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
    @log()
    @required_roles(['read'])
    @return_list(VMachine)
    @load(VMachine)
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

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VMachine)
    def set_as_template(self, vmachine):
        """
        Sets a given machine as template
        """
        return VMachineController.set_as_template.delay(machineguid=vmachine.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VMachine)
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
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VMachine)
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
    @log()
    @required_roles(['read'])
    @return_list(PMachine)
    @load(VMachine)
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

    @action()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VMachine)
    def set_configparams(self, vmachine, configparams):
        """
        Sets configuration parameters to a given vmachine/vdisk. Items not passed are (re)set.
        """
        return VMachineController.set_configparams.delay(vmachine_guid=vmachine.guid, configparams=configparams)

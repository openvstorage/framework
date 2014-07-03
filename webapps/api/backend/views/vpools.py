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
VPool module
"""
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vmachine import VMachine
from ovs.lib.vpool import VPoolController
from ovs.lib.vmachine import VMachineController
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
from backend.decorators import required_roles, expose, validate, get_list, get_object, celery_task


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about vPools
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vpools'
    base_name = 'vpools'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @get_list(VPool, 'name')
    def list(self, request, format=None, hints=None):
        """
        Overview of all vPools
        """
        _ = request, format, hints
        return VPoolList.get_vpools()

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VPool)
    @get_object(VPool)
    def retrieve(self, request, obj):
        """
        Load information about a given vPool
        """
        _ = request
        return obj

    @action()
    @expose(internal=True)
    @required_roles(['view', 'create'])
    @validate(VPool)
    @celery_task()
    def sync_vmachines(self, request, obj):
        """
        Syncs the vMachine of this vPool
        """
        _ = request
        return VPoolController.sync_with_hypervisor.delay(obj.guid)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VPool)
    @get_list(VMachine)
    def serving_vsas(self, request, obj, hints):
        """
        Retreives a list of VSA guids, serving a given vPool
        """
        _ = request
        vsa_guids = []
        vsas = []
        for vsr in obj.vsrs:
            vsa_guids.append(vsr.serving_vmachine_guid)
            if hints['full'] is True:
                vsas.append(vsr.serving_vmachine)
        return vsas if hints['full'] is True else vsa_guids

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VPool)
    @celery_task()
    def update_vsrs(self, request, obj):
        """
        Update VSRs for a given vPool (both adding and removing VSRs)
        """
        vsas = []
        if 'vsa_guids' in request.DATA:
            if request.DATA['vsa_guids'].strip() != '':
                for vsa_guid in request.DATA['vsa_guids'].strip().split(','):
                    vsa = VMachine(vsa_guid)
                    if vsa.is_internal is not True:
                        raise NotAcceptable('vMachine is not a VSA')
                    vsas.append((vsa.ip, vsa.machineid))
        if 'vsr_guid' not in request.DATA:
            raise NotAcceptable('No VSR guid passed')
        vsr_guids = []
        if 'vsr_guids' in request.DATA:
            if request.DATA['vsr_guids'].strip() != '':
                for vsr_guid in request.DATA['vsr_guids'].strip().split(','):
                    vsr = VolumeStorageRouter(vsr_guid)
                    if vsr.vpool_guid != obj.guid:
                        raise NotAcceptable('Given VSR does not belong to this vPool')
                    vsr_guids.append(vsr.guid)

        vsr = VolumeStorageRouter(request.DATA['vsr_guid'])
        parameters = {'vpool_name':          obj.name,
                      'backend_type':        obj.backend_type,
                      'connection_host':     None if obj.backend_connection is None else obj.backend_connection.split(':')[0],
                      'connection_port':     None if obj.backend_connection is None else int(obj.backend_connection.split(':')[1]),
                      'connection_timeout':  0,  # Not in use anyway
                      'connection_username': obj.backend_login,
                      'connection_password': obj.backend_password,
                      'mountpoint_bfs':      vsr.mountpoint_bfs,
                      'mountpoint_temp':     vsr.mountpoint_temp,
                      'mountpoint_md':       vsr.mountpoint_md,
                      'mountpoint_cache':    vsr.mountpoint_cache,
                      'storage_ip':          vsr.storage_ip,
                      'vrouter_port':        vsr.port}
        for field in parameters:
            if not parameters[field] is int:
                parameters[field] = str(parameters[field])

        return VMachineController.update_vsrs.delay(vsr_guids, vsas, parameters)

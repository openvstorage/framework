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
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vmachine import VMachine
from ovs.lib.vpool import VPoolController
from ovs.lib.vmachine import VMachineController
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles, expose, validate
from backend.toolbox import Toolbox


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about vPools
    """
    permission_classes = (IsAuthenticated,)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all vPools
        """
        _ = format
        vpools = VPoolList.get_vpools()
        vpools, serializer, contents = Toolbox.handle_list(vpools, request, default_sort='name')
        serialized = serializer(VPool, contents=contents, instance=vpools, many=True)
        return Response(serialized.data, status=status.HTTP_200_OK)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VPool)
    def retrieve(self, request, obj):
        """
        Load information about a given vPool
        """
        contents = Toolbox.handle_retrieve(request)
        return Response(FullSerializer(VPool, contents=contents, instance=obj).data, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True)
    @required_roles(['view', 'create'])
    @validate(VPool)
    def sync_vmachines(self, request, obj):
        """
        Syncs the vMachine of this vPool
        """
        _ = request
        task = VPoolController.sync_with_hypervisor.delay(obj.guid)
        return Response(task.id, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VPool)
    def serving_vsas(self, request, obj):
        """
        Retreives a list of VSA guids, serving a given vPool
        """
        _ = request
        vsa_guids = []
        for vsr in obj.vsrs:
            vsa_guids.append(vsr.serving_vmachine_guid)
        return Response(vsa_guids, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VPool)
    def vsas_to_vpool(self, request, obj):
        """
        Adds a vpool to a VSA, given a VSR
        """

        vsas = []
        if 'vsa_guids' in request.DATA:
            for vsa_guid in request.DATA['vsa_guids'].split(','):
                vsa = VMachine(vsa_guid)
                if vsa.is_internal is not True:
                    raise NotAcceptable('vMachine is not a VSA')
                vsas.append((vsa.ip, vsa.machineid))
        if 'vsr_guid' not in request.DATA:
            raise NotAcceptable('No VSR guid passed')

        vsr = VolumeStorageRouter(request.DATA['vsr_guid'])
        parameters = {'vpool_name':          obj.name,
                      'backend_type':        obj.backend_type,
                      'connection_host':     obj.backend_connection.split(':')[0],
                      'connection_port':     int(obj.backend_connection.split(':')[1]),
                      'connection_timeout':  0,  # Not in use anyway
                      'connection_username': obj.backend_login,
                      'connection_password': obj.backend_password,
                      'mountpoint_temp':     vsr.mountpoint_temp,
                      'mountpoint_dfs':      vsr.mountpoint_dfs,
                      'mountpoint_md':       vsr.mountpoint_md,
                      'mountpoint_cache':    vsr.mountpoint_cache,
                      'storage_ip':          vsr.storage_ip,
                      'vrouter_port':        vsr.port}
        for field in parameters:
            if not parameters[field] is int:
                parameters[field] = str(parameters[field])

        task = VMachineController.add_vpools.delay(vsas, parameters)
        return Response(task.id, status=status.HTTP_200_OK)

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
MDSService module
"""
import random
from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.extensions.generic.system import System
from volumedriver.storagerouter.storagerouterclient import MDSNodeConfig, MDSMetaDataBackendConfig


class MDSServiceController(object):
    """
    Contains all BLL related to MDSServices
    """

    @staticmethod
    def prepare_mds_service(client, storagerouter, vpool, fresh_only=True, start=False):
        """
        Prepares an MDS service:
        * Creates the required configuration
        * Sets up the service files

        Assumes the StorageRouter and VPool are already configured with a StorageDriver and that all model-wise
        configuration regarding both is completed.
        """
        mdsservice_type = ServiceTypeList.get_by_name('MetadataServer')
        storagedriver = [sd for sd in vpool.storagedrivers if sd.storagerouter_guid == storagerouter.guid][0]

        # Fetch service sequence number
        service_number = -1
        for mds_service in vpool.mds_services:
            if mds_service.service.storagerouter_guid == storagerouter.guid:
                service_number = max(mds_service.number, service_number)

        if fresh_only is True and service_number >= 0:
            return  # There are already one or more MDS services running, aborting
        service_number += 1

        # Find free port
        occupied_ports = []
        for service in mdsservice_type.services:
            if service.storagerouter_guid == storagerouter.guid:
                occupied_ports.append(service.port)
        port = System.get_free_ports(Configuration.get('ovs.ports.mds'),
                                     exclude=occupied_ports, nr=1, client=client)[0]

        # Add service to the model
        service = Service()
        service.name = 'metadataserver_{0}_{1}'.format(vpool.name, service_number)
        service.type = mdsservice_type
        service.storagerouter = storagerouter
        service.port = port
        service.save()
        mds_service = MDSService()
        mds_service.service = service
        mds_service.vpool = vpool
        mds_service.number = service_number
        mds_service.save()

        # Prepare some directores
        scratch_dir = '{0}/mds_{0}_{1}'.format(storagedriver.mountpoint_temp, vpool.name, service_number)
        rocksdb_dir = '{0}/mds_{0}_{1}'.format(storagedriver.mountpoint_md, vpool.name, service_number)
        client.run('mkdir -p {0}'.format(scratch_dir))
        # @TODO (after OVS-1605): client.run('mkdir -p {0}'.format(rocksdb_dir))

        # Generate the configuration file
        metadataserver_config = StorageDriverConfiguration('metadataserver', vpool.name, number=service_number)
        metadataserver_config.load(client)
        metadataserver_config.clean()  # Clean out obsolete values
        metadataserver_config.configure_backend_connection_manager(**vpool.metadata)
        metadataserver_config.configure_metadata_server(mds_address=storagerouter.ip,
                                                        mds_port=service.port,
                                                        mds_scratch_dir=scratch_dir,
                                                        mds_rocksdb_path=rocksdb_dir)
        metadataserver_config.save(client)

        # Create system services
        params = {'<VPOOL_NAME>': vpool.name,
                  '<SERVICE_NUMBER>': '0'}
        template_dir = '/opt/OpenvStorage/config/templates/upstart'
        client.run('cp -f {0}/ovs-metadataserver.conf {0}/ovs-metadataserver_{1}_{2}.conf'.format(template_dir, vpool.name, service_number))
        service_script = """
from ovs.plugin.provider.service import Service
Service.add_service(package=('openvstorage', 'metadataserver'), name='metadataserver_{0}_{1}', command=None, stop_command=None, params={2})
""".format(vpool.name, service_number, params)
        System.exec_remote_python(client, service_script)

        if start is True:
            System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
Service.enable_service('{0}')
""".format(service.name))
            System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
Service.start_service('{0}')
""".format(service.name))

    @staticmethod
    def sync_vdisk_to_reality(vdisk):
        """
        Syncs a vdisk to reality (except hypervisor)
        """
        vdisk.invalidate_dynamics(['info'])
        config = vdisk.info['metadata_backend_config']
        config_dict = {}
        for item in config:
            if item['ip'] not in config_dict:
                config_dict[item['ip']] = []
            config_dict[item['ip']].append(item['port'])
        mds_dict = {}
        for junction in vdisk.mds_services:
            service = junction.mds_service.service
            storagerouter = service.storagerouter
            if config[0]['ip'] == storagerouter.ip and config[0]['port'] == service.port:
                junction.is_master = True
                junction.save()
                if storagerouter.ip not in mds_dict:
                    mds_dict[storagerouter.ip] = []
                mds_dict[storagerouter.ip].append(service.port)
            elif storagerouter.ip in config_dict and service.port in config_dict[storagerouter.ip]:
                junction.is_master = False
                junction.save()
                if storagerouter.ip not in mds_dict:
                    mds_dict[storagerouter.ip] = []
                mds_dict[storagerouter.ip].append(service.port)
            else:
                junction.delete()
        for ip, ports in config_dict.iteritems():
            for port in ports:
                if ip not in mds_dict or port not in mds_dict[ip]:
                    service = ServiceList.get_by_ip_port(ip, port)
                    if service is not None:
                        mds_service_vdisk = MDSServiceVDisk()
                        mds_service_vdisk.vdisk = vdisk
                        mds_service_vdisk.mds_service = service.mds_service
                        mds_service_vdisk.save()

    @staticmethod
    def ensure_safety(vdisk, excluded_storagerouters=None):
        """
        Ensures (or tries to ensure) the safety of a given vdisk (except hypervisor)
        """
        changes = True
        if excluded_storagerouters is None:
            excluded_storagerouters = []
        vdisk.invalidate_dynamics(['info'])
        config = vdisk.info['metadata_backend_config']
        storagerouters = [storagedriver.storagerouter for storagedriver in vdisk.vpool.storagedrivers
                          if storagedriver.storagerouter not in excluded_storagerouters]
        mds_services_with_namespace = []
        node_configs = []
        nodes = []
        is_master = True
        for item in config:
            if item['ip'] not in nodes:
                if is_master is False:
                    mds_service = ServiceList.get_by_ip_port(item['ip'], item['port'])
                    if MDSServiceController.get_mds_load(mds_service) > Configuration.getInt('ovs.storagedriver.mds.maxload'):
                        mds_services_with_namespace.append(mds_service)
                        continue
                nodes.append(item['ip'])
                node_configs.append(MDSNodeConfig(address=item['ip'], port=item['port']))
            else:
                changes = True
            is_master = False
        while len(nodes) < len(storagerouters) and len(nodes) < Configuration.getInt('ovs.storagedriver.mds.safety'):
            for storagerouter in storagerouters:
                mds_service = MDSServiceController.get_preferred_mds(storagerouter, vdisk.vpool)
                service = mds_service.service
                if storagerouter.ip not in nodes:
                    nodes.append(storagerouter.ip)
                    node_configs.append(MDSNodeConfig(address=storagerouter.ip, port=service.port))
                    if mds_service not in mds_services_with_namespace:
                        mds_service.metadataserver_client.create_namespace(vdisk.volume_id)
                    changes = True
        if changes is True:
            vdisk.storagedriver_client.update_metadata_backend_config(
                volume_id=vdisk.volume_id,
                metadata_backend_config=MDSMetaDataBackendConfig(node_configs)
            )
            MDSServiceController.sync_vdisk_to_reality(vdisk)

    @staticmethod
    def get_preferred_mds(storagerouter, vpool, include_load=False):
        """
        Gets the MDS on this StorageRouter/VPool pair which is preferred to achieve optimal balancing
        """

        mds_service = None
        for current_mds_service in vpool.mds_services:
            if current_mds_service.service.storagerouter_guid == storagerouter.guid:
                load = MDSServiceController.get_mds_load(current_mds_service)
                if mds_service is None or load < mds_service[1]:
                    mds_service = (current_mds_service, load)
        if include_load is True:
            return mds_service
        return mds_service[0] if mds_service is not None else None

    @staticmethod
    def get_mds_load(mds_service):
        """
        Gets a 'load' for an MDS service based on its capacity and the amount of assinged VDisks
        """
        if mds_service.capacity < 0:
            return 50
        return int(len(mds_service.vdisks_guids) / float(mds_service.capacity) * 100.0)

    @staticmethod
    def get_mds_storagedriver_config_set(vpool):
        """
        Builds a configuration for all StorageRouters from a given VPool with following goals:
        * Primary MDS is the local one
        * All slaves are on different hosts
        * Maximum `mds.safety` nodes are returned
        """

        mds_per_storagerouter = {}
        mds_per_load = {}
        for storagedriver in vpool.storagedrivers:
            storagerouter = storagedriver.storagerouter
            mds_service, load = MDSServiceController.get_preferred_mds(storagerouter, vpool, include_load=True)
            mds_per_storagerouter[storagerouter.guid] = {'host': storagerouter.ip, 'port': mds_service.service.port}
            if load not in mds_per_load:
                mds_per_load[load] = []
            mds_per_load[load].append(storagerouter.guid)

        safety = Configuration.getInt('ovs.storagedriver.mds.safety')
        config_set = {}
        for storagerouter_guid in mds_per_storagerouter:
            config_set[storagerouter_guid] = [mds_per_storagerouter[storagerouter_guid]]
            for load in sorted(mds_per_load.keys()):
                if len(config_set[storagerouter_guid]) > safety:
                    break
                sr_guids = mds_per_load[load]
                random.shuffle(sr_guids)
                for sr_guid in sr_guids:
                    if len(config_set[storagerouter_guid]) > safety:
                        break
                    if sr_guid != storagerouter_guid:
                        config_set[storagerouter_guid].append(mds_per_storagerouter[sr_guid])
        return config_set

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
import math
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
            return None  # There are already one or more MDS services running, aborting
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

        return mds_service

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

        if excluded_storagerouters is None:
            excluded_storagerouters = []
        maxload = Configuration.getInt('ovs.storagedriver.mds.maxload')
        safety = Configuration.getInt('ovs.storagedriver.mds.safety')
        services = [mds_service.service for mds_service in vdisk.vpool.mds_services
                    if mds_service.service.storagerouter not in excluded_storagerouters]
        services_per_load = {}
        services_load = {}
        service_per_key = {}
        for service in services:
            load = MDSServiceController.get_mds_load(service.mds_service)
            if load not in services_per_load:
                services_per_load[load] = []
            services_per_load[load].append(service)
            services_load[service.guid] = load
            service_per_key['{0}:{1}'.format(service.storagerouter.ip, service.port)] = service

        # List current configuration and filter out excluded services
        vdisk.invalidate_dynamics(['info'])
        configs = vdisk.info['metadata_backend_config']
        for config in configs:
            config['key'] = '{0}:{1}'.format(config['ip'], config['port'])
        master_service = service_per_key.get(configs[0]['key'])
        slave_services = []
        for config in configs[1:]:
            if config['key'] in service_per_key:
                slave_services.append(service_per_key[config['key']])

        # Prepare fresh configuration
        new_services = []

        # Check whether the master (if available) is non-local to the vdisk and/or is overloaded
        master_ok = master_service is not None
        if master_ok is True:
            master_ok = master_service.storagerouter_guid == vdisk.storagerouter_guid \
                        and services_load[master_service.guid] <= maxload

        if master_ok:
            # Add this master to the fresh configuration
            new_services.append(master_service)
        else:
            # Try to find the best non-overloaded local MDS (slave)
            candidate_master = None
            candidate_master_load = 0
            local_mds = None
            local_mds_load = 0
            for service in services:
                load = services_load[service.guid]
                if load <= maxload and service.storagerouter_guid == vdisk.storagerouter_guid:
                    if local_mds is None or local_mds_load > load:
                        # This service is a non-overloaded local MDS
                        local_mds = service
                        local_mds_load = load
                    if service in slave_services:
                        if candidate_master is None or candidate_master_load > load:
                            # This service is a non-overloaded local slave
                            candidate_master = service
                            candidate_master_load = load
            if candidate_master is not None:
                # A non-overloaded local slave was found.
                up_to_date = True  # @TODO: Call storagedriver to figure out whether its (more or less) up to date
                if up_to_date is True:
                    # It's up to date, so add it as a new master
                    new_services.append(candidate_master)
                    if master_service is not None:
                        # The current master (if available) is now candidate for become one of the slaves
                        slave_services.append(master_service)
                else:
                    # It's not up to date, keep the previous master (if available) and give the local slave
                    # some more time to catch up
                    if master_service is not None:
                        new_services.append(master_service)
                    new_services.append(candidate_master)
                slave_services.remove(candidate_master)
            else:
                # There's no non-overloaded local slave found. Keep the current master (if available) and add
                # a local MDS (if available) as slave
                if master_service is not None:
                    new_services.append(master_service)
                if local_mds is not None:
                    new_services.append(local_mds)
                    slave_services.remove(local_mds)

        # At this point, there might (or might not) be a (new) master, and a (catching up) slave. The rest of the non-local
        # MDS nodes must now be added to the configuration until the safety is reached. It is preferred to recycle at
        # least floor(safety/2) current slaves to prevent to much fresh slaves that still have to sync all data
        recycle_count = math.floor(safety / 2.0)
        loads = sorted(services_per_load.keys())
        nodes = [service.storagerouter.ip for service in new_services]
        if len(new_services) < recycle_count:
            for load in loads:
                for service in services_per_load[load]:
                    if service in slave_services:
                        if len(new_services) < recycle_count:
                            if service.storagerouter.ip not in nodes:
                                new_services.append(service)
                                slave_services.remove(service)
                                nodes.append(service.storagerouter.ip)
        if len(new_services) < safety:
            for load in loads:
                for service in services_per_load[load]:
                    if len(new_services) < safety and service.storagerouter.ip not in nodes:
                        new_services.append(service)
                        nodes.append(service.storagerouter.ip)

        # Build the new configuration and update the vdisk
        configs = []
        for service in new_services:
            configs.append(MDSNodeConfig(address=service.storagerouter.ip,
                                         port=service.port))
        vdisk.storagedriver_client.update_metadata_backend_config(
            volume_id=vdisk.volume_id,
            metadata_backend_config=MDSMetaDataBackendConfig(configs)
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
        return len(mds_service.vdisks_guids) / float(mds_service.capacity) * 100.0

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

# Copyright 2016 iNuron NV
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
import time
import random

from celery.schedules import crontab
from ovs.celery_run import celery
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.sshclient import UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.storageserver.storagedriver import MetadataServerClient
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.decorators import ensure_single
from ovs.log.logHandler import LogHandler
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter.storagerouterclient import MDSMetaDataBackendConfig
from volumedriver.storagerouter.storagerouterclient import MDSNodeConfig

logger = LogHandler.get('lib', name='mds')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
# noinspection PyArgumentList
storagerouterclient.Logger.enableLogging()


class MDSServiceController(object):
    """
    Contains all BLL related to MDSServices
    """

    @staticmethod
    def prepare_mds_service(storagerouter, vpool, fresh_only, reload_config):
        """
        Prepares an MDS service:
        * Creates the required configuration
        * Sets up the service files

        Assumes the StorageRouter and VPool are already configured with a StorageDriver and that all model-wise
        configuration regarding both is completed.
        :param storagerouter: Storagerouter on which MDS service will be created
        :type storagerouter: StorageRouter

        :param vpool: The vPool for which the MDS service will be created
        :type vpool: VPool

        :param fresh_only: If True and no current mds services exist for this vpool on this storagerouter, a new 1 will be created
        :type fresh_only: bool

        :param reload_config: If True, the volumedriver's updated configuration will be reloaded
        :type reload_config: bool

        :return: Newly created service
        :rtype: MDSService
        """
        # Fetch service sequence number based on MDS services for current vPool and current storage router
        service_number = -1
        for mds_service in vpool.mds_services:
            if mds_service.service.storagerouter_guid == storagerouter.guid:
                service_number = max(mds_service.number, service_number)

        if fresh_only is True and service_number >= 0:
            return  # There is already 1 or more MDS services running, aborting

        # VALIDATIONS
        # 1. Find free port based on MDS services for all vPools on current storage router
        client = SSHClient(storagerouter)
        mdsservice_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER)
        occupied_ports = []
        for service in mdsservice_type.services:
            if service.storagerouter_guid == storagerouter.guid:
                occupied_ports.extend(service.ports)

        mds_port_range = EtcdConfiguration.get('/ovs/framework/hosts/{0}/ports|mds'.format(System.get_my_machine_id(client)))
        free_ports = System.get_free_ports(selected_range=mds_port_range,
                                           exclude=occupied_ports,
                                           nr=1,
                                           client=client)
        if not free_ports:
            raise RuntimeError('Failed to find an available port on storage router {0} within range {1}'.format(storagerouter.name, mds_port_range))

        # 2. Partition check
        db_partition = None
        for disk in storagerouter.disks:
            for partition in disk.partitions:
                if DiskPartition.ROLES.DB in partition.roles:
                    db_partition = partition
                    break
        if db_partition is None:
            raise RuntimeError('Could not find DB partition on storage router {0}'.format(storagerouter.name))

        # 3. Verify storage driver configured
        storagedrivers = [sd for sd in vpool.storagedrivers if sd.storagerouter_guid == storagerouter.guid]
        if not storagedrivers:
            raise RuntimeError('Expected to find a configured storagedriver for vpool {0} on storage router {1}'.format(vpool.name, storagerouter.name))
        storagedriver = storagedrivers[0]

        # MODEL UPDATES
        # 1. Service
        service_number += 1
        service = Service()
        service.name = 'metadataserver_{0}_{1}'.format(vpool.name, service_number)
        service.type = mdsservice_type
        service.ports = [free_ports[0]]
        service.storagerouter = storagerouter
        service.save()
        mds_service = MDSService()
        mds_service.vpool = vpool
        mds_service.number = service_number
        mds_service.service = service
        mds_service.save()

        # 2. Storage driver partitions
        from ovs.lib.storagedriver import StorageDriverController
        sdp = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                 'role': DiskPartition.ROLES.DB,
                                                                                 'sub_role': StorageDriverPartition.SUBROLE.MDS,
                                                                                 'partition': db_partition,
                                                                                 'mds_service': mds_service})

        # CONFIGURATIONS
        # 1. Volumedriver
        mds_nodes = []
        for service in mdsservice_type.services:
            if service.storagerouter_guid == storagerouter.guid:
                mds_service = service.mds_service
                if mds_service is not None:
                    if mds_service.vpool_guid == vpool.guid:
                        mds_nodes.append({'host': service.storagerouter.ip,
                                          'port': service.ports[0],
                                          'db_directory': sdp.path,
                                          'scratch_directory': sdp.path})

        # Generate the correct section in the Storage Driver's configuration
        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
        storagedriver_config.load()
        storagedriver_config.clean()  # Clean out obsolete values
        storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
        storagedriver_config.save(client, reload_config=reload_config)

        return mds_service

    @staticmethod
    def remove_mds_service(mds_service, vpool, reconfigure, allow_offline=False):
        """
        Removes an MDS service
        :param mds_service: The MDS service to remove
        :type mds_service: MDSService

        :param vpool: The vPool for which the MDS service will be removed
        :type vpool: VPool

        :param reconfigure: Indicates whether reconfiguration is required
        :type reconfigure: bool

        :param allow_offline: Indicates whether it's OK that the node for which mds services are cleaned is offline
        :type allow_offline: bool
        """
        if len(mds_service.vdisks_guids) > 0 and allow_offline is False:
            raise RuntimeError('Cannot remove MDSService that is still serving disks')

        mdsservice_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER)

        # Clean up model
        directories_to_clean = []
        for sd_partition in mds_service.storagedriver_partitions:
            directories_to_clean.append(sd_partition.path)
            sd_partition.delete()

        if allow_offline is True:  # Certain vdisks might still be attached to this offline MDS service --> Delete relations
            for junction in mds_service.vdisks:
                junction.delete()

        mds_service.delete()
        mds_service.service.delete()

        storagerouter = mds_service.service.storagerouter
        try:
            client = SSHClient(storagerouter)
            if reconfigure is True:
                # Generate new mds_nodes section
                mds_nodes = []
                for service in mdsservice_type.services:
                    if service.storagerouter_guid == storagerouter.guid:
                        mds_service = service.mds_service
                        if mds_service.vpool_guid == vpool.guid:
                            sdp = [sd_partition.path for sd_partition in mds_service.storagedriver_partitions if sd_partition.role == DiskPartition.ROLES.DB]
                            mds_nodes.append({'host': service.storagerouter.ip,
                                              'port': service.ports[0],
                                              'db_directory': sdp[0],
                                              'scratch_directory': sdp[0]})

                # Generate the correct section in the Storage Driver's configuration
                storagedriver = [sd for sd in storagerouter.storagedrivers if sd.vpool_guid == vpool.guid][0]
                storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
                storagedriver_config.load()
                storagedriver_config.clean()  # Clean out obsolete values
                storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
                storagedriver_config.save(client, reload_config=reconfigure)

            tries = 5
            while tries > 0:
                try:
                    root_client = SSHClient(storagerouter, username='root')
                    root_client.dir_delete(directories=directories_to_clean,
                                           follow_symlinks=True)
                    for dir_name in directories_to_clean:
                        logger.debug('Recursively removed {0}'.format(dir_name))
                    break
                except Exception:
                    logger.debug('Waiting for the MDS service to go down...')
                    time.sleep(5)
                    tries -= 1
                    if tries == 0:
                        raise
        except UnableToConnectException:
            if allow_offline is True:
                logger.info('Allowed offline node during mds service removal')
            else:
                raise

    @staticmethod
    def sync_vdisk_to_reality(vdisk):
        """
        Syncs a vdisk to reality (except hypervisor)
        :param vdisk: vDisk to synchronize
        :type vdisk: VDisk

        :return: None
        """
        vdisk.reload_client()
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
            if config[0]['ip'] == storagerouter.ip and config[0]['port'] == service.ports[0]:
                junction.is_master = True
                junction.save()
                if storagerouter.ip not in mds_dict:
                    mds_dict[storagerouter.ip] = []
                mds_dict[storagerouter.ip].append(service.ports[0])
            elif storagerouter.ip in config_dict and service.ports[0] in config_dict[storagerouter.ip]:
                junction.is_master = False
                junction.save()
                if storagerouter.ip not in mds_dict:
                    mds_dict[storagerouter.ip] = []
                mds_dict[storagerouter.ip].append(service.ports[0])
            else:
                junction.delete()
        for ip, ports in config_dict.iteritems():
            for port in ports:
                if ip not in mds_dict or port not in mds_dict[ip]:
                    service = ServiceList.get_by_ip_ports(ip, [port])
                    if service is not None:
                        mds_service_vdisk = MDSServiceVDisk()
                        mds_service_vdisk.vdisk = vdisk
                        mds_service_vdisk.mds_service = service.mds_service
                        mds_service_vdisk.is_master = config[0]['ip'] == service.storagerouter.ip and config[0]['port'] == service.ports[0]
                        mds_service_vdisk.save()

    @staticmethod
    def ensure_safety(vdisk, excluded_storagerouters=None):
        """
        Ensures (or tries to ensure) the safety of a given vdisk (except hypervisor).
        Assumptions:
        * A local overloaded master is better than a non-local non-overloaded master
        * Prefer master/services to be on different hosts, a subsequent slave on the same node doesn't add safety
        * Don't actively overload services (e.g. configure an MDS as slave causing it to get overloaded)
        * Too much safety is not wanted (it adds loads to nodes while not required)
        :param vdisk: vDisk to calculate a new safety for
        :type vdisk: VDisk

        :param excluded_storagerouters: Storagerouters to leave out of calculation (Eg: When 1 is down or unavailable)
        :type excluded_storagerouters: list

        :return: None
        """
        def _add_suitable_nodes(local_failure_domain, local_safety):
            if len(nodes) < local_safety:
                for local_load in sorted(failure_domain_load_dict[local_failure_domain]):
                    for local_service in failure_domain_load_dict[local_failure_domain][local_load]:
                        if len(nodes) < local_safety and local_service.storagerouter.ip not in nodes:
                            try:
                                SSHClient(local_service.storagerouter)
                                new_services.append(local_service)
                                nodes.add(local_service.storagerouter.ip)
                            except UnableToConnectException:
                                logger.debug('MDS safety: vDisk {0}: Skipping storagerouter with IP {1} as it is unreachable'.format(vdisk.guid, service.storagerouter.ip))
            return nodes, new_services

        ######################
        # GATHER INFORMATION #
        ######################
        logger.debug('MDS safety: vDisk {0}: Start checkup for virtual disk {1}'.format(vdisk.guid, vdisk.name))
        vdisk.reload_client()
        vdisk.invalidate_dynamics(['info', 'storagedriver_id', 'storagerouter_guid'])
        if vdisk.storagerouter_guid is None:
            raise ValueError('Cannot ensure MDS safety for vDisk {0} with guid {1} because vDisk is not attached to any Storage Router'.format(vdisk.name, vdisk.guid))

        if excluded_storagerouters is None:
            excluded_storagerouters = []

        # Sorted was added merely for unittests, because they rely on specific order of services and their ports
        # Default sorting behavior for relations used to be based on order in which relations were added
        # Now sorting is based on guid (DAL speedup changes)
        services = sorted([mds_service.service for mds_service in vdisk.vpool.mds_services
                           if mds_service.service.storagerouter not in excluded_storagerouters], key=lambda k: k.ports)
        nodes = set(service.storagerouter.ip for service in services)

        vdisk_storagerouter = StorageRouter(vdisk.storagerouter_guid)
        primary_failure_domain = vdisk_storagerouter.primary_failure_domain
        if vdisk.secondary_failure_domain is not None:
            secondary_failure_domain = vdisk.secondary_failure_domain
        else:
            secondary_failure_domain = vdisk_storagerouter.secondary_failure_domain

        failure_domain_load_dict = {primary_failure_domain: {}}
        failure_domain_used_services_dict = {primary_failure_domain: []}
        failure_domain_available_services_dict = {primary_failure_domain: []}
        storage_router_failure_domain_dict = dict((storage_router, primary_failure_domain) for storage_router in primary_failure_domain.primary_storagerouters)

        if secondary_failure_domain is not None:
            failure_domain_load_dict[secondary_failure_domain] = {}
            failure_domain_used_services_dict[secondary_failure_domain] = []
            failure_domain_available_services_dict[secondary_failure_domain] = []
            storage_router_failure_domain_dict.update(dict((storage_router, secondary_failure_domain) for storage_router in secondary_failure_domain.primary_storagerouters))

        services_load = {}
        service_per_key = {}
        for service in services:
            services_load[service] = MDSServiceController.get_mds_load(service.mds_service)
            service_per_key['{0}:{1}'.format(service.storagerouter.ip, service.ports[0])] = service

        configs = vdisk.info['metadata_backend_config']  # Ordered MASTER, SLAVE (backup failure domain of master)
        for config in configs:
            config['key'] = '{0}:{1}'.format(config['ip'], config['port'])

        ###################################
        # VERIFY RECONFIGURATION REQUIRED #
        ###################################
        master_service = None
        reconfigure_reasons = []
        if len(configs) > 0:
            config = configs.pop(0)
            if config['key'] in service_per_key:
                master_service = service_per_key.get(config['key'])
            else:
                reconfigure_reasons.append('Master ({0}:{1}) cannot be used anymore'.format(config['ip'], config['port']))
        slave_services = []
        for config in configs:
            if config['key'] in service_per_key:
                slave_services.append(service_per_key[config['key']])
            else:
                reconfigure_reasons.append('Slave ({0}:{1}) cannot be used anymore'.format(config['ip'], config['port']))

        # If MDS already in use, take current load, else take next load
        tlogs = EtcdConfiguration.get('/ovs/framework/storagedriver|mds_tlogs')
        safety = EtcdConfiguration.get('/ovs/framework/storagedriver|mds_safety')
        max_load = EtcdConfiguration.get('/ovs/framework/storagedriver|mds_maxload')
        for service in services:
            if service == master_service or service in slave_services:
                load = services_load[service][0]
                if service.storagerouter in storage_router_failure_domain_dict:  # Services in use per failure domain
                    failure_domain_used_services_dict[storage_router_failure_domain_dict[service.storagerouter]].append(service)
                else:
                    reconfigure_reasons.append('Service {0} cannot be used anymore because storagerouter with IP {1} is not part of the failure domains'.format(service.name, service.storagerouter.ip))
            else:
                load = services_load[service][1]
            services_load[service] = load
            if service.storagerouter in storage_router_failure_domain_dict:  # All services available in model per failure domain
                failure_domain = storage_router_failure_domain_dict[service.storagerouter]
                failure_domain_available_services_dict[failure_domain].append(service)
                if load <= max_load:
                    if load not in failure_domain_load_dict[failure_domain]:
                        failure_domain_load_dict[failure_domain][load] = []
                    failure_domain_load_dict[failure_domain][load].append(service)

        service_nodes = []
        if master_service is not None:
            service_nodes.append(master_service.storagerouter.ip)
        for service in slave_services:
            ip = service.storagerouter.ip
            if ip in service_nodes:
                reconfigure_reasons.append('Multiple MDS services on the same node')
            else:
                service_nodes.append(ip)

        if len(service_nodes) > safety:
            reconfigure_reasons.append('Too much safety')
        if len(service_nodes) < safety and len(service_nodes) < len(nodes):
            reconfigure_reasons.append('Not enough safety')
        if master_service is not None and services_load[master_service] > max_load:
            reconfigure_reasons.append('Master overloaded')
        if master_service is not None and master_service.storagerouter_guid != vdisk.storagerouter_guid:
            reconfigure_reasons.append('Master is not local')
        if any(service for service in slave_services if services_load[service] > max_load):
            reconfigure_reasons.append('One or more slaves overloaded')

        # Check reconfigure required based upon failure domains
        recommended_primary = math.ceil(safety / 2.0) if secondary_failure_domain is not None else safety
        recommended_secondary = safety - recommended_primary

        if master_service is not None and master_service not in failure_domain_used_services_dict[primary_failure_domain]:
            # Master service not present in primary failure domain
            reconfigure_reasons.append('Master service not in primary failure domain')

        primary_services_used = len(failure_domain_used_services_dict[primary_failure_domain])
        primary_services_available = len(failure_domain_available_services_dict[primary_failure_domain])
        if primary_services_used < recommended_primary and primary_services_used < primary_services_available:
            # More services can be used in primary failure domain
            reconfigure_reasons.append('Not enough services in use in primary failure domain')

        if secondary_failure_domain is not None:
            # More services can be used in secondary failure domain
            secondary_services_used = len(failure_domain_used_services_dict[secondary_failure_domain])
            secondary_services_available = len(failure_domain_available_services_dict[secondary_failure_domain])
            if secondary_services_used < recommended_secondary and secondary_services_used < secondary_services_available:
                reconfigure_reasons.append('Not enough services in use in secondary failure domain')

            # If secondary failure domain present, check order in which the slave services are configured
            secondary = False
            for slave_service in slave_services:
                if secondary is True and slave_service in failure_domain_used_services_dict[primary_failure_domain]:
                    reconfigure_reasons.append('A slave in secondary failure domain has priority over a slave in primary failure domain')
                    break
                if slave_service in failure_domain_used_services_dict[secondary_failure_domain]:
                    secondary = True

        if not reconfigure_reasons:
            logger.debug('MDS safety: vDisk {0}: No reconfiguration required'.format(vdisk.guid))
            MDSServiceController.sync_vdisk_to_reality(vdisk)
            return

        logger.debug('MDS safety: vDisk {0}: Reconfiguration required. Reasons:'.format(vdisk.guid))
        for reason in reconfigure_reasons:
            logger.debug('MDS safety: vDisk {0}:    * {1}'.format(vdisk.guid, reason))

        # Check whether the master (if available) is non-local to the vdisk and/or is overloaded
        new_services = []
        master_ok = master_service is not None
        if master_ok is True:
            master_ok = master_service.storagerouter_guid == vdisk.storagerouter_guid and services_load[master_service] <= max_load

        ############################
        # CREATE NEW CONFIGURATION #
        ############################
        if master_ok:
            # Add this master to the fresh configuration
            new_services.append(master_service)
        else:
            # Try to find the best non-overloaded LOCAL MDS slave to make master
            candidate_master_service = None
            candidate_master_load = 0
            local_mds = None
            local_mds_load = 0
            for service in failure_domain_available_services_dict[primary_failure_domain]:
                load = services_load[service]
                if load <= max_load and service.storagerouter_guid == vdisk.storagerouter_guid:
                    if local_mds is None or local_mds_load > load:
                        # This service is a non-overloaded local MDS
                        local_mds = service
                        local_mds_load = load
                    if service in slave_services:
                        if candidate_master_service is None or candidate_master_load > load:
                            # This service is a non-overloaded local slave
                            candidate_master_service = service
                            candidate_master_load = load
            if candidate_master_service is not None:
                # A non-overloaded local slave was found.
                client = MetadataServerClient.load(candidate_master_service)
                try:
                    amount_of_tlogs = client.catch_up(str(vdisk.volume_id), True)
                except RuntimeError as ex:
                    if 'Namespace does not exist' in ex.message:
                        client.create_namespace(str(vdisk.volume_id))
                        amount_of_tlogs = client.catch_up(str(vdisk.volume_id), True)
                    else:
                        raise
                if amount_of_tlogs < tlogs:
                    # Almost there. Catching up right now, and continue as soon as it's up-to-date
                    start = time.time()
                    client.catch_up(str(vdisk.volume_id), False)
                    logger.debug('MDS safety: vDisk {0}: Catchup took {1}s'.format(vdisk.guid, round(time.time() - start, 2)))
                    # It's up to date, so add it as a new master
                    new_services.append(candidate_master_service)
                    if master_service is not None:
                        # The current master (if available) is now candidate to become one of the slaves
                        slave_services.append(master_service)
                else:
                    # It's not up to date, keep the previous master (if available) and give the local slave
                    # some more time to catch up
                    if master_service is not None:
                        new_services.append(master_service)
                    new_services.append(candidate_master_service)
                if candidate_master_service in slave_services:
                    slave_services.remove(candidate_master_service)
            else:
                # There's no non-overloaded local slave found. Keep the current master (if available) and add
                # a local MDS (if available) as slave
                if master_service is not None:
                    new_services.append(master_service)
                if local_mds is not None:
                    new_services.append(local_mds)
                    if local_mds in slave_services:
                        slave_services.remove(local_mds)

        # At this point, there might (or might not) be a (new) master, and a (catching up) slave. The rest of the non-local
        # MDS nodes must now be added to the configuration until the safety is reached. There's always one extra
        # slave recycled to make sure there's always an (almost) up-to-date slave ready for failover
        nodes = set(service.storagerouter.ip for service in new_services)

        # Recycle slave for faster failover
        secondary_node_count = 0
        service_to_recycle = None
        if len(nodes) < safety:
            if recommended_primary > 1:  # If primary is 1, we only have master in primary
                # Try to recycle slave which is in primary failure domain
                for load in sorted(failure_domain_load_dict[primary_failure_domain]):
                    for service in failure_domain_load_dict[primary_failure_domain][load]:
                        if service_to_recycle is None and service in slave_services and service.storagerouter.ip not in nodes:
                            try:
                                SSHClient(service.storagerouter)
                                service_to_recycle = service
                            except UnableToConnectException:
                                logger.debug('MDS safety: vDisk {0}: Skipping storagerouter with IP {1} as it is unreachable'.format(vdisk.guid, service.storagerouter.ip))
            # Try to recycle slave which is in secondary failure domain if none found in primary
            if service_to_recycle is None and secondary_failure_domain is not None:
                for load in sorted(failure_domain_load_dict[secondary_failure_domain]):
                    for service in failure_domain_load_dict[secondary_failure_domain][load]:
                        if service_to_recycle is None and service in slave_services and service.storagerouter.ip not in nodes:
                            try:
                                SSHClient(service.storagerouter)
                                service_to_recycle = service
                                secondary_node_count = 1  # We do not want to configure the secondary slave BEFORE the primary slaves
                            except UnableToConnectException:
                                logger.debug('MDS safety: vDisk {0}: Skipping storagerouter with IP {1} as it is unreachable'.format(vdisk.guid, service.storagerouter.ip))
        if service_to_recycle is not None:
            slave_services.remove(service_to_recycle)
            if secondary_node_count == 0:  # Add service to recycle because its in primary failure domain
                new_services.append(service_to_recycle)
                nodes.add(service_to_recycle.storagerouter.ip)

        # Add extra (new) slaves until primary safety reached
        nodes, new_services = _add_suitable_nodes(local_failure_domain=primary_failure_domain,
                                                  local_safety=recommended_primary)

        # Add recycled secondary slave after primary slaves have been added
        if secondary_node_count == 1:
            new_services.append(service_to_recycle)
            nodes.add(service_to_recycle.storagerouter.ip)

        # Add extra (new) slaves until secondary safety reached
        if secondary_failure_domain is not None:
            nodes, new_services = _add_suitable_nodes(local_failure_domain=secondary_failure_domain,
                                                      local_safety=safety)
            # Add extra slaves from primary failure domain in case no suitable nodes found in secondary failure domain
            if len(nodes) < safety:
                nodes, new_services = _add_suitable_nodes(local_failure_domain=primary_failure_domain,
                                                          local_safety=safety)

        # Build the new configuration and update the vdisk
        configs = []
        for service in new_services:
            client = MetadataServerClient.load(service)
            client.create_namespace(str(vdisk.volume_id))
            # noinspection PyArgumentList
            configs.append(MDSNodeConfig(address=str(service.storagerouter.ip),
                                         port=service.ports[0]))
        vdisk.storagedriver_client.update_metadata_backend_config(volume_id=str(vdisk.volume_id),
                                                                  metadata_backend_config=MDSMetaDataBackendConfig(configs))
        MDSServiceController.sync_vdisk_to_reality(vdisk)
        logger.debug('MDS safety: vDisk {0}: Completed'.format(vdisk.guid))

    @staticmethod
    def get_preferred_mds(storagerouter, vpool):
        """
        Gets the MDS on this StorageRouter/VPool pair which is preferred to achieve optimal balancing
        :param storagerouter: Storagerouter to retrieve the best MDS service for
        :type storagerouter: StorageRouter

        :param vpool: vPool to retrieve the best MDS service for
        :type vpool: VPool

        :return: Preferred MDS service (least loaded), current load on that MDS service
        :rtype: tuple
        """
        mds_service = (None, float('inf'))
        for current_mds_service in vpool.mds_services:
            if current_mds_service.service.storagerouter_guid == storagerouter.guid:
                load = MDSServiceController.get_mds_load(current_mds_service)[0]
                if mds_service is None or load < mds_service[1]:
                    mds_service = (current_mds_service, load)
        return mds_service

    @staticmethod
    def get_mds_load(mds_service):
        """
        Gets a 'load' for an MDS service based on its capacity and the amount of assigned VDisks
        :param mds_service: MDS service the get current load for
        :type mds_service: MDSService

        :return: Load of the MDS service
        :rtype: tuple
        """
        service_capacity = float(mds_service.capacity)
        if service_capacity < 0:
            return 50, 50
        if service_capacity == 0:
            return float('inf'), float('inf')
        usage = len(mds_service.vdisks_guids)
        return round(usage / service_capacity * 100.0, 5), round((usage + 1) / service_capacity * 100.0, 5)

    @staticmethod
    def get_mds_storagedriver_config_set(vpool, check_online=False):
        """
        Builds a configuration for all StorageRouters from a given VPool with following goals:
        * Primary MDS is the local one
        * All slaves are on different hosts
        * Maximum `mds.safety` nodes are returned
        The configuration returned is the default configuration used by the volumedriver of which in normal use-cases
        only the 1st entry is used, because at volume creation time, the volumedriver needs to create 1 master MDS
        During ensure_safety, we actually create/set the MDS slaves for each volume

        :param vpool: vPool to get storagedriver configuration for
        :type vpool: VPool

        :param check_online: Check whether the storage routers are actually responsive
        :type check_online: bool

        :return: MDS configuration for a vPool
        :rtype: dict
        """
        mds_per_storagerouter = {}
        mds_per_load = {}
        for storagedriver in vpool.storagedrivers:
            storagerouter = storagedriver.storagerouter
            if check_online is True:
                try:
                    client = SSHClient(storagerouter)
                    client.run('pwd')
                except UnableToConnectException:
                    continue
            mds_service, load = MDSServiceController.get_preferred_mds(storagerouter, vpool)
            if mds_service is None:
                raise RuntimeError('Could not find an MDS service')
            mds_per_storagerouter[storagerouter] = {'host': storagerouter.ip, 'port': mds_service.service.ports[0]}
            if load not in mds_per_load:
                mds_per_load[load] = []
            mds_per_load[load].append(storagerouter)

        safety = EtcdConfiguration.get('/ovs/framework/storagedriver|mds_safety')
        config_set = {}
        for storagerouter, ip_info in mds_per_storagerouter.iteritems():
            primary_failure_domain = storagerouter.primary_failure_domain
            secondary_failure_domain = storagerouter.secondary_failure_domain
            config_set[storagerouter.guid] = [ip_info]
            for load in sorted(mds_per_load):
                if len(config_set[storagerouter.guid]) >= safety:
                    break
                other_storagerouters = mds_per_load[load]
                random.shuffle(other_storagerouters)
                for other_storagerouter in other_storagerouters:
                    if len(config_set[storagerouter.guid]) >= safety:
                        break
                    if other_storagerouter != storagerouter and other_storagerouter in primary_failure_domain.primary_storagerouters:
                        config_set[storagerouter.guid].append(mds_per_storagerouter[other_storagerouter])
            if secondary_failure_domain is not None:
                for load in sorted(mds_per_load):
                    if len(config_set[storagerouter.guid]) >= safety:
                        break
                    other_storagerouters = mds_per_load[load]
                    random.shuffle(other_storagerouters)
                    for other_storagerouter in other_storagerouters:
                        if len(config_set[storagerouter.guid]) >= safety:
                            break
                        if other_storagerouter != storagerouter and other_storagerouter in secondary_failure_domain.primary_storagerouters:
                            config_set[storagerouter.guid].append(mds_per_storagerouter[other_storagerouter])
        return config_set

    @staticmethod
    @celery.task(name='ovs.mds.mds_checkup', schedule=crontab(minute='30', hour='0,4,8,12,16,20'))
    @ensure_single(task_name='ovs.mds.mds_checkup', mode='CHAINED')
    def mds_checkup():
        """
        Validates the current MDS setup/configuration and takes actions where required
        """
        logger.info('MDS checkup - Started')
        mds_dict = {}
        for vpool in VPoolList.get_vpools():
            logger.info('MDS checkup - vPool {0}'.format(vpool.name))
            mds_dict[vpool] = {}
            for mds_service in vpool.mds_services:
                storagerouter = mds_service.service.storagerouter
                if storagerouter not in mds_dict[vpool]:
                    mds_dict[vpool][storagerouter] = {'client': None,
                                                      'services': []}
                    try:
                        client = SSHClient(storagerouter, username = 'root')
                        client.run('pwd')
                        mds_dict[vpool][storagerouter]['client'] = client
                        logger.info('MDS checkup - vPool {0} - Storage Router {1} - ONLINE'.format(vpool.name, storagerouter.name))
                    except UnableToConnectException:
                        logger.info('MDS checkup - vPool {0} - Storage Router {1} - OFFLINE'.format(vpool.name, storagerouter.name))
                mds_dict[vpool][storagerouter]['services'].append(mds_service)

        failures = []
        max_load = EtcdConfiguration.get('/ovs/framework/storagedriver|mds_maxload')
        for vpool, storagerouter_info in mds_dict.iteritems():
            # 1. First, make sure there's at least one MDS on every StorageRouter that's not overloaded
            # If not, create an extra MDS for that StorageRouter
            for storagerouter in storagerouter_info:
                client = mds_dict[vpool][storagerouter]['client']
                mds_services = mds_dict[vpool][storagerouter]['services']
                has_room = False
                for mds_service in mds_services[:]:
                    if mds_service.capacity == 0 and len(mds_service.vdisks_guids) == 0:
                        logger.info('MDS checkup - Removing mds_service {0} for vPool {1}'.format(mds_service.number, vpool.name))
                        MDSServiceController.remove_mds_service(mds_service, vpool, reconfigure=True, allow_offline=client is None)
                        mds_services.remove(mds_service)
                for mds_service in mds_services:
                    _, load = MDSServiceController.get_mds_load(mds_service)
                    if load < max_load:
                        has_room = True
                        break
                logger.info('MDS checkup - vPool {0} - Storage Router {1} - Capacity available: {2}'.format(vpool.name, storagerouter.name, has_room))
                if has_room is False and client is not None:
                    mds_service = MDSServiceController.prepare_mds_service(storagerouter=storagerouter,
                                                                           vpool=vpool,
                                                                           fresh_only=False,
                                                                           reload_config=True)
                    if mds_service is None:
                        raise RuntimeError('Could not add MDS node')
                    mds_services.append(mds_service)
            mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool, True)
            for storagerouter in storagerouter_info:
                client = mds_dict[vpool][storagerouter]['client']
                if client is None:
                    logger.info('MDS checkup - vPool {0} - Storage Router {1} - Marked as offline, not setting default MDS configuration'.format(vpool.name, storagerouter.name))
                    continue
                storagedriver = [sd for sd in storagerouter.storagedrivers if sd.vpool_guid == vpool.guid][0]
                storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
                storagedriver_config.load()
                if storagedriver_config.is_new is False:
                    logger.info('MDS checkup - vPool {0} - Storage Router {1} - Storing default MDS configuration: {2}'.format(vpool.name, storagerouter.name, mds_config_set[storagerouter.guid]))
                    storagedriver_config.clean()  # Clean out obsolete values
                    storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[storagerouter.guid])
                    storagedriver_config.save(client)
            # 2. Per VPool, execute a safety check, making sure the master/slave configuration is optimal.
            logger.info('MDS checkup - vPool {0} - Ensuring safety for all virtual disks'.format(vpool.name))
            for vdisk in vpool.vdisks:
                try:
                    MDSServiceController.ensure_safety(vdisk)
                except Exception as ex:
                    failures.append('Ensure safety for vDisk {0} with guid {1} failed with error: {2}'.format(vdisk.name, vdisk.guid, ex))
        if len(failures) > 0:
            raise Exception('\n - ' + '\n - '.join(failures))
        logger.info('MDS checkup - Finished')


if __name__ == '__main__':
    from ovs.dal.lists.storagerouterlist import StorageRouterList
    try:
        while True:
            output = ['',
                      'Open vStorage - MDS debug information',
                      '=====================================',
                      'timestamp: {0}'.format(time.time()),
                      '']
            for _sr in StorageRouterList.get_storagerouters():
                output.append('+ {0} ({1})'.format(_sr.name, _sr.ip))
                vpools = set(sd.vpool for sd in _sr.storagedrivers)
                for _vpool in vpools:
                    output.append('  + {0}'.format(_vpool.name))
                    for _mds_service in _vpool.mds_services:
                        if _mds_service.service.storagerouter_guid == _sr.guid:
                            masters, slaves = 0, 0
                            for _junction in _mds_service.vdisks:
                                if _junction.is_master:
                                    masters += 1
                                else:
                                    slaves += 1
                            capacity = _mds_service.capacity
                            if capacity == -1:
                                capacity = 'infinite'
                            _load, _ = MDSServiceController.get_mds_load(_mds_service)
                            if _load == float('inf'):
                                _load = 'infinite'
                            else:
                                _load = '{0}%'.format(round(_load, 2))
                            output.append('    + {0} - port {1} - {2} master(s), {3} slave(s) - capacity: {4}, load: {5}'.format(
                                _mds_service.number, _mds_service.service.ports[0], masters, slaves, capacity, _load
                            ))
            output += ['',
                       'Press ^C to exit',
                       '']
            print '\x1b[2J\x1b[H' + '\n'.join(output)
            time.sleep(1)
    except KeyboardInterrupt:
        pass

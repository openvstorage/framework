# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MDSService module
"""
import time
import random

from celery.schedules import crontab
from ovs.celery_run import celery
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service as DalService
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration, MetadataServerClient
from ovs.extensions.generic.system import System
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.lib.helpers.decorators import ensure_single
from ovs.log.logHandler import LogHandler
from volumedriver.storagerouter.storagerouterclient import MDSNodeConfig, MDSMetaDataBackendConfig
from volumedriver.storagerouter import storagerouterclient

logger = LogHandler.get('lib', name='mds')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
storagerouterclient.Logger.enableLogging()


class MDSServiceController(object):
    """
    Contains all BLL related to MDSServices
    """

    @staticmethod
    def prepare_mds_service(client, storagerouter, vpool, fresh_only=True, reload_config=False):
        """
        Prepares an MDS service:
        * Creates the required configuration
        * Sets up the service files

        Assumes the StorageRouter and VPool are already configured with a StorageDriver and that all model-wise
        configuration regarding both is completed.
        """
        from ovs.lib.storagedriver import StorageDriverController

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
                occupied_ports.append(service.ports[0])
        port = System.get_free_ports(Configuration.get('ovs.ports.mds'),
                                     exclude=occupied_ports, nr=1, client=client)[0]

        # Add service to the model
        service = DalService()
        service.name = 'metadataserver_{0}_{1}'.format(vpool.name, service_number)
        service.type = mdsservice_type
        service.storagerouter = storagerouter
        service.ports = [port]
        service.save()
        mds_service = MDSService()
        mds_service.service = service
        mds_service.vpool = vpool
        mds_service.number = service_number
        mds_service.save()
        scrub_partition = None
        db_partition = None
        for disk in storagerouter.disks:
            for partition in disk.partitions:
                if DiskPartition.ROLES.DB in partition.roles:
                    db_partition = partition
                if DiskPartition.ROLES.SCRUB in partition.roles:
                    scrub_partition = partition
        if scrub_partition is None or db_partition is None:
            raise RuntimeError('Could not find DB or SCRUB partition on StorageRouter {0}'.format(storagerouter.name))
        StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                           'role': DiskPartition.ROLES.DB,
                                                                           'sub_role': StorageDriverPartition.SUBROLE.MDS,
                                                                           'partition': db_partition,
                                                                           'mds_service': mds_service})
        StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                           'role': DiskPartition.ROLES.SCRUB,
                                                                           'sub_role': StorageDriverPartition.SUBROLE.MDS,
                                                                           'partition': scrub_partition,
                                                                           'mds_service': mds_service})
        mds_nodes = []
        for service in mdsservice_type.services:
            if service.storagerouter_guid == storagerouter.guid:
                mds_service = service.mds_service
                if mds_service.vpool_guid == vpool.guid:
                    mds_nodes.append({'host': service.storagerouter.ip,
                                      'port': service.ports[0],
                                      'db_directory': [sd_partition.path for sd_partition in mds_service.storagedriver_partitions
                                                       if sd_partition.role == DiskPartition.ROLES.DB and sd_partition.sub_role == StorageDriverPartition.SUBROLE.MDS][0],
                                      'scratch_directory': [sd_partition.path for sd_partition in mds_service.storagedriver_partitions
                                                            if sd_partition.role == DiskPartition.ROLES.SCRUB and sd_partition.sub_role == StorageDriverPartition.SUBROLE.MDS][0]})

        # Generate the correct section in the Storage Driver's configuration
        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.name)
        storagedriver_config.load(client)
        storagedriver_config.clean()  # Clean out obsolete values
        storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
        storagedriver_config.save(client, reload_config=reload_config)

        return mds_service

    @staticmethod
    def remove_mds_service(mds_service, client, storagerouter, vpool, reload_config):
        """
        Removes an MDS service
        """
        if len(mds_service.vdisks_guids) > 0:
            raise RuntimeError('Cannot remove MDSService that is still serving disks')

        mdsservice_type = ServiceTypeList.get_by_name('MetadataServer')

        # Clean up model
        directories_to_clean = []
        for sd_partition in mds_service.storagedriver_partitions:
            directories_to_clean.append(sd_partition.path)
            sd_partition.delete()

        service = mds_service.service
        mds_service.delete()
        service.delete()

        # Generate new mds_nodes section
        mds_nodes = []
        for service in mdsservice_type.services:
            if service.storagerouter_guid == storagerouter.guid:
                mds_service = service.mds_service
                if mds_service.vpool_guid == vpool.guid:
                    mds_nodes.append({'host': service.storagerouter.ip,
                                      'port': service.ports[0],
                                      'db_directory': [sd_partition.path for sd_partition in mds_service.storagedriver_partitions if sd_partition.role == DiskPartition.ROLES.DB][0],
                                      'scratch_directory': [sd_partition.path for sd_partition in mds_service.storagedriver_partitions if sd_partition.role == DiskPartition.ROLES.SCRUB][0]})

        # Generate the correct section in the Storage Driver's configuration
        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.name)
        storagedriver_config.load(client)
        storagedriver_config.clean()  # Clean out obsolete values
        storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
        storagedriver_config.save(client, reload_config=reload_config)

        tries = 5
        cleaned = False
        while tries > 0 and cleaned is False:
            try:
                client.dir_delete(directories_to_clean)
                logger.debug('MDS files cleaned up')
                cleaned = True
            except Exception:
                time.sleep(5)
                logger.debug('Waiting for the MDS service to go down...')
                tries -= 1

    @staticmethod
    def sync_vdisk_to_reality(vdisk):
        """
        Syncs a vdisk to reality (except hypervisor)
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
        """

        logger.debug('Ensuring MDS safety for vdisk {0}'.format(vdisk.guid))
        vdisk.reload_client()
        if excluded_storagerouters is None:
            excluded_storagerouters = []
        maxload = Configuration.get('ovs.storagedriver.mds.maxload')
        safety = Configuration.get('ovs.storagedriver.mds.safety')
        tlogs = Configuration.get('ovs.storagedriver.mds.tlogs')
        services = [mds_service.service for mds_service in vdisk.vpool.mds_services
                    if mds_service.service.storagerouter not in excluded_storagerouters]
        nodes = set(service.storagerouter.ip for service in services)
        services_load = {}
        service_per_key = {}
        for service in services:
            load, load_plus = MDSServiceController.get_mds_load(service.mds_service)
            services_load[service.guid] = load, load_plus
            service_per_key['{0}:{1}'.format(service.storagerouter.ip, service.ports[0])] = service

        # List current configuration and filter out excluded services
        reconfigure_required = False
        reconfigure_reasons = []
        vdisk.invalidate_dynamics(['info', 'storagedriver_id', 'storagerouter_guid'])
        configs = vdisk.info['metadata_backend_config']
        for config in configs:
            config['key'] = '{0}:{1}'.format(config['ip'], config['port'])
        master_service = None
        if len(configs) > 0:
            config = configs[0]
            if config['key'] in service_per_key:
                master_service = service_per_key.get(config['key'])
                configs.remove(config)
            else:
                reconfigure_required = True
                reconfigure_reasons.append('Master ({0}:{1}) cannot be used anymore'.format(config['ip'], config['port']))
        slave_services = []
        for config in configs:
            if config['key'] in service_per_key:
                slave_services.append(service_per_key[config['key']])
            else:
                reconfigure_required = True
                reconfigure_reasons.append('Slave ({0}:{1}) cannot be used anymore'.format(config['ip'], config['port']))

        # Fix services_load
        services_per_load = {}
        for service in services:
            if service == master_service or service in slave_services:
                load = services_load[service.guid][0]
            else:
                load = services_load[service.guid][1]
            services_load[service.guid] = load
            if load not in services_per_load:
                services_per_load[load] = []
            services_per_load[load].append(service)

        # Further checks if a reconfiguration is required.
        service_nodes = []
        if master_service is not None:
            service_nodes.append(master_service.storagerouter.ip)
        for service in slave_services:
            ip = service.storagerouter.ip
            if ip in service_nodes:
                reconfigure_required = True
                reconfigure_reasons.append('Multiple MDS services on the same node')
            else:
                service_nodes.append(ip)
        if len(service_nodes) > safety:
            # Too much safety
            reconfigure_required = True
            reconfigure_reasons.append('Too much safety')
        if len(service_nodes) < safety and len(service_nodes) < len(nodes):
            # Insufficient MDS services configured while there should be sufficient nodes available
            reconfigure_required = True
            reconfigure_reasons.append('Not enough safety')
        if master_service is not None and services_load[master_service.guid] > maxload:
            # The master service is overloaded
            reconfigure_required = True
            reconfigure_reasons.append('Master overloaded')
        if master_service is not None and master_service.storagerouter_guid != vdisk.storagerouter_guid:
            # The master is not local
            reconfigure_required = True
            reconfigure_reasons.append('Master is not local')
        if any(service for service in slave_services if services_load[service.guid] > maxload):
            # There's a slave service overloaded
            reconfigure_required = True
            reconfigure_reasons.append('One or more slaves overloaded')

        if reconfigure_required is False:
            logger.debug('No reconfiguration required for vdisk {0}'.format(vdisk.guid))
            MDSServiceController.sync_vdisk_to_reality(vdisk)
            return

        logger.debug('Reconfiguration required for vdisk {0}:'.format(vdisk.guid))
        for reason in reconfigure_reasons:
            logger.debug('Reason: {0} - vdisk {1}'.format(reason, vdisk.guid))
        # Prepare fresh configuration
        new_services = []

        # Check whether the master (if available) is non-local to the vdisk and/or is overloaded
        master_ok = master_service is not None
        if master_ok is True:
            master_ok = master_service.storagerouter_guid == vdisk.storagerouter_guid and services_load[master_service.guid] <= maxload

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
                client = MetadataServerClient.load(candidate_master)
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
                    logger.debug('MDS catch up for vdisk {0} took {1}s'.format(vdisk.guid, round(time.time() - start, 2)))
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
                if candidate_master in slave_services:
                    slave_services.remove(candidate_master)
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
        loads = sorted(load for load in services_per_load.keys() if load <= maxload)
        nodes = set(service.storagerouter.ip for service in new_services)
        slave_added = False
        if len(nodes) < safety:
            for load in loads:
                for service in services_per_load[load]:
                    if slave_added is False and service in slave_services and service.storagerouter.ip not in nodes:
                        try:
                            SSHClient(service.storagerouter)
                            new_services.append(service)
                            slave_services.remove(service)
                            nodes.add(service.storagerouter.ip)
                            slave_added = True
                        except UnableToConnectException:
                            logger.debug('Skip {0} as it is unreachable'.format(service.storagerouter.ip))
        if len(nodes) < safety:
            for load in loads:
                for service in services_per_load[load]:
                    if len(nodes) < safety and service.storagerouter.ip not in nodes:
                        try:
                            SSHClient(service.storagerouter)
                            new_services.append(service)
                            nodes.add(service.storagerouter.ip)
                        except UnableToConnectException:
                            logger.debug('Skip {0} as it is unreachable'.format(service.storagerouter.ip))

        # Build the new configuration and update the vdisk
        configs = []
        for service in new_services:
            client = MetadataServerClient.load(service)
            client.create_namespace(str(vdisk.volume_id))
            configs.append(MDSNodeConfig(address=str(service.storagerouter.ip),
                                         port=service.ports[0]))
        vdisk.storagedriver_client.update_metadata_backend_config(
            volume_id=str(vdisk.volume_id),
            metadata_backend_config=MDSMetaDataBackendConfig(configs)
        )
        MDSServiceController.sync_vdisk_to_reality(vdisk)
        logger.debug('Ensuring MDS safety for vdisk {0} completed'.format(vdisk.guid))

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
        Gets a 'load' for an MDS service based on its capacity and the amount of assigned VDisks
        """
        service_capacity = float(mds_service.capacity)
        if service_capacity < 0:
            return 50, 50
        if service_capacity == 0:
            return float('inf'), float('inf')
        usage = len(mds_service.vdisks_guids)
        return round(usage / service_capacity * 100.0, 5), round((usage + 1) / service_capacity * 100.0, 5)

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
            mds_per_storagerouter[storagerouter.guid] = {'host': storagerouter.ip, 'port': mds_service.service.ports[0]}
            if load not in mds_per_load:
                mds_per_load[load] = []
            mds_per_load[load].append(storagerouter.guid)

        safety = Configuration.get('ovs.storagedriver.mds.safety')
        config_set = {}
        for storagerouter_guid in mds_per_storagerouter:
            config_set[storagerouter_guid] = [mds_per_storagerouter[storagerouter_guid]]
            for load in sorted(mds_per_load.keys()):
                if len(config_set[storagerouter_guid]) >= safety:
                    break
                sr_guids = mds_per_load[load]
                random.shuffle(sr_guids)
                for sr_guid in sr_guids:
                    if len(config_set[storagerouter_guid]) >= safety:
                        break
                    if sr_guid != storagerouter_guid:
                        config_set[storagerouter_guid].append(mds_per_storagerouter[sr_guid])
        return config_set

    @staticmethod
    @celery.task(name='ovs.mds.mds_checkup', bind=True, schedule=crontab(minute='30', hour='0,6,12,18'))
    @ensure_single(['ovs.mds.mds_checkup'])
    def mds_checkup():
        """
        Validates the current MDS setup/configuration and takes actions where required
        """
        mds_dict = {}
        for vpool in VPoolList.get_vpools():
            for mds_service in vpool.mds_services:
                storagerouter = mds_service.service.storagerouter
                if vpool not in mds_dict:
                    mds_dict[vpool] = {}
                if storagerouter not in mds_dict[vpool]:
                    mds_dict[vpool][storagerouter] = {'client': SSHClient(storagerouter, username='root'),
                                                      'services': []}
                mds_dict[vpool][storagerouter]['services'].append(mds_service)
        for vpool, storagerouter_info in mds_dict.iteritems():
            # 1. First, make sure there's at least one MDS on every StorageRouter that's not overloaded
            # If not, create an extra MDS for that StorageRouter
            for storagerouter in storagerouter_info:
                client = mds_dict[vpool][storagerouter]['client']
                mds_services = mds_dict[vpool][storagerouter]['services']
                has_room = False
                for mds_service in mds_services[:]:
                    if mds_service.capacity == 0 and len(mds_service.vdisks_guids) == 0:
                        client = SSHClient(storagerouter)
                        MDSServiceController.remove_mds_service(mds_service, client, storagerouter, vpool, reload_config=True)
                        mds_services.remove(mds_service)
                for mds_service in mds_services:
                    _, load = MDSServiceController.get_mds_load(mds_service)
                    if load < Configuration.get('ovs.storagedriver.mds.maxload'):
                        has_room = True
                        break
                if has_room is False:
                    mds_service = MDSServiceController.prepare_mds_service(client, storagerouter, vpool,
                                                                           fresh_only=False, reload_config=True)
                    if mds_service is None:
                        raise RuntimeError('Could not add MDS node')
                    mds_services.append(mds_service)
            mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool)
            for storagerouter in mds_dict[vpool]:
                client = mds_dict[vpool][storagerouter]['client']
                storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.name)
                storagedriver_config.load(client)
                if storagedriver_config.is_new is False:
                    storagedriver_config.clean()  # Clean out obsolete values
                    storagedriver_config.configure_filesystem(
                        fs_metadata_backend_mds_nodes=mds_config_set[storagerouter.guid]
                    )
                    storagedriver_config.save(client)
            # 2. Per VPool, execute a safety check, making sure the master/slave configuration is optimal.
            for vdisk in vpool.vdisks:
                MDSServiceController.ensure_safety(vdisk)


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

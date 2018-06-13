# Copyright (C) 2018 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Shared module. Contains mds-related methods
"""

import collections
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storageserver.storagedriver import MDSMetaDataBackendConfig, MDSNodeConfig, SRCObjectNotFoundException, StorageDriverConfiguration
from ovs.log.log_handler import LogHandler


class MDSShared(object):

    _logger = LogHandler.get('lib', name='mds shared')

    @classmethod
    def _get_mds_load(cls, mds_service):
        """
        Gets a 'load' for an MDS service based on its capacity and the amount of assigned vDisks
        :param mds_service: MDS service the get current load for
        :type mds_service: ovs.dal.hybrids.j_mdsservice.MDSService
        :return: Load of the MDS service
        :rtype: tuple(float, float)
        """
        service_capacity = float(mds_service.capacity)
        if service_capacity < 0:
            return 50.0, 50.0
        if service_capacity == 0:
            return float('inf'), float('inf')
        usage = len(mds_service.vdisks_guids)
        return round(usage / service_capacity * 100.0, 5), round((usage + 1) / service_capacity * 100.0, 5)
    
    @classmethod
    def _sync_vdisk_to_reality(cls, vdisk):
        """
        Syncs the MDS junction services for a vDisk to the services configured in the StorageDriver
        :param vdisk: vDisk to synchronize
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :return: None
        :rtype: NoneType
        """
        cls._logger.info('vDisk {0} - {1}: Syncing to reality'.format(vdisk.guid, vdisk.name))

        sd_master_ip = None  # IP of the master service according to StorageDriver
        sd_master_port = None  # Port of the master service according to StorageDriver
        sd_mds_config = collections.OrderedDict()  # MDS services according to StorageDriver
        model_mds_config = collections.OrderedDict()  # MDS services according to model

        vdisk.reload_client('storagedriver')
        vdisk.invalidate_dynamics(['info', 'storagerouter_guid'])

        # Verify the StorageDriver services
        cls._logger.debug('vDisk {0} - {1}: Current MDS Config: {2}'.format(vdisk.guid, vdisk.name, vdisk.info['metadata_backend_config']))
        for index, mds_entry in enumerate(vdisk.info['metadata_backend_config']):
            ip = mds_entry['ip']
            port = mds_entry['port']
            if index == 0:  # First entry is the master MDS service
                sd_master_ip = ip
                sd_master_port = port
            if ip not in sd_mds_config:
                sd_mds_config[ip] = []
            sd_mds_config[ip].append(port)

        # Verify the model junction services (Relations between the MDS Services and the vDisks)
        for junction in list(vdisk.mds_services):
            model_ip = junction.mds_service.service.storagerouter.ip
            model_port = junction.mds_service.service.ports[0]
            cls._logger.debug('vDisk {0} - {1}: Validating junction service {2}:{3}'.format(vdisk.guid, vdisk.name, model_ip, model_port))

            # Remove duplicate junction services
            if model_ip in model_mds_config and model_port in model_mds_config[model_ip]:
                cls._logger.warning('vDisk {0} - {1}: Deleting junction service {2}:{3} : Duplicate'.format(vdisk.guid, vdisk.name, model_ip, model_port))
                junction.delete()
                continue

            # Remove junction services not known by StorageDriver
            elif model_ip not in sd_mds_config or model_port not in sd_mds_config[model_ip]:
                cls._logger.warning('vDisk {0} - {1}: Deleting junction service {2}:{3} : Unknown by StorageDriver'.format(vdisk.guid, vdisk.name, model_ip, model_port))
                junction.delete()
                continue

            junction.is_master = model_ip == sd_master_ip and model_port == sd_master_port
            junction.save()
            if model_ip not in model_mds_config:
                model_mds_config[model_ip] = []
            model_mds_config[model_ip].append(model_port)

        cls._logger.debug('vDisk {0} - {1}: MDS services according to model: {2}'.format(vdisk.guid, vdisk.name, ', '.join(['{0}:{1}'.format(ip, port) for ip, ports in model_mds_config.iteritems() for port in ports])))
        cls._logger.debug('vDisk {0} - {1}: MDS services according to StorageDriver: {2}'.format(vdisk.guid, vdisk.name, ', '.join(['{0}:{1}'.format(ip, port) for ip, ports in sd_mds_config.iteritems() for port in ports])))
        for ip, ports in sd_mds_config.iteritems():
            for port in ports:
                if ip not in model_mds_config or port not in model_mds_config[ip]:
                    cls._logger.debug('vDisk {0} - {1}: Modeling junction service {2}:{3}'.format(vdisk.guid, vdisk.name, ip, port))
                    service = ServiceList.get_by_ip_ports(ip, [port])
                    if service is None and vdisk.storagerouter_guid is not None:
                        cls._logger.critical('vDisk {0} - {1}: Failed to find an MDS Service for {2}:{3}. Creating a new MDS Service'.format(vdisk.guid, vdisk.name, ip, port))
                        storagerouter = StorageRouter(vdisk.storagerouter_guid)
                        try:
                            service = cls.prepare_mds_service(storagerouter=storagerouter, vpool=vdisk.vpool).service
                        except Exception:
                            cls._logger.exception('vDisk {0} - {1}: Creating MDS Service failed'.format(vdisk.guid, vdisk.name))

                    if service is not None:
                        mds_service_vdisk = MDSServiceVDisk()
                        mds_service_vdisk.vdisk = vdisk
                        mds_service_vdisk.mds_service = service.mds_service
                        mds_service_vdisk.is_master = sd_master_ip == service.storagerouter.ip and sd_master_port == service.ports[0]
                        mds_service_vdisk.save()
                        cls._logger.debug('vDisk {0} - {1}: Modeled junction service {2}:{3}'.format(vdisk.guid, vdisk.name, ip, port))
        cls._logger.info('vDisk {0} - {1}: Synced to reality'.format(vdisk.guid, vdisk.name))

    @classmethod
    def prepare_mds_service(cls, storagerouter, vpool):
        """
        Prepares an MDS service:
            * Creates the required configuration
            * Sets up the service files
        Assumes the StorageRouter and vPool are already configured with a StorageDriver and that all model-wise configurations regarding both have been completed.

        :param storagerouter: StorageRouter on which the MDS service will be created
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param vpool: The vPool for which the MDS service will be created
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :raises RuntimeError: vPool is not extended on StorageRouter
                              No ServiceType found for 'MetadataServer'
                              No free port is found for the new MDSService
                              No partition found on StorageRouter with DB role
        :return: Newly created junction service
        :rtype: ovs.dal.hybrids.j_mdsservice.MDSService
        """
        from ovs.lib.storagedriver import StorageDriverController  # Import here to prevent from circular imports

        cls._logger.info('StorageRouter {0} - vPool {1}: Preparing MDS junction service'.format(storagerouter.name, vpool.name))

        mds_service = MDSService()
        with volatile_mutex(name='prepare_mds_{0}'.format(storagerouter.guid), wait=30):
            # VALIDATIONS
            # Verify passed StorageRouter is part of the vPool
            storagerouter.invalidate_dynamics(['vpools_guids'])
            if vpool.guid not in storagerouter.vpools_guids:
                raise RuntimeError('StorageRouter {0} is not part of vPool {1}'.format(storagerouter.name, vpool.name))

            # Verify ServiceType existence
            mds_service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER)
            if mds_service_type is None:
                raise RuntimeError('No ServiceType found with name {0}'.format(ServiceType.SERVICE_TYPES.MD_SERVER))

            # Retrieve occupied ports for current StorageRouter and max MDSService number for current vPool/StorageRouter combo
            service_number = -1
            occupied_ports = []
            for service in mds_service_type.services:
                if service.storagerouter_guid == storagerouter.guid:
                    occupied_ports.extend(service.ports)
                    if service.mds_service.vpool_guid == vpool.guid:
                        service_number = max(service.mds_service.number, service_number)

            client = SSHClient(endpoint=storagerouter)
            mds_port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|mds'.format(System.get_my_machine_id(client)))
            free_ports = System.get_free_ports(selected_range=mds_port_range,
                                               exclude=occupied_ports,
                                               nr=1,
                                               client=client)
            if len(free_ports) != 1:
                raise RuntimeError('Failed to find an available port on StorageRouter {0} within range {1}'.format(storagerouter.name, mds_port_range))

            # Partition check
            db_partition = None
            for disk in storagerouter.disks:
                for partition in disk.partitions:
                    if DiskPartition.ROLES.DB in partition.roles:
                        db_partition = partition
                        break
            if db_partition is None:
                raise RuntimeError('Could not find DB partition on StorageRouter {0}'.format(storagerouter.name))

            # Verify StorageDriver configured
            storagedrivers = [sd for sd in vpool.storagedrivers if sd.storagerouter_guid == storagerouter.guid]
            if len(storagedrivers) != 1:
                raise RuntimeError('Expected to find a configured StorageDriver for vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))

            # MODEL UPDATES
            # Service and MDS service
            service_number += 1
            cls._logger.info('StorageRouter {0} - vPool {1}: Adding junction service with number {2}'.format(storagerouter.name, vpool.name, service_number))

            service = Service()
            service.name = 'metadataserver_{0}_{1}'.format(vpool.name, service_number)
            service.type = mds_service_type
            service.ports = free_ports
            service.storagerouter = storagerouter
            service.save()
            mds_service.vpool = vpool
            mds_service.number = service_number
            mds_service.service = service
            mds_service.save()

            # StorageDriver partitions
            cls._logger.info('StorageRouter {0} - vPool {1}: Adding StorageDriverPartition on partition with mount point {2}'.format(storagerouter.name, vpool.name, db_partition.mountpoint))
            storagedriver = storagedrivers[0]
            sdp = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                     'role': DiskPartition.ROLES.DB,
                                                                                     'sub_role': StorageDriverPartition.SUBROLE.MDS,
                                                                                     'partition': db_partition,
                                                                                     'mds_service': mds_service})

            # CONFIGURATIONS
            # Volumedriver
            mds_nodes = []
            for sd_partition in storagedriver.partitions:
                if sd_partition.role == DiskPartition.ROLES.DB and sd_partition.sub_role == StorageDriverPartition.SUBROLE.MDS and sd_partition.mds_service is not None:
                    service = sd_partition.mds_service.service
                    mds_nodes.append({'host': service.storagerouter.ip,
                                      'port': service.ports[0],
                                      'db_directory': '{0}/db'.format(sd_partition.path),
                                      'scratch_directory': '{0}/scratch'.format(sd_partition.path)})

            cls._logger.info('StorageRouter {0} - vPool {1}: Configuring StorageDriver with MDS nodes: {2}'.format(storagerouter.name, vpool.name, mds_nodes))
            # Generate the correct section in the StorageDriver's configuration
            try:
                storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
                storagedriver_config.load()
                storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
                storagedriver_config.save(client)
            except Exception:
                cls._logger.exception('StorageRouter {0} - vPool {1}: Configuring StorageDriver failed. Reverting model changes'.format(storagerouter.name, vpool.name))
                # Clean up model changes if error occurs
                sdp.delete()
                mds_service.delete()  # Must be removed before the service
                service.delete()
        return mds_service

    @staticmethod
    def map_mds_services_by_socket(vdisk):
        """
        Maps the mds services related to the vpool by their socket
        :param vdisk: VDisk object to
        :return: A dict wth sockets as key, service as value
        :rtype: Dict[str, ovs.dal.hybrids.j_mdsservice.MDSService
        """
        # Sorted was added merely for unittests, because they rely on specific order of services and their ports
        # Default sorting behavior for relations used to be based on order in which relations were added
        # Now sorting is based on guid (DAL speedup changes)
        service_per_key = collections.OrderedDict()  # OrderedDict to keep the ordering in the dict
        for service in sorted([mds.service for mds in vdisk.vpool.mds_services], key=lambda k: k.ports):
            service_per_key['{0}:{1}'.format(service.storagerouter.ip, service.ports[0])] = service
        return service_per_key

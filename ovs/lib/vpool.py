# Copyright (C) 2016 iNuron NV
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
VPoolController class responsible for making changes to existing vPools
"""

import json
import time
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.api.client import OVSClient
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import LocalStorageRouterClient, StorageDriverConfiguration, is_connection_failure
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import log, ovs_task
from ovs.lib.helpers.vpool.shared import VPoolShared
from ovs.lib.helpers.vpool.installer import VPoolInstaller
from ovs.lib.helpers.storagerouter.installer import StorageRouterInstaller
from ovs.lib.helpers.storagedriver.installer import StorageDriverInstaller
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.vdisk import VDiskController


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """
    _logger = Logger('lib')
    _service_manager = ServiceFactory.get_manager()

    @classmethod
    @ovs_task(name='ovs.storagerouter.add_vpool')
    def add_vpool(cls, parameters):
        """
        Add a vPool to the machine this task is running on
        :param parameters: Parameters for vPool creation
        :type parameters: dict
        :return: None
        :rtype: NoneType
        """
        # TODO: Add logging
        cls._logger.debug('Adding vpool. Parameters: {}'.format(parameters))
        # VALIDATIONS
        if not isinstance(parameters, dict):
            raise ValueError('Parameters passed to create a vPool should be of type dict')

        # Check StorageRouter existence
        storagerouter = StorageRouterList.get_by_ip(ip=parameters.get('storagerouter_ip'))
        if storagerouter is None:
            raise RuntimeError('Could not find StorageRouter')

        # Validate requested vPool configurations
        vp_installer = VPoolInstaller(name=parameters.get('vpool_name'))
        vp_installer.validate(storagerouter=storagerouter)

        # Validate requested StorageDriver configurations
        cls._logger.info('vPool {0}: Validating StorageDriver configurations'.format(vp_installer.name))
        sd_installer = StorageDriverInstaller(vp_installer=vp_installer,
                                              configurations={'storage_ip': parameters.get('storage_ip'),
                                                              'caching_info': parameters.get('caching_info'),
                                                              'backend_info': {'main': parameters.get('backend_info'),
                                                                               StorageDriverConfiguration.CACHE_BLOCK: parameters.get('backend_info_bc'),
                                                                               StorageDriverConfiguration.CACHE_FRAGMENT: parameters.get('backend_info_fc')},
                                                              'connection_info': {'main': parameters.get('connection_info'),
                                                                                  StorageDriverConfiguration.CACHE_BLOCK: parameters.get('connection_info_bc'),
                                                                                  StorageDriverConfiguration.CACHE_FRAGMENT: parameters.get('connection_info_fc')},
                                                              'sd_configuration': parameters.get('config_params')})

        partitions_mutex = volatile_mutex('add_vpool_partitions_{0}'.format(storagerouter.guid))
        try:
            # VPOOL CREATION
            # Create the vPool as soon as possible in the process to be displayed in the GUI (INSTALLING/EXTENDING state)
            if vp_installer.is_new is True:
                vp_installer.create(rdma_enabled=sd_installer.rdma_enabled)
                vp_installer.configure_mds(config=parameters.get('mds_config_params', {}))
            else:
                vp_installer.update_status(status=VPool.STATUSES.EXTENDING)

            # ADDITIONAL VALIDATIONS
            # Check StorageRouter connectivity
            cls._logger.info('vPool {0}: Validating StorageRouter connectivity'.format(vp_installer.name))
            linked_storagerouters = [storagerouter]
            if vp_installer.is_new is False:
                linked_storagerouters += [sd.storagerouter for sd in vp_installer.vpool.storagedrivers]

            sr_client_map = SSHClient.get_clients(endpoints=linked_storagerouters, user_names=['ovs', 'root'])
            offline_nodes = sr_client_map.pop('offline')
            if storagerouter in offline_nodes:
                raise RuntimeError('Node on which the vPool is being {0} is not reachable'.format('created' if vp_installer.is_new is True else 'extended'))

            sr_installer = StorageRouterInstaller(root_client=sr_client_map[storagerouter]['root'],
                                                  sd_installer=sd_installer,
                                                  vp_installer=vp_installer,
                                                  storagerouter=storagerouter)

            # When 2 or more jobs simultaneously run on the same StorageRouter, we need to check and create the StorageDriver partitions in locked context
            partitions_mutex.acquire(wait=60)
            sr_installer.partition_info = StorageRouterController.get_partition_info(storagerouter_guid=storagerouter.guid)
            sr_installer.validate_vpool_extendable()
            sr_installer.validate_global_write_buffer(requested_size=parameters.get('writecache_size', 0))
            sr_installer.validate_local_cache_size(requested_proxies=parameters.get('parallelism', {}).get('proxies', 2))

            # MODEL STORAGEDRIVER AND PARTITION JUNCTIONS
            sd_installer.create()
            sd_installer.create_partitions()
            partitions_mutex.release()

            vp_installer.refresh_metadata()
        except Exception:
            cls._logger.exception('Something went wrong during the validation or modeling of vPool {0} on StorageRouter {1}'.format(vp_installer.name, storagerouter.name))
            partitions_mutex.release()
            vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            raise

        # Arakoon setup
        counter = 0
        while counter < 300:
            try:
                if StorageDriverController.manual_voldrv_arakoon_checkup() is True:
                    break
            except Exception:
                cls._logger.exception('Arakoon checkup for voldrv cluster failed')
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
                raise
            counter += 1
            time.sleep(1)
            if counter == 300:
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
                raise RuntimeError('Arakoon checkup for the StorageDriver cluster could not be started')

        # Cluster registry
        try:
            vp_installer.configure_cluster_registry(allow_raise=True)
        except Exception:
            if vp_installer.is_new is True:
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            else:
                vp_installer.revert_vpool(status=VPool.STATUSES.FAILURE)
            raise

        try:
            sd_installer.setup_proxy_configs()
            sd_installer.configure_storagedriver_service()
            DiskController.sync_with_reality(storagerouter.guid)
            MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vp_installer.vpool)

            # Update the MDS safety if changed via API (vpool.configuration will be available at this point also for the newly added StorageDriver)
            vp_installer.vpool.invalidate_dynamics('configuration')
            if vp_installer.mds_safety is not None and vp_installer.vpool.configuration['mds_config']['mds_safety'] != vp_installer.mds_safety:
                Configuration.set(key='/ovs/vpools/{0}/mds_config|mds_safety'.format(vp_installer.vpool.guid), value=vp_installer.mds_safety)

            sd_installer.start_services()  # Create and start watcher volumedriver, DTL, proxies and StorageDriver services

            # Post creation/extension checkups
            mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vp_installer.vpool, offline_nodes=offline_nodes)
            for sr, clients in sr_client_map.iteritems():
                for current_storagedriver in [sd for sd in sr.storagedrivers if sd.vpool_guid == vp_installer.vpool.guid]:
                    storagedriver_config = StorageDriverConfiguration(vpool_guid=vp_installer.vpool.guid, storagedriver_id=current_storagedriver.storagedriver_id)
                    if storagedriver_config.config_missing is False:
                        # Filesystem section in StorageDriver configuration are all parameters used for vDisks created directly on the filesystem
                        # So when a vDisk gets created on the filesystem, these MDSes will be assigned to them
                        storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                        storagedriver_config.save(client=clients['ovs'])

            # Everything's reconfigured, refresh new cluster configuration
            for current_storagedriver in vp_installer.vpool.storagedrivers:
                if current_storagedriver.storagerouter not in sr_client_map:
                    continue
                vp_installer.vpool.storagedriver_client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id), req_timeout_secs=10)
        except Exception:
            cls._logger.exception('vPool {0}: Creation failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # When a node is offline, we can run into errors, but also when 1 or more volumes are not running
        # Scheduled tasks below, so don't really care whether they succeed or not
        try:
            VDiskController.dtl_checkup(vpool_guid=vp_installer.vpool.guid, ensure_single_timeout=600)
        except:
            pass
        for vdisk in vp_installer.vpool.vdisks:
            try:
                MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
            except:
                pass
        vp_installer.update_status(status=VPool.STATUSES.RUNNING)
        cls._logger.info('Add vPool {0} ended successfully'.format(vp_installer.name))

    # @classmethod
    # @ovs_task(name='ovs.vpool.extend_vpool')
    # def extend_vpool(cls, ...):
    # TODO: Make sure extend and create cannot be executed at the same time for the same vPool

    @classmethod
    @ovs_task(name='ovs.vpool.shrink_vpool')
    def shrink_vpool(cls, storagedriver_guid, offline_storage_router_guids=None):
        """
        Removes a StorageDriver (if its the last StorageDriver for a vPool, the vPool is removed as well)
        :param storagedriver_guid: Guid of the StorageDriver to remove
        :type storagedriver_guid: str
        :param offline_storage_router_guids: Guids of StorageRouters which are offline and will be removed from cluster.
                                             WHETHER VPOOL WILL BE DELETED DEPENDS ON THIS
        :type offline_storage_router_guids: list
        :return: None
        :rtype: NoneType
        """
        # TODO: Add logging
        # TODO: Unit test individual pieces of code
        # Validations
        if offline_storage_router_guids is None:
            offline_storage_router_guids = []
        storagedriver = StorageDriver(storagedriver_guid)
        storagerouter = storagedriver.storagerouter
        cls._logger.info('StorageDriver {0} - Deleting StorageDriver {1}'.format(storagedriver.guid, storagedriver.name))

        vp_installer = VPoolInstaller(name=storagedriver.vpool.name)
        vp_installer.validate(storagedriver=storagedriver)

        sd_installer = StorageDriverInstaller(vp_installer=vp_installer,
                                              storagedriver=storagedriver)

        cls._logger.info('StorageDriver {0} - Checking availability of related StorageRouters'.format(storagedriver.guid, storagedriver.name))
        sr_client_map = SSHClient.get_clients(endpoints=[sd.storagerouter for sd in vp_installer.vpool.storagedrivers], user_names=['root'])
        sr_installer = StorageRouterInstaller(root_client=sr_client_map.get(storagerouter, {}).get('root'),
                                              storagerouter=storagerouter,
                                              vp_installer=vp_installer,
                                              sd_installer=sd_installer)

        offline_srs = sr_client_map.pop('offline')
        if sorted([sr.guid for sr in offline_srs]) != sorted(offline_storage_router_guids):
            raise RuntimeError('Not all StorageRouters are reachable')

        if storagerouter not in offline_srs:
            mtpt_pids = sr_installer.root_client.run("lsof -t +D '/mnt/{0}' || true".format(vp_installer.name.replace(r"'", r"'\''")), allow_insecure=True).splitlines()
            if len(mtpt_pids) > 0:
                raise RuntimeError('vPool cannot be deleted. Following processes keep the vPool mount point occupied: {0}'.format(', '.join(mtpt_pids)))

        # Retrieve reachable StorageDrivers
        reachable_storagedrivers = []
        for sd in vp_installer.vpool.storagedrivers:
            if sd.storagerouter not in sr_client_map:
                # StorageRouter is offline
                continue

            sd_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vp_installer.vpool.guid, sd.storagedriver_id)
            if Configuration.exists(sd_key) is True:
                path = Configuration.get_configuration_path(sd_key)
                with remote(sd.storagerouter.ip, [LocalStorageRouterClient]) as rem:
                    try:
                        lsrc = rem.LocalStorageRouterClient(path)
                        lsrc.server_revision()  # 'Cheap' call to verify whether volumedriver is responsive
                        cls._logger.info('StorageDriver {0} - Responsive StorageDriver {1} on node with IP {2}'.format(storagedriver.guid, sd.name, sd.storagerouter.ip))
                        reachable_storagedrivers.append(sd)
                    except Exception as exception:
                        if not is_connection_failure(exception):
                            raise

        if len(reachable_storagedrivers) == 0:
            raise RuntimeError('Could not find any responsive node in the cluster')

        # Start removal
        if vp_installer.storagedriver_amount > 1:
            vp_installer.update_status(status=VPool.STATUSES.SHRINKING)
        else:
            vp_installer.update_status(status=VPool.STATUSES.DELETING)

        # Clean up stale vDisks
        cls._logger.info('StorageDriver {0} - Removing stale vDisks'.format(storagedriver.guid))
        VDiskController.remove_stale_vdisks(vpool=vp_installer.vpool)

        # Un-configure or reconfigure the MDSes
        cls._logger.info('StorageDriver {0} - Reconfiguring MDSes'.format(storagedriver.guid))
        vdisks = set()
        mds_services_to_remove = [mds_service for mds_service in storagedriver.vpool.mds_services if mds_service.service.storagerouter_guid == storagerouter.guid]
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.add(vdisk)
                vdisk.invalidate_dynamics(['storagedriver_id'])
                if vdisk.storagedriver_id:
                    try:
                        StorageRouterController._logger.debug('StorageDriver {0} - vDisk {1} - Ensuring MDS safety'.format(storagedriver.guid, vdisk.guid))
                        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid,
                                                           excluded_storagerouter_guids=[storagerouter.guid] + offline_storage_router_guids)
                    except Exception:
                        cls._logger.exception('StorageDriver {0} - vDisk {1} - Ensuring MDS safety failed'.format(storagedriver.guid, vdisk.guid))

        # Validate that all MDSes on current StorageRouter have been moved away
        # Ensure safety does not always throw an error, that's why we perform this check here instead of in the Exception clause of above code
        vdisks = []
        for mds in vp_installer.mds_services:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                cls._logger.critical('StorageDriver {0} - vDisk {1} {2} - MDS Services have not been migrated away'.format(storagedriver.guid, vdisk.guid, vdisk.name))
        if len(vdisks) > 0:
            # Put back in RUNNING, so it can be used again. Errors keep on displaying in GUI now anyway
            vp_installer.update_status(status=VPool.STATUSES.RUNNING)
            raise RuntimeError('Not all MDS Services have been successfully migrated away')

        # Start with actual removal
        errors_found = False
        if storagerouter not in offline_srs:
            errors_found &= sd_installer.stop_services()

        errors_found &= vp_installer.configure_cluster_registry(exclude=[storagedriver], apply_on=reachable_storagedrivers)
        errors_found &= vp_installer.update_node_distance_map()
        errors_found &= vp_installer.remove_mds_services()
        errors_found &= sd_installer.clean_config_management()
        errors_found &= sd_installer.clean_model()

        if storagerouter not in offline_srs:
            errors_found &= sd_installer.clean_directories(mountpoints=StorageRouterController.get_mountpoints(client=sr_installer.root_client))

            try:
                DiskController.sync_with_reality(storagerouter_guid=storagerouter.guid)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Synchronizing disks with reality failed'.format(storagedriver.guid))
                errors_found = True

        if vp_installer.storagedriver_amount > 1:
            # Update the vPool metadata and run DTL checkup
            vp_installer.vpool.metadata['caching_info'].pop(sr_installer.storagerouter.guid, None)
            vp_installer.vpool.save()

            try:
                VDiskController.dtl_checkup(vpool_guid=vp_installer.vpool.guid, ensure_single_timeout=600)
            except Exception:
                cls._logger.exception('StorageDriver {0} - DTL checkup failed for vPool {1} with guid {2}'.format(storagedriver.guid, vp_installer.name, vp_installer.vpool.guid))
        else:
            cls._logger.info('StorageDriver {0} - Removing vPool from model'.format(storagedriver.guid))
            # Clean up model
            try:
                vp_installer.vpool.delete()
            except Exception:
                errors_found = True
                cls._logger.exception('StorageDriver {0} - Cleaning up vPool from the model failed'.format(storagedriver.guid))
            Configuration.delete('/ovs/vpools/{0}'.format(vp_installer.vpool.guid))

        cls._logger.info('StorageDriver {0} - Running MDS checkup'.format(storagedriver.guid))
        try:
            MDSServiceController.mds_checkup()
        except Exception:
            cls._logger.exception('StorageDriver {0} - MDS checkup failed'.format(storagedriver.guid))

        # Update vPool status
        if errors_found is True:
            if vp_installer.storagedriver_amount > 1:
                vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise RuntimeError('1 or more errors occurred while trying to remove the StorageDriver. Please check the logs for more information')

        if vp_installer.storagedriver_amount > 1:
            vp_installer.update_status(status=VPool.STATUSES.RUNNING)
        cls._logger.info('StorageDriver {0} - Deleted StorageDriver {1}'.format(storagedriver.guid, storagedriver.name))

        if len(VPoolList.get_vpools()) == 0:
            cluster_name = ArakoonInstaller.get_cluster_name('voldrv')
            if ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)['internal'] is True:
                cls._logger.debug('StorageDriver {0} - Removing Arakoon cluster {1}'.format(storagedriver.guid, cluster_name))
                try:
                    installer = ArakoonInstaller(cluster_name=cluster_name)
                    installer.load()
                    installer.delete_cluster()
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Delete voldrv Arakoon cluster failed'.format(storagedriver.guid))
                service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
                service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
                for service in list(service_type.services):
                    if service.name == service_name:
                        service.delete()

        # Remove watcher volumedriver service if last StorageDriver on current StorageRouter
        if len(storagerouter.storagedrivers) == 0 and storagerouter not in offline_srs:  # ensure client is initialized for StorageRouter
            try:
                if cls._service_manager.has_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=sr_installer.root_client):
                    cls._service_manager.stop_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=sr_installer.root_client)
                    cls._service_manager.remove_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=sr_installer.root_client)
            except Exception:
                cls._logger.exception('StorageDriver {0} - {1} service deletion failed'.format(storagedriver.guid, ServiceFactory.SERVICE_WATCHER_VOLDRV))

    @staticmethod
    @ovs_task(name='ovs.vpool.up_and_running')
    @log('VOLUMEDRIVER_TASK')
    def up_and_running(storagedriver_id):
        """
        Volumedriver informs us that the service is completely started. Post-start events can be executed
        :param storagedriver_id: ID of the storagedriver
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()

    # noinspection PyTypeChecker
    @staticmethod
    @ovs_task(name='ovs.storagerouter.create_hprm_config_files')
    def create_hprm_config_files(vpool_guid, local_storagerouter_guid, parameters):
        """
        Create the required configuration files to be able to make use of HPRM (aka PRACC)
        This configuration will be zipped and made available for download
        :param vpool_guid: The guid of the VPool for which a HPRM manager needs to be deployed
        :type vpool_guid: str
        :param local_storagerouter_guid: The guid of the StorageRouter the API was requested on
        :type local_storagerouter_guid: str
        :param parameters: Additional information required for the HPRM configuration files
        :type parameters: dict
        :return: Name of the zipfile containing the configuration files
        :rtype: str
        """
        # Validations
        required_params = {'port': (int, {'min': 1, 'max': 65535}),
                           'identifier': (str, ExtensionsToolbox.regex_vpool)}
        ExtensionsToolbox.verify_required_params(actual_params=parameters,
                                                 required_params=required_params)
        vpool = VPool(vpool_guid)
        identifier = parameters['identifier']
        config_path = None
        local_storagerouter = StorageRouter(local_storagerouter_guid)
        for sd in vpool.storagedrivers:
            if len(sd.alba_proxies) == 0:
                raise ValueError('No ALBA proxies configured for vPool {0} on StorageRouter {1}'.format(vpool.name,
                                                                                                        sd.storagerouter.name))
            config_path = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, sd.alba_proxies[0].guid)

        if config_path is None:
            raise ValueError('vPool {0} has not been extended any StorageRouter'.format(vpool.name))
        proxy_cfg = Configuration.get(key=config_path.format('main'))

        cache_info = {}
        arakoons = {}
        cache_types = VPool.CACHES.values()
        if not any(ctype in parameters for ctype in cache_types):
            raise ValueError('At least one cache type should be passed: {0}'.format(', '.join(cache_types)))
        for ctype in cache_types:
            if ctype not in parameters:
                continue
            required_dict = {'read': (bool, None),
                             'write': (bool, None)}
            required_params.update({ctype: (dict, required_dict)})
            ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params=required_params)
            read = parameters[ctype]['read']
            write = parameters[ctype]['write']
            if read is False and write is False:
                cache_info[ctype] = ['none']
                continue
            path = parameters[ctype].get('path')
            if path is not None:
                path = path.strip()
                if not path or path.endswith('/.') or '..' in path or '/./' in path:
                    raise ValueError('Invalid path specified')
                required_dict.update({'path': (str, None),
                                      'size': (int, {'min': 1, 'max': 10 * 1024})})
                ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params=required_params)
                while '//' in path:
                    path = path.replace('//', '/')
                cache_info[ctype] = ['local', {'path': path,
                                               'max_size': parameters[ctype]['size'] * 1024 ** 3,
                                               'cache_on_read': read,
                                               'cache_on_write': write}]
            else:
                required_dict.update({'backend_info': (dict, {'preset': (str, ExtensionsToolbox.regex_preset),
                                                              'alba_backend_guid': (str, ExtensionsToolbox.regex_guid),
                                                              'alba_backend_name': (str, ExtensionsToolbox.regex_backend)}),
                                      'connection_info': (dict, {'host': (str, ExtensionsToolbox.regex_ip, False),
                                                                 'port': (int, {'min': 1, 'max': 65535}, False),
                                                                 'client_id': (str, ExtensionsToolbox.regex_guid, False),
                                                                 'client_secret': (str, None, False)})})
                ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params=required_params)
                connection_info = parameters[ctype]['connection_info']
                if connection_info['host']:  # Remote Backend for accelerated Backend
                    alba_backend_guid = parameters[ctype]['backend_info']['alba_backend_guid']
                    ovs_client = OVSClient.get_instance(connection_info=connection_info)
                    arakoon_config = VPoolShared.retrieve_alba_arakoon_config(alba_backend_guid=alba_backend_guid, ovs_client=ovs_client)
                    arakoons[ctype] = ArakoonClusterConfig.convert_config_to(arakoon_config, return_type='INI')
                else:  # Local Backend for accelerated Backend
                    alba_backend_name = parameters[ctype]['backend_info']['alba_backend_name']
                    if Configuration.exists(key='/ovs/arakoon/{0}-abm/config'.format(alba_backend_name),
                                            raw=True) is False:
                        raise ValueError('Arakoon cluster for ALBA Backend {0} could not be retrieved'.format(alba_backend_name))
                    arakoons[ctype] = Configuration.get(key='/ovs/arakoon/{0}-abm/config'.format(alba_backend_name),
                                                        raw=True)
                cache_info[ctype] = ['alba', {'albamgr_cfg_url': '/etc/hprm/{0}/{1}_cache_arakoon.ini'.format(identifier, ctype),
                                              'bucket_strategy': ['1-to-1', {'prefix': vpool.guid,
                                                                             'preset': parameters[ctype]['backend_info']['preset']}],
                                              'manifest_cache_size': proxy_cfg['manifest_cache_size'],
                                              'cache_on_read': read,
                                              'cache_on_write': write}]

        tgz_name = 'hprm_config_files_{0}_{1}.tgz'.format(identifier, vpool.name)
        config = {'ips': ['127.0.0.1'],
                  'port': parameters['port'],
                  'pracc': {'uds_path': '/var/run/hprm/{0}/uds_path'.format(identifier),
                            'max_clients': 1000,
                            'max_read_buf_size': 64 * 1024,  # Buffer size for incoming requests (in bytes)
                            'thread_pool_size': 64},  # Amount of threads
                  'transport': 'tcp',
                  'log_level': 'info',
                  'read_preference': proxy_cfg['read_preference'],
                  'albamgr_cfg_url': '/etc/hprm/{0}/arakoon.ini'.format(identifier),
                  'manifest_cache_size': proxy_cfg['manifest_cache_size']}
        file_contents_map = {}
        for ctype in cache_types:
            if ctype in cache_info:
                config['{0}_cache'.format(ctype)] = cache_info[ctype]
            if ctype in arakoons:
                file_contents_map['/opt/OpenvStorage/config/{0}/{1}_cache_arakoon.ini'.format(identifier, ctype)] = arakoons[ctype]
        file_contents_map.update({'/opt/OpenvStorage/config/{0}/config.json'.format(identifier): json.dumps(config, indent=4),
                                  '/opt/OpenvStorage/config/{0}/arakoon.ini'.format(identifier): Configuration.get(key=config_path.format('abm'), raw=True)})

        local_client = SSHClient(endpoint=local_storagerouter)
        local_client.dir_create(directories='/opt/OpenvStorage/config/{0}'.format(identifier))
        local_client.dir_create(directories='/opt/OpenvStorage/webapps/frontend/downloads')
        for file_name, contents in file_contents_map.iteritems():
            local_client.file_write(contents=contents, filename=file_name)
        local_client.run(command=['tar', '--transform', 's#^config/{0}#{0}#'.format(identifier),
                                  '-czf', '/opt/OpenvStorage/webapps/frontend/downloads/{0}'.format(tgz_name),
                                  'config/{0}'.format(identifier)])
        local_client.dir_delete(directories='/opt/OpenvStorage/config/{0}'.format(identifier))
        return tgz_name

    @staticmethod
    def retrieve_alba_arakoon_config(alba_backend_guid, ovs_client):
        """
        Retrieve the ALBA Arakoon configuration
        WARNING: YOU DO NOT BELONG HERE, PLEASE MOVE TO YOUR OWN PLUGIN
        :param alba_backend_guid: Guid of the ALBA Backend
        :type alba_backend_guid: str
        :param ovs_client: OVS client object
        :type ovs_client: OVSClient
        :return: Arakoon configuration information
        :rtype: dict
        """
        task_id = ovs_client.get('/alba/backends/{0}/get_config_metadata'.format(alba_backend_guid))
        successful, arakoon_config = ovs_client.wait_for_task(task_id, timeout=300)
        if successful is False:
            raise RuntimeError('Could not load metadata from environment {0}'.format(ovs_client.ip))
        return arakoon_config

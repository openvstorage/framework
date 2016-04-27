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
StorageRouter module
"""
import os
import json
import time
import uuid
import random
from ConfigParser import RawConfigParser
from subprocess import check_output, CalledProcessError
from StringIO import StringIO
from ovs.celery_run import celery
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_albaproxy import AlbaProxy
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service as DalService
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.lists.clientlist import ClientList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.api.client import OVSClient
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.disk import DiskTools
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration, StorageDriverClient
from ovs.extensions.support.agent import SupportAgent
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import add_hooks, ensure_single
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.vdisk import VDiskController
from ovs.lib.vpool import VPoolController
from ovs.log.logHandler import LogHandler
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter.storagerouterclient import ArakoonNodeConfig
from volumedriver.storagerouter.storagerouterclient import ClusterNodeConfig
from volumedriver.storagerouter.storagerouterclient import ClusterRegistry
from volumedriver.storagerouter.storagerouterclient import LocalStorageRouterClient

logger = LogHandler.get('lib', name='storagerouter')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
# noinspection PyArgumentList
storagerouterclient.Logger.enableLogging()


class StorageRouterController(object):
    """
    Contains all BLL related to StorageRouter
    """
    SUPPORT_AGENT = 'support-agent'
    PARTITION_DEFAULT_USAGES = {DiskPartition.ROLES.DB: (40, 20),  # 1st number is exact size in GiB, 2nd number is percentage (highest of the 2 will be taken)
                                DiskPartition.ROLES.SCRUB: (0, 0)}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_metadata')
    def get_metadata(storagerouter_guid):
        """
        Gets physical information about the machine this task is running on
        :param storagerouter_guid: Storage Router guid to retrieve the metadata for
        :type storagerouter_guid: str

        :return: Metadata information about the Storage Router
        :rtype: dict
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter)
        if storagerouter.pmachine.hvtype == 'KVM':
            ipaddresses = ['127.0.0.1']
        else:
            ipaddresses = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().splitlines()
            ipaddresses = [ipaddr.strip() for ipaddr in ipaddresses]
            ipaddresses.remove('127.0.0.1')

        mountpoints = StorageRouterController._get_mountpoints(client)
        partitions = dict((role, []) for role in DiskPartition.ROLES)
        shared_size = 0
        readcache_size = 0
        writecache_size = 0

        for disk in storagerouter.disks:
            for disk_partition in disk.partitions:
                claimed_space = 0
                used_space_by_roles = 0
                for storagedriver_partition in disk_partition.storagedrivers:
                    claimed_space += storagedriver_partition.size if storagedriver_partition.size is not None else 0
                    directory_used_size = 0
                    if client.dir_exists(storagedriver_partition.path):
                        try:
                            used_size, _ = client.run('du -B 1 -d 0 {0}'.format(storagedriver_partition.path)).split('\t')
                            directory_used_size = int(used_size)
                        except Exception as ex:
                            logger.warning('Failed to get directory usage for {0}. {1}'.format(storagedriver_partition.path, ex))
                    used_space_by_roles += directory_used_size

                partition_available_space = None
                if disk_partition.mountpoint is not None:
                    disk_partition_device = client.file_read_link(path=disk_partition.path)
                    try:
                        available = client.run('df -B 1 --output=avail {0}'.format(disk_partition_device)).splitlines()[-1]
                        partition_available_space = int(available)
                    except Exception as ex:
                        logger.warning('Failed to get partition usage for {0}. {1}'.format(disk_partition.mountpoint, ex))

                shared = False
                for role in disk_partition.roles:
                    size = disk_partition.size if disk_partition.size is not None else 0
                    if partition_available_space is not None:
                        # Take available space reported by df then add back used by roles so that the only used space reported is space not managed by us
                        # then we'll subtract the roles reserved size
                        available = partition_available_space + used_space_by_roles - claimed_space
                    else:
                        available = size - claimed_space  # Subtract size for roles which have already been claimed by other vpools (but not necessarily already been fully used)
                    # Subtract size for competing roles on the same partition
                    for sub_role, required_size in StorageRouterController.PARTITION_DEFAULT_USAGES.iteritems():
                        if sub_role in disk_partition.roles and sub_role != role:
                            amount = required_size[0] * 1024 ** 3
                            percentage = required_size[1] * disk_partition.size / 100
                            available -= max(amount, percentage)

                    if available > 0:
                        if (role == DiskPartition.ROLES.READ or role == DiskPartition.ROLES.WRITE) and DiskPartition.ROLES.READ in disk_partition.roles and DiskPartition.ROLES.WRITE in disk_partition.roles and shared is False:
                            shared_size += available
                            shared = True
                        elif role == DiskPartition.ROLES.READ and shared is False:
                            readcache_size += available
                        elif role == DiskPartition.ROLES.WRITE and shared is False:
                            writecache_size += available
                    else:
                        available = 0
                    partitions[role].append({'ssd': disk.is_ssd,
                                             'guid': disk_partition.guid,
                                             'size': size or 0,
                                             'in_use': any(junction for junction in disk_partition.storagedrivers
                                                           if junction.role == role),
                                             'available': available,
                                             'mountpoint': disk_partition.folder,  # Equals to mountpoint unless mountpoint is root ('/'), then we pre-pend mountpoint with '/mnt/storage'
                                             'storagerouter_guid': disk_partition.disk.storagerouter_guid})

        for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services:
            if service.name == 'arakoon-ovsdb':
                continue
            for partition in partitions[DiskPartition.ROLES.DB]:
                if service.storagerouter_guid == partition['storagerouter_guid']:
                    partition['in_use'] = True
        for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER).services:
            for partition in partitions[DiskPartition.ROLES.DB]:
                if service.storagerouter_guid == partition['storagerouter_guid']:
                    partition['in_use'] = True

        return {'partitions': partitions,
                'mountpoints': mountpoints,
                'ipaddresses': ipaddresses,
                'shared_size': shared_size,
                'readcache_size': readcache_size,
                'writecache_size': writecache_size,
                'scrub_available': StorageRouterController._check_scrub_partition_present()}

    @staticmethod
    @celery.task(name='ovs.storagerouter.add_vpool')
    def add_vpool(parameters):
        """
        Add a vPool to the machine this task is running on
        :param parameters: Parameters for vPool creation
        :type parameters: dict

        :return: None
        """
        sd_config_params = (dict, {'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                   'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                   'dedupe_mode': (str, StorageDriverClient.VPOOL_DEDUPE_MAP.keys()),
                                   'cluster_size': (int, StorageDriverClient.CLUSTER_SIZES),
                                   'write_buffer': (int, {'min': 128, 'max': 10240}),
                                   'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys()),
                                   'cache_strategy': (str, StorageDriverClient.VPOOL_CACHE_MAP.keys())})
        required_params = {'type': (str, ['local', 'distributed', 'alba', 'ceph_s3', 'amazon_s3', 'swift_s3']),
                           'vpool_name': (str, Toolbox.regex_vpool),
                           'storage_ip': (str, Toolbox.regex_ip),
                           'storagerouter_ip': (str, Toolbox.regex_ip),
                           'integratemgmt': (bool, None),
                           'readcache_size': (int, {'min': 1, 'max': 10240}),
                           'writecache_size': (int, {'min': 1, 'max': 10240})}
        required_params_new_distributed = {'config_params': sd_config_params}
        required_params_new_alba = {'config_params': sd_config_params,
                                    'fragment_cache_on_read': (bool, None),
                                    'fragment_cache_on_write': (bool, None),
                                    'backend_connection_info': (dict, {'host': (str, Toolbox.regex_ip, False),
                                                                       'port': (int, None),
                                                                       'username': (str, None),
                                                                       'password': (str, None),
                                                                       'backend': (dict, {'backend': (str, Toolbox.regex_guid),
                                                                                          'metadata': (str, Toolbox.regex_preset)})}),
                                    'backend_connection_info_aa': (dict, {'host': (str, Toolbox.regex_ip, False),
                                                                          'port': (int, None),
                                                                          'username': (str, None),
                                                                          'password': (str, None),
                                                                          'backend': (dict, {'backend': (str, Toolbox.regex_guid),
                                                                                             'metadata': (str, Toolbox.regex_preset)})},
                                                                   False)}
        required_params_other = {'config_params': sd_config_params,
                                 'backend_connection_info': (dict, {'host': (str, Toolbox.regex_ip, False),
                                                                    'port': (int, None),
                                                                    'username': (str, None),
                                                                    'password': (str, None)})}

        ###############
        # VALIDATIONS #
        ###############

        # Check parameters
        if not isinstance(parameters, dict):
            raise ValueError('Parameters should be of type "dict"')
        Toolbox.verify_required_params(required_params, parameters)

        client = SSHClient(parameters['storagerouter_ip'])
        unique_id = System.get_my_machine_id(client)

        sd_config_params = parameters['config_params']
        sco_size = sd_config_params['sco_size']
        write_buffer = sd_config_params['write_buffer']
        if (sco_size == 128 and write_buffer < 256) or not (128 <= write_buffer <= 10240):
            raise ValueError('Incorrect storagedriver configuration settings specified')

        # Check backend type existence
        vpool_type = parameters['type']
        if vpool_type not in [be.code for be in BackendTypeList.get_backend_types()]:
            raise ValueError('Unsupported backend type specified: "{0}"'.format(vpool_type))

        # Verify vPool status and additional parameters
        vpool_name = parameters['vpool_name']
        vpool = VPoolList.get_vpool_by_name(vpool_name)
        backend_type = BackendTypeList.get_backend_type_by_code(vpool_type)
        if vpool is not None:
            if vpool.status != VPool.STATUSES.RUNNING:
                raise ValueError('VPool should be in {0} status'.format(VPool.STATUSES.RUNNING))
        else:
            if backend_type.code in ['local', 'distributed']:
                extra_required_params = required_params_new_distributed
            elif backend_type.code == 'alba':
                extra_required_params = required_params_new_alba
            else:
                extra_required_params = required_params_other
            Toolbox.verify_required_params(extra_required_params, parameters)

        # Check storagerouter existence
        storagerouter = StorageRouterList.get_by_ip(client.ip)
        if storagerouter is None:
            raise RuntimeError('Could not find Storage Router with given IP address')

        # Check duplicate vPool name
        all_storagerouters = [storagerouter]
        current_storage_driver_config = {}
        if vpool is not None:
            required_params_sd_config = {'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                         'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                         'dedupe_mode': (str, StorageDriverClient.VPOOL_DEDUPE_MAP.keys()),
                                         'write_buffer': (float, None),
                                         'cache_strategy': (str, StorageDriverClient.VPOOL_CACHE_MAP.keys()),
                                         'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys()),
                                         'tlog_multiplier': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.values())}
            current_storage_driver_config = VPoolController.get_configuration(vpool.guid)
            Toolbox.verify_required_params(required_params=required_params_sd_config,
                                           actual_params=current_storage_driver_config)

            if vpool.backend_type.code == 'local':
                # Might be an issue, investigating whether it's on the same Storage Router or not
                if len(vpool.storagedrivers) == 1 and vpool.storagedrivers[0].storagerouter.machine_id != unique_id:
                    raise RuntimeError('A local vPool with name {0} already exists'.format(vpool_name))
            for vpool_storagedriver in vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    raise RuntimeError('A Storage Driver is already linked to this Storage Router for this vPool: {0}'.format(vpool_name))
            all_storagerouters += [sd.storagerouter for sd in vpool.storagedrivers]

        # Check storagerouter connectivity
        ip_client_map = {}
        offline_nodes_detected = False
        for sr in all_storagerouters:
            try:
                ovs_client = SSHClient(sr.ip, username='ovs')
                root_client = SSHClient(sr.ip, username='root')
                ovs_client.run('pwd')
                root_client.run('pwd')
                ip_client_map[sr.ip] = {'root': root_client,
                                        'ovs': ovs_client}
            except UnableToConnectException:
                offline_nodes_detected = True  # We currently want to allow offline nodes while setting up or extend a vpool (etcd implementation should prevent further failures)
            except Exception as ex:
                raise RuntimeError('Something went wrong building SSH connections. {0}'.format(ex))

        # Check partition role presence
        arakoon_service_found = False
        for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services:
            if service.name == 'arakoon-voldrv':
                arakoon_service_found = True
                break

        error_messages = []
        metadata = StorageRouterController.get_metadata(storagerouter.guid)
        partition_info = metadata['partitions']
        for required_role in [DiskPartition.ROLES.READ, DiskPartition.ROLES.WRITE, DiskPartition.ROLES.DB]:
            if required_role not in partition_info:
                error_messages.append('Missing required partition role {0}'.format(required_role))
            elif len(partition_info[required_role]) == 0:
                error_messages.append('At least 1 {0} partition role is required'.format(required_role))
            else:
                total_available = [part['available'] for part in partition_info[required_role]]
                if total_available == 0:
                    error_messages.append('Not enough available space for {0}'.format(required_role))

        # Create vpool metadata
        cluster_policies = []
        cluster_frag_size = 1
        cluster_total_size = 0
        cluster_nsm_part_guids = []

        sco_size = current_storage_driver_config['sco_size'] if current_storage_driver_config else sco_size
        sco_size *= 1024.0 ** 2
        use_accelerated_alba = False
        backend_connection_info = parameters.get('backend_connection_info', {})
        connection_host = backend_connection_info.get('host')
        connection_port = backend_connection_info.get('port')
        connection_username = backend_connection_info.get('username')
        connection_password = backend_connection_info.get('password')

        if backend_type.code in ['local', 'distributed']:
            vpool_metadata = {'backend_type': 'LOCAL'}
        elif backend_type.code in ['ceph_s3', 'amazon_s3', 'swift_s3']:
            vpool_metadata = {'s3_connection_host': connection_host,
                              's3_connection_port': connection_port,
                              's3_connection_username': connection_username,
                              's3_connection_password': connection_password,
                              's3_connection_flavour': 'SWIFT' if backend_type.code == 'swift_s3' else 'S3',
                              's3_connection_strict_consistency': 'false' if backend_type.code == 'swift_s3' else 'true',
                              's3_connection_verbose_logging': 1,
                              'backend_type': 'S3'}
        else:
            backend_connection_info_aa = parameters.get('backend_connection_info_aa', {})
            backend_guid = backend_connection_info['backend']['backend']
            backend_guid_aa = backend_connection_info_aa.get('backend', {}).get('backend')
            use_accelerated_alba = backend_guid_aa is not None
            if backend_guid == backend_guid_aa:
                raise RuntimeError('Backend and accelerated backend cannot be the same')

            if vpool is not None:
                backend_info_map = {}
                for key, info in vpool.metadata.iteritems():
                    local = info['connection']['local']
                    backend_info_map[key] = {'backend': {'backend': info['backend_guid'],
                                                         'metadata': info['preset']},
                                             'host': info['connection']['host'] if local is False else '',
                                             'port': info['connection']['port'] if local is False else '',
                                             'username': info['connection']['client_id'] if local is False else '',
                                             'password': info['connection']['client_secret'] if local is False else ''}
            else:
                backend_info_map = {'backend': backend_connection_info}
            if use_accelerated_alba is True:
                backend_info_map[storagerouter.guid] = backend_connection_info_aa

            vpool_metadata = {}
            for key, backend_info in backend_info_map.iteritems():
                preset_name = backend_info['backend']['metadata']
                backend_guid = backend_info['backend']['backend']
                connection_info = StorageRouterController._retrieve_alba_connection_info(backend_info=backend_info)
                fragment_cache_on_read = parameters['fragment_cache_on_read']
                fragment_cache_on_write = parameters['fragment_cache_on_write']

                ovs_client = OVSClient(ip=connection_info['host'],
                                       port=connection_info['port'],
                                       credentials=(connection_info['client_id'], connection_info['client_secret']),
                                       version=1)
                backend_dict = ovs_client.get('/alba/backends/{0}/'.format(backend_guid), params={'contents': 'metadata_information,name,ns_statistics,presets'})
                preset_info = dict((preset['name'], preset) for preset in backend_dict['presets'])
                if preset_name not in preset_info:
                    raise RuntimeError('Given preset {0} is not available in backend {1}'.format(preset_name, backend_guid))

                local_backend = connection_info['local']
                policies = []
                for policy_info in preset_info[preset_name]['policies']:
                    policy = json.loads('[{0}]'.format(policy_info.strip('()')))
                    policies.append([policy[0], policy[1]])
                    if local_backend is True:
                        cluster_policies.append([policy[0], policy[1]])

                total_size = float(backend_dict['ns_statistics']['global']['size'])
                fragment_size = float(preset_info[preset_name]['fragment_size'])
                nsm_partition_guids = list(set(backend_dict['metadata_information']['nsm_partition_guids']))
                if local_backend is True:
                    cluster_frag_size = fragment_size
                    cluster_total_size = total_size
                    cluster_nsm_part_guids = nsm_partition_guids
                vpool_metadata[key] = {'name': backend_dict['name'],
                                       'arakoon_config': StorageRouterController._retrieve_alba_arakoon_config(backend_guid=backend_guid, ovs_client=ovs_client),
                                       'backend_info': {'policies': policies,
                                                        'sco_size': sco_size,
                                                        'frag_size': fragment_size,
                                                        'total_size': total_size,
                                                        'nsm_partition_guids': nsm_partition_guids,
                                                        'fragment_cache_on_read': fragment_cache_on_read,
                                                        'fragment_cache_on_write': fragment_cache_on_write},
                                       'connection': connection_info,
                                       'preset': preset_name,
                                       'backend_guid': backend_guid}

        # Check mountpoints are mounted
        db_partition_guids = set()
        read_partition_guids = set()
        write_partition_guids = set()
        for role, part_info in partition_info.iteritems():
            for part in part_info:
                if not client.is_mounted(part['mountpoint']) and part['mountpoint'] != DiskPartition.VIRTUAL_STORAGE_LOCATION:
                    error_messages.append('Mountpoint {0} is not mounted'.format(part['mountpoint']))
                if role == 'DB':
                    db_partition_guids.add(part['guid'])
                elif role == 'READ':
                    read_partition_guids.add(part['guid'])
                elif role == 'WRITE':
                    write_partition_guids.add(part['guid'])

        # Calculate alba metadata overhead
        db_overlap = len(db_partition_guids.intersection(cluster_nsm_part_guids)) > 0  # We only want to take DB partitions into account already claimed by the NSM clusters
        read_overlap = db_overlap and len(db_partition_guids.intersection(read_partition_guids)) > 0
        write_overlap = db_overlap and len(db_partition_guids.intersection(write_partition_guids)) > 0
        sizes_to_reserve = [0]

        if read_overlap is True or write_overlap is True:
            for policy in cluster_policies:
                size_to_reserve = int(cluster_total_size / sco_size * (1200 + (policy[0] + policy[1]) * (25 * sco_size / policy[0] / cluster_frag_size + 56)))
                sizes_to_reserve.append(size_to_reserve)
            # For more information about above formula: see http://jira.cloudfounders.com/browse/OVS-3553

        # Check over-allocation for DB
        db_available_size = partition_info[DiskPartition.ROLES.DB][0]['available']
        db_required_size = StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.DB][0] * 1024 ** 3 + max(sizes_to_reserve)

        if db_available_size < db_required_size:
            error_messages.append('Assigned partition for DB role should be at least {0:.2f} GB'.format(db_required_size / 1024.0 ** 3))

        # Check over-allocation for read, write cache
        shared_size_available = metadata['shared_size']
        readcache_size_available = metadata['readcache_size']
        writecache_size_available = metadata['writecache_size']

        if read_overlap is True and write_overlap is True:
            shared_size_available -= max(sizes_to_reserve)
            if shared_size_available < 0:
                shared_size_available = 0
        elif read_overlap is True:
            readcache_size_available -= max(sizes_to_reserve)
            if readcache_size_available < 0:
                readcache_size_available = 0
        elif write_overlap is True:
            writecache_size_available -= max(sizes_to_reserve)
            if writecache_size_available < 0:
                writecache_size_available = 0

        readcache_size_requested = parameters['readcache_size'] * 1024 ** 3
        writecache_size_requested = parameters['writecache_size'] * 1024 ** 3
        if readcache_size_requested > readcache_size_available + shared_size_available:
            error_messages.append('Too much space requested for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.READ,
                                                                                                                                  (readcache_size_available + shared_size_available) / 1024.0 ** 3,
                                                                                                                                  readcache_size_requested / 1024.0 ** 3))
        if writecache_size_requested > writecache_size_available + shared_size_available:
            error_messages.append('Too much space requested for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.WRITE,
                                                                                                                                  (writecache_size_available + shared_size_available) / 1024.0 ** 3,
                                                                                                                                  writecache_size_requested / 1024.0 ** 3))
        if readcache_size_requested + writecache_size_requested > readcache_size_available + writecache_size_available + shared_size_available:
            error_messages.append('Too much space requested. Available: {0:.2f} GiB, Requested: {1:.2f} GiB'.format((readcache_size_available + writecache_size_available + shared_size_available) / 1024.0 ** 3,
                                                                                                                    (readcache_size_requested + writecache_size_requested) / 1024.0 ** 3))

        if StorageRouterController._check_scrub_partition_present() is False:
            error_messages.append('At least 1 Storage Router must have a {0} partition'.format(DiskPartition.ROLES.SCRUB))

        if arakoon_service_found is False and (DiskPartition.ROLES.DB not in partition_info or len(partition_info[DiskPartition.ROLES.DB]) == 0):
            error_messages.append('DB partition role required')

        # Check available IP addresses
        ipaddresses = metadata['ipaddresses']
        grid_ip = EtcdConfiguration.get('/ovs/framework/hosts/{0}/ip'.format(unique_id))
        if grid_ip in ipaddresses:
            ipaddresses.remove(grid_ip)
        if not ipaddresses:
            error_messages.append('No available IP addresses found suitable for Storage Router storage IP')

        # Check storage IP (for VMWARE)
        storage_ip = parameters['storage_ip']
        if vpool is not None:
            for existing_storagedriver in vpool.storagedrivers:
                if existing_storagedriver.storage_ip != storage_ip:
                    error_messages.append('Storage IP {0} is not identical to previously configured storage IPs'.format(storage_ip))
                    break

        if error_messages:
            raise ValueError('Errors validating the partition roles:\n - {0}'.format('\n - '.join(set(error_messages))))

        ######################
        # START ADDING VPOOL #
        ######################
        logger.info('Add vPool {0} started'.format(vpool_name))
        new_vpool = False
        if vpool is None:  # Keep in mind that if the Storage Driver exists, the vPool does as well
            new_vpool = True
            vpool = VPool()
            vpool.backend_type = backend_type
            vpool.metadata = vpool_metadata
            vpool.name = vpool_name
            vpool.login = connection_username
            vpool.password = connection_password
            vpool.connection = '{0}:{1}'.format(connection_host, connection_port) if connection_host else None
            vpool.description = '{0} {1}'.format(vpool.backend_type.code, vpool_name)
            vpool.rdma_enabled = sd_config_params['dtl_transport'] == StorageDriverClient.FRAMEWORK_DTL_TRANSPORT_RSOCKET
            vpool.status = VPool.STATUSES.INSTALLING
            vpool.save()
        else:
            vpool.status = VPool.STATUSES.EXTENDING
            if vpool.backend_type.code == 'alba':
                vpool.metadata = vpool_metadata
            vpool.save()

        ###################
        # CREATE SERVICES #
        ###################
        if arakoon_service_found is False:
            StorageDriverController.manual_voldrv_arakoon_checkup()

        # Verify SD arakoon cluster is available and 'in_use'
        root_client = ip_client_map[storagerouter.ip]['root']
        watcher_volumedriver_service = 'watcher-volumedriver'
        if not ServiceManager.has_service(watcher_volumedriver_service, client=root_client):
            ServiceManager.add_service(watcher_volumedriver_service, client=root_client)
            ServiceManager.enable_service(watcher_volumedriver_service, client=root_client)
            ServiceManager.start_service(watcher_volumedriver_service, client=root_client)

        local_backend_data = {}
        if vpool.backend_type.code in ['local', 'distributed']:
            local_backend_data = {'backend_type': 'LOCAL',
                                  'local_connection_path': parameters.get('distributed_mountpoint', '/tmp')}

        model_ports_in_use = []
        for port_storagedriver in StorageDriverList.get_storagedrivers():
            if port_storagedriver.storagerouter_guid == storagerouter.guid:
                # Local storagedrivers
                model_ports_in_use += port_storagedriver.ports
                if port_storagedriver.alba_proxy is not None:
                    model_ports_in_use.append(port_storagedriver.alba_proxy.service.ports[0])

        # Connection information is Storage Driver related information
        ports = StorageRouterController._get_free_ports(client, model_ports_in_use, 3)
        model_ports_in_use += ports

        vrouter_id = '{0}{1}'.format(vpool_name, unique_id)
        arakoon_cluster_name = str(EtcdConfiguration.get('/ovs/framework/arakoon_clusters|voldrv'))
        config = ArakoonClusterConfig(arakoon_cluster_name)
        config.load_config()
        arakoon_nodes = []
        arakoon_node_configs = []
        for node in config.nodes:
            arakoon_nodes.append({'node_id': node.name, 'host': node.ip, 'port': node.client_port})
            arakoon_node_configs.append(ArakoonNodeConfig(str(node.name), str(node.ip), node.client_port))
        node_configs = []
        for existing_storagedriver in StorageDriverList.get_storagedrivers():
            if existing_storagedriver.vpool_guid == vpool.guid:
                node_configs.append(ClusterNodeConfig(str(existing_storagedriver.storagedriver_id),
                                                      str(existing_storagedriver.cluster_ip),
                                                      existing_storagedriver.ports[0],
                                                      existing_storagedriver.ports[1],
                                                      existing_storagedriver.ports[2]))
        node_configs.append(ClusterNodeConfig(vrouter_id, str(grid_ip), ports[0], ports[1], ports[2]))

        try:
            vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), arakoon_cluster_name, arakoon_node_configs)
            vrouter_clusterregistry.set_node_configs(node_configs)
        except:
            vpool.status = VPool.STATUSES.FAILURE
            vpool.save()
            if new_vpool is True:
                vpool.delete()
            raise

        filesystem_config = StorageDriverConfiguration.build_filesystem_by_hypervisor(storagerouter.pmachine.hvtype)
        filesystem_config.update({'fs_enable_shm_interface': 1,
                                  'fs_metadata_backend_arakoon_cluster_nodes': [],
                                  'fs_metadata_backend_mds_nodes': [],
                                  'fs_metadata_backend_type': 'MDS'})

        # Updating the model
        storagedriver = StorageDriver()
        storagedriver.name = vrouter_id.replace('_', ' ')
        storagedriver.ports = ports
        storagedriver.vpool = vpool
        storagedriver.cluster_ip = grid_ip
        storagedriver.storage_ip = '127.0.0.1' if storagerouter.pmachine.hvtype == 'KVM' else storage_ip
        storagedriver.mountpoint = '/mnt/{0}'.format(vpool_name)
        storagedriver.mountpoint_dfs = local_backend_data.get('local_connection_path')
        storagedriver.description = storagedriver.name
        storagedriver.storagerouter = storagerouter
        storagedriver.storagedriver_id = vrouter_id
        storagedriver.save()

        ##############################
        # CREATE PARTITIONS IN MODEL #
        ##############################

        # 1. Retrieve largest write mountpoint (SSD > SATA)
        largest_ssd_write_mountpoint = None
        largest_sata_write_mountpoint = None
        if backend_type.code == 'alba':  # We need largest SSD to put fragment cache on
            largest_ssd = 0
            largest_sata = 0
            for role, info in partition_info.iteritems():
                if role == DiskPartition.ROLES.WRITE:
                    for part in info:
                        if part['ssd'] is True and part['available'] > largest_ssd:
                            largest_ssd = part['available']
                            largest_ssd_write_mountpoint = part['guid']
                        elif part['ssd'] is False and part['available'] > largest_sata:
                            largest_sata = part['available']
                            largest_sata_write_mountpoint = part['guid']

        largest_write_mountpoint = DiskPartition(largest_ssd_write_mountpoint or largest_sata_write_mountpoint or partition_info[DiskPartition.ROLES.WRITE][0]['guid'])
        mountpoint_fragment_cache = None
        if backend_type.code == 'alba' and use_accelerated_alba is False:
            mountpoint_fragment_cache = largest_write_mountpoint

        # 2. Calculate WRITE / FRAG cache
        # IMPORTANT: Available size in partition_info has already been subtracted with competing roles (DB, SCRUB) and claimed space by other vpools
        #   - Creation of partitions is important:  1st WRITE, 2nd READ, 3rd DB/SCRUB
        #   - Example: Partition with DB and READ role
        #   - If we would first create SCRUB and DB storagedriver partition and request the partition_info again, this already claimed space would be taken into account
        #   - and the competing DB role would also be taken into account again, resulting READ space would be (total - 2 x DB space)
        frag_size = None
        sdp_frag = None
        dirs2create = list()
        writecaches = list()
        writecache_information = partition_info[DiskPartition.ROLES.WRITE]
        total_available = sum([part['available'] for part in writecache_information])
        for writecache_info in writecache_information:
            available = writecache_info['available']
            partition = DiskPartition(writecache_info['guid'])
            proportion = available * 100.0 / total_available
            size_to_be_used = proportion * writecache_size_requested / 100
            if mountpoint_fragment_cache is not None and partition == mountpoint_fragment_cache:
                frag_size = int(size_to_be_used * 0.10)  # Bytes
                w_size = int(size_to_be_used * 0.88 / 1024 / 4096) * 4096  # KiB
                sdp_frag = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                              'role': DiskPartition.ROLES.WRITE,
                                                                                              'sub_role': StorageDriverPartition.SUBROLE.FCACHE,
                                                                                              'partition': DiskPartition(writecache_info['guid'])})
                sdp_write = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': long(size_to_be_used),
                                                                                               'role': DiskPartition.ROLES.WRITE,
                                                                                               'sub_role': StorageDriverPartition.SUBROLE.SCO,
                                                                                               'partition': DiskPartition(writecache_info['guid'])})
                dirs2create.append(sdp_frag.path)
            else:
                w_size = int(size_to_be_used * 0.98 / 1024 / 4096) * 4096
                sdp_write = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': long(size_to_be_used),
                                                                                               'role': DiskPartition.ROLES.WRITE,
                                                                                               'sub_role': StorageDriverPartition.SUBROLE.SCO,
                                                                                               'partition': DiskPartition(writecache_info['guid'])})
            writecaches.append({'path': sdp_write.path,
                                'size': '{0}KiB'.format(w_size)})
            dirs2create.append(sdp_write.path)

        # 3. Calculate READ cache
        if shared_size_available > 0:  # If READ, WRITE are shared, WRITE will have taken up space by now
            partition_info = StorageRouterController.get_metadata(storagerouter.guid)['partitions']
        readcaches = list()
        files2create = list()
        readcache_size = 0
        readcache_information = partition_info[DiskPartition.ROLES.READ]
        total_available = sum([part['available'] for part in readcache_information])
        for readcache_info in readcache_information:
            available = readcache_info['available']
            proportion = available * 100.0 / total_available
            size_to_be_used = proportion * readcache_size_requested / 100
            r_size = int(size_to_be_used * 0.98 / 1024 / 4096) * 4096  # KiB
            readcache_size += r_size

            sdp_read = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': long(size_to_be_used),
                                                                                          'role': DiskPartition.ROLES.READ,
                                                                                          'partition': DiskPartition(readcache_info['guid'])})
            readcaches.append({'path': '{0}/read.dat'.format(sdp_read.path),
                               'size': '{0}KiB'.format(r_size)})
            files2create.append('{0}/read.dat'.format(sdp_read.path))

        # 4. Assign DB
        db_info = partition_info[DiskPartition.ROLES.DB][0]
        size = StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.DB][0] * 1024 ** 3 + max(sizes_to_reserve)
        percentage = db_info['available'] * StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.DB][1] / 100.0 + max(sizes_to_reserve)
        sdp_tlogs = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                       'role': DiskPartition.ROLES.DB,
                                                                                       'sub_role': StorageDriverPartition.SUBROLE.TLOG,
                                                                                       'partition': DiskPartition(db_info['guid'])})
        sdp_metadata = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': long(max(size, percentage)),
                                                                                          'role': DiskPartition.ROLES.DB,
                                                                                          'sub_role': StorageDriverPartition.SUBROLE.MD,
                                                                                          'partition': DiskPartition(db_info['guid'])})
        volume_manager_config = {"tlog_path": sdp_tlogs.path,
                                 "metadata_path": sdp_metadata.path,
                                 "clean_interval": 1,
                                 "dtl_throttle_usecs": 4000}

        # 5. Create SCRUB storagedriver partition (if necessary)
        sdp_scrub = None
        scrub_info = partition_info[DiskPartition.ROLES.SCRUB]
        if len(scrub_info) > 0:
            scrub_info = scrub_info[0]
            size = StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.SCRUB][0] * 1024 ** 3
            percentage = scrub_info['available'] * StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.SCRUB][1] / 100.0
            sdp_scrub = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': long(max(size, percentage)),
                                                                                           'role': DiskPartition.ROLES.SCRUB,
                                                                                           'partition': DiskPartition(scrub_info['guid'])})
            dirs2create.append(sdp_scrub.path)
        dirs2create.append(sdp_tlogs.path)
        dirs2create.append(sdp_metadata.path)

        sdp_fd = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                    'role': DiskPartition.ROLES.WRITE,
                                                                                    'sub_role': StorageDriverPartition.SUBROLE.FD,
                                                                                    'partition': largest_write_mountpoint})
        sdp_dtl = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                     'role': DiskPartition.ROLES.WRITE,
                                                                                     'sub_role': StorageDriverPartition.SUBROLE.DTL,
                                                                                     'partition': largest_write_mountpoint})
        rsppath = '{0}/{1}'.format(EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|rsp'.format(unique_id)), vpool_name)
        dirs2create.append(sdp_dtl.path)
        dirs2create.append(sdp_fd.path)
        dirs2create.append(rsppath)
        dirs2create.append(storagedriver.mountpoint)

        if backend_type.code == 'alba' and frag_size is None and use_accelerated_alba is False:
            vpool.status = VPool.STATUSES.FAILURE
            vpool.save()
            raise ValueError('Something went wrong trying to calculate the fragment cache size')

        root_client.dir_create(dirs2create)
        root_client.file_create(files2create)

        config_dir = '{0}/storagedriver/storagedriver'.format(EtcdConfiguration.get('/ovs/framework/paths|cfgdir'))
        client.dir_create(config_dir)
        alba_proxy = storagedriver.alba_proxy
        if alba_proxy is None and vpool.backend_type.code == 'alba':
            service = DalService()
            service.storagerouter = storagerouter
            service.ports = [StorageRouterController._get_free_ports(client, model_ports_in_use, 1)]
            service.name = 'albaproxy_{0}'.format(vpool_name)
            service.type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ALBA_PROXY)
            service.save()
            alba_proxy = AlbaProxy()
            alba_proxy.service = service
            alba_proxy.storagedriver = storagedriver
            alba_proxy.save()

            config_tree = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, alba_proxy.guid)
            metadata_keys = {'backend': 'abm'} if use_accelerated_alba is False else {'backend': 'abm', storagerouter.guid: 'abm_aa'}
            for metadata_key in metadata_keys:
                arakoon_config = vpool.metadata[metadata_key]['arakoon_config']
                config = RawConfigParser()
                for section in arakoon_config:
                    config.add_section(section)
                    for key, value in arakoon_config[section].iteritems():
                        config.set(section, key, value)
                config_io = StringIO()
                config.write(config_io)
                EtcdConfiguration.set(config_tree.format(metadata_keys[metadata_key]), config_io.getvalue(), raw=True)

            fragment_cache_on_read = parameters['fragment_cache_on_read']
            fragment_cache_on_write = parameters['fragment_cache_on_write']
            if use_accelerated_alba is True:
                fragment_cache_info = ['alba', {'albamgr_cfg_url': 'etcd://127.0.0.1:2379{0}'.format(config_tree.format('abm_aa')),
                                                'bucket_strategy': ['1-to-1', {'prefix': vpool.metadata[storagerouter.guid]['name'],
                                                                               'preset': vpool.metadata[storagerouter.guid]['preset']}],
                                                'manifest_cache_size': 100000,
                                                'cache_on_read': fragment_cache_on_read,
                                                'cache_on_write': fragment_cache_on_write}]
            else:
                fragment_cache_info = ['local', {'path': sdp_frag.path,
                                                 'max_size': frag_size,
                                                 'cache_on_read': fragment_cache_on_read,
                                                 'cache_on_write': fragment_cache_on_write}]

            EtcdConfiguration.set(config_tree.format('main'), json.dumps({
                'log_level': 'info',
                'port': alba_proxy.service.ports[0],
                'ips': ['127.0.0.1'],
                'manifest_cache_size': 100000,
                'fragment_cache': fragment_cache_info,
                'albamgr_cfg_url': 'etcd://127.0.0.1:2379{0}'.format(config_tree.format('abm'))
            }), raw=True)

        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
        storagedriver_config.load()

        # Possible modes: ['classic', 'ganesha']
        volumedriver_mode = 'classic'
        if storagerouter.pmachine.hvtype == 'VMWARE':
            volumedriver_mode = EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|vmware_mode'.format(unique_id))
            if volumedriver_mode == 'ganesha':
                ganesha_config = '/opt/OpenvStorage/config/storagedriver/storagedriver/{0}_ganesha.conf'.format(vpool_name)
                contents = ''
                for template in ['ganesha-core', 'ganesha-export']:
                    contents += client.file_read('/opt/OpenvStorage/config/templates/{0}.conf'.format(template))
                params = {'VPOOL_NAME': vpool_name,
                          'VPOOL_MOUNTPOINT': '/mnt/{0}'.format(vpool_name),
                          'CONFIG_PATH': storagedriver_config.remote_path,
                          'NFS_FILESYSTEM_ID': storagerouter.ip.split('.', 2)[-1]}
                for key, value in params.iteritems():
                    contents = contents.replace('<{0}>'.format(key), value)
                client.file_write(ganesha_config, contents)

        if new_vpool is True:  # New vPool
            sco_size = sd_config_params['sco_size']
            dtl_mode = sd_config_params['dtl_mode']
            dedupe_mode = sd_config_params['dedupe_mode']
            cluster_size = sd_config_params['cluster_size']
            dtl_transport = sd_config_params['dtl_transport']
            cache_strategy = sd_config_params['cache_strategy']
            tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[sco_size]
            sco_factor = float(write_buffer) / tlog_multiplier / sco_size  # sco_factor = write buffer / tlog multiplier (default 20) / sco size (in MiB)
        else:  # Extend vPool
            sco_size = current_storage_driver_config['sco_size']
            dtl_mode = current_storage_driver_config['dtl_mode']
            dedupe_mode = current_storage_driver_config['dedupe_mode']
            cluster_size = current_storage_driver_config['cluster_size']
            dtl_transport = current_storage_driver_config['dtl_transport']
            cache_strategy = current_storage_driver_config['cache_strategy']
            tlog_multiplier = current_storage_driver_config['tlog_multiplier']
            sco_factor = float(current_storage_driver_config['write_buffer']) / tlog_multiplier / sco_size

        if dtl_mode == 'no_sync':
            filesystem_config['fs_dtl_host'] = None
            filesystem_config['fs_dtl_config_mode'] = StorageDriverClient.VOLDRV_DTL_MANUAL_MODE
        else:
            filesystem_config['fs_dtl_mode'] = StorageDriverClient.VPOOL_DTL_MODE_MAP[dtl_mode]
            filesystem_config['fs_dtl_config_mode'] = StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE

        volume_manager_config['default_cluster_size'] = cluster_size * 1024
        volume_manager_config['read_cache_default_mode'] = StorageDriverClient.VPOOL_DEDUPE_MAP[dedupe_mode]
        volume_manager_config['read_cache_default_behaviour'] = StorageDriverClient.VPOOL_CACHE_MAP[cache_strategy]
        volume_manager_config['number_of_scos_in_tlog'] = tlog_multiplier
        volume_manager_config['non_disposable_scos_factor'] = sco_factor

        queue_urls = []
        for current_storagerouter in StorageRouterList.get_masters():
            queue_urls.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(EtcdConfiguration.get('/ovs/framework/messagequeue|protocol'),
                                                                      EtcdConfiguration.get('/ovs/framework/messagequeue|user'),
                                                                      EtcdConfiguration.get('/ovs/framework/messagequeue|password'),
                                                                      current_storagerouter.ip)})

        storagedriver_config.clean()  # Clean out obsolete values
        if vpool.backend_type.code == 'alba':
            backend_connection_manager = {'alba_connection_host': '127.0.0.1',
                                          'alba_connection_port': alba_proxy.service.ports[0],
                                          'alba_connection_preset': vpool.metadata['backend']['preset'],
                                          'alba_connection_timeout': 15,
                                          'backend_type': 'ALBA'}
        elif vpool.backend_type.code in ['local', 'distributed']:
            backend_connection_manager = local_backend_data
        else:
            backend_connection_manager = vpool.metadata
        backend_connection_manager.update({'backend_interface_retries_on_error': 5,
                                           'backend_interface_retry_interval_secs': 1,
                                           'backend_interface_retry_backoff_multiplier': 2.0})
        storagedriver_config.configure_backend_connection_manager(**backend_connection_manager)
        storagedriver_config.configure_content_addressed_cache(clustercache_mount_points=readcaches,
                                                               read_cache_serialization_path=rsppath)
        storagedriver_config.configure_scocache(scocache_mount_points=writecaches,
                                                trigger_gap='1GB',
                                                backoff_gap='2GB')
        storagedriver_config.configure_distributed_transaction_log(dtl_path=sdp_dtl.path,
                                                                   dtl_transport=StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[dtl_transport])
        storagedriver_config.configure_filesystem(**filesystem_config)
        storagedriver_config.configure_volume_manager(**volume_manager_config)
        storagedriver_config.configure_volume_router(vrouter_id=vrouter_id,
                                                     vrouter_redirect_timeout_ms='5000',
                                                     vrouter_routing_retries=10,
                                                     vrouter_volume_read_threshold=1024,
                                                     vrouter_volume_write_threshold=1024,
                                                     vrouter_file_read_threshold=1024,
                                                     vrouter_file_write_threshold=1024,
                                                     vrouter_min_workers=4,
                                                     vrouter_max_workers=16,
                                                     vrouter_sco_multiplier=sco_size * 1024 / cluster_size,  # sco multiplier = SCO size (in MiB) / cluster size (currently 4KiB),
                                                     vrouter_backend_sync_timeout_ms=5000,
                                                     vrouter_migrate_timeout_ms=5000)
        storagedriver_config.configure_volume_router_cluster(vrouter_cluster_id=vpool.guid)
        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=arakoon_cluster_name,
                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                              dls_arakoon_cluster_id=arakoon_cluster_name,
                                                              dls_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_file_driver(fd_cache_path=sdp_fd.path,
                                                   fd_extent_cache_capacity='1024',
                                                   fd_namespace='fd-{0}-{1}'.format(vpool_name, vpool.guid))
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=EtcdConfiguration.get('/ovs/framework/messagequeue|queues.storagedriver'),
                                                       events_amqp_uris=queue_urls)
        storagedriver_config.configure_threadpool_component(num_threads=16)
        storagedriver_config.save(client, reload_config=False)

        DiskController.sync_with_reality(storagerouter.guid)

        MDSServiceController.prepare_mds_service(storagerouter=storagerouter,
                                                 vpool=vpool,
                                                 fresh_only=True,
                                                 reload_config=False)

        if sdp_scrub is not None:
            root_client.dir_chmod(sdp_scrub.path, 0777)  # Used by gather_scrub_work which is a celery task executed by 'ovs' user and should be able to write in it

        params = {'VPOOL_MOUNTPOINT': storagedriver.mountpoint,
                  'HYPERVISOR_TYPE': storagerouter.pmachine.hvtype,
                  'VPOOL_NAME': vpool_name,
                  'VPOOL_GUID': vpool.guid,
                  'CONFIG_PATH': storagedriver_config.remote_path,
                  'UUID': str(uuid.uuid4()),
                  'OVS_UID': check_output('id -u ovs', shell=True).strip(),
                  'OVS_GID': check_output('id -g ovs', shell=True).strip(),
                  'KILL_TIMEOUT': str(int(readcache_size / 1024.0 / 1024.0 / 6.0 + 30))}

        logger.info('volumedriver_mode: {0}'.format(volumedriver_mode))
        logger.info('backend_type: {0}'.format(vpool.backend_type.code))
        dtl_service = 'ovs-dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name='ovs-dtl', params=params, client=root_client, target_name=dtl_service)
        ServiceManager.start_service(dtl_service, client=root_client)
        dependencies = None
        template_name = 'ovs-ganesha' if volumedriver_mode == 'ganesha' else 'ovs-volumedriver'
        if vpool.backend_type.code == 'alba':
            alba_proxy_service = 'ovs-albaproxy_{0}'.format(vpool.name)
            params['PROXY_ID'] = storagedriver.alba_proxy_guid
            ServiceManager.add_service(name='ovs-albaproxy', params=params, client=root_client, target_name=alba_proxy_service)
            ServiceManager.start_service(alba_proxy_service, client=root_client)
            dependencies = [alba_proxy_service]

        voldrv_service = 'ovs-volumedriver_{0}'.format(vpool.name)
        ServiceManager.add_service(name=template_name, params=params, client=root_client, target_name=voldrv_service, additional_dependencies=dependencies)

        if storagerouter.pmachine.hvtype == 'VMWARE' and volumedriver_mode == 'classic':
            root_client.run("grep -q '/tmp localhost(ro,no_subtree_check)' /etc/exports || echo '/tmp localhost(ro,no_subtree_check)' >> /etc/exports")
            root_client.run('service nfs-kernel-server start')

        if storagerouter.pmachine.hvtype == 'KVM':
            vpool_overview = root_client.run('virsh pool-list --all').splitlines()
            if vpool_overview:
                vpool_overview.pop(1)  # Pop   ---------------
                vpool_overview.pop(0)  # Pop   Name   State   Autostart
                virsh_pool_already_exists = False
                for vpool_info in vpool_overview:
                    virsh_vpool_name = vpool_info.split()[0].strip()
                    if vpool.name == virsh_vpool_name:
                        virsh_pool_already_exists = True
                        break
                if not virsh_pool_already_exists:
                    root_client.run('virsh pool-define-as {0} dir - - - - {1}'.format(vpool_name,
                                                                                      storagedriver.mountpoint))
                    root_client.run('virsh pool-build {0}'.format(vpool_name))
                    root_client.run('virsh pool-start {0}'.format(vpool_name))
                    root_client.run('virsh pool-autostart {0}'.format(vpool_name))

        # Start service
        storagedriver = StorageDriver(storagedriver.guid)
        current_startup_counter = storagedriver.startup_counter
        ServiceManager.enable_service(voldrv_service, client=root_client)
        ServiceManager.start_service(voldrv_service, client=root_client)
        tries = 60
        while storagedriver.startup_counter == current_startup_counter and tries > 0:
            logger.debug('Waiting for the StorageDriver to start up...')
            running = ServiceManager.get_service_status(voldrv_service, client=root_client)
            if running is False:
                vpool.status = VPool.STATUSES.FAILURE
                vpool.save()
                raise RuntimeError('StorageDriver service failed to start (service not running)')
            tries -= 1
            time.sleep(60 - tries)
            storagedriver = StorageDriver(storagedriver.guid)
        if storagedriver.startup_counter == current_startup_counter:
            vpool.status = VPool.STATUSES.FAILURE
            vpool.save()
            raise RuntimeError('StorageDriver service failed to start (got no event)')
        logger.debug('StorageDriver running')

        mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vpool, check_online=not offline_nodes_detected)
        for sr in all_storagerouters:
            if sr.ip not in ip_client_map:
                continue
            node_client = ip_client_map[sr.ip]['ovs']
            storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
            storagedriver_config.load()
            if storagedriver_config.is_new is False:
                storagedriver_config.clean()  # Clean out obsolete values
                storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                storagedriver_config.save(node_client)

        # Everything's reconfigured, refresh new cluster configuration
        sd_client = StorageDriverClient.load(vpool)
        for current_storagedriver in vpool.storagedrivers:
            if current_storagedriver.storagerouter.ip not in ip_client_map:
                continue
            sd_client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id))

        # Fill vPool size
        with Remote(root_client.ip, [os], 'root') as remote:
            vfs_info = remote.os.statvfs('/mnt/{0}'.format(vpool_name))
            vpool.size = vfs_info.f_blocks * vfs_info.f_bsize
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()

        if offline_nodes_detected is True:
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
            except:
                pass
            try:
                for vdisk in vpool.vdisks:
                    MDSServiceController.ensure_safety(vdisk=vdisk)
            except:
                pass
        else:
            VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
            for vdisk in vpool.vdisks:
                MDSServiceController.ensure_safety(vdisk=vdisk)

        mgmt_center = Factory.get_mgmtcenter(storagerouter.pmachine)
        if mgmt_center:
            if parameters['integratemgmt'] is True:
                mgmt_center.configure_vpool_for_host(vpool.guid, storagerouter.pmachine.ip)
        else:
            logger.info('Storagerouter {0} does not have management center'.format(storagerouter.name))
        logger.info('Add vPool {0} ended successfully'.format(vpool_name))

    @staticmethod
    @celery.task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(storagedriver_guid, offline_storage_router_guids=None):
        """
        Removes a Storage Driver (if its the last Storage Driver for a vPool, the vPool is removed as well)
        :param storagedriver_guid: Guid of the Storage Driver to remove
        :type storagedriver_guid: str

        :param offline_storage_router_guids: Guids of Storage Routers which are offline and will be removed from cluster.
                                             WHETHER VPOOL WILL BE DELETED DEPENDS ON THIS
        :type offline_storage_router_guids: list

        :return: None
        """
        storage_driver = StorageDriver(storagedriver_guid)
        logger.info('Remove Storage Driver - Guid {0} - Deleting Storage Driver {1}'.format(storage_driver.guid, storage_driver.name))

        if offline_storage_router_guids is None:
            offline_storage_router_guids = []

        client = None
        temp_client = None
        storage_drivers_left = False

        vpool = storage_driver.vpool
        storage_router = storage_driver.storagerouter
        storage_router_online = True
        storage_routers_offline = [StorageRouter(storage_router_guid) for storage_router_guid in offline_storage_router_guids]
        sr_sd_map = {}
        for sd in vpool.storagedrivers:
            sr_sd_map[sd.storagerouter] = sd

        # Validations
        logger.info('Remove Storage Driver - Guid {0} - Checking availability of related Storage Routers'.format(storage_driver.guid, storage_driver.name))
        if vpool.status != VPool.STATUSES.RUNNING:
            raise ValueError('VPool should be in {0} status'.format(VPool.STATUSES.RUNNING))
        for sr, sd in sr_sd_map.iteritems():
            if sr in storage_routers_offline:
                logger.info('Remove Storage Driver - Guid {0} - Storage Router {1} with IP {2} is offline'.format(storage_driver.guid, sr.name, sr.ip))
                continue
            if sr != storage_router:
                storage_drivers_left = True
            try:
                temp_client = SSHClient(sr, username='root')
                with Remote(temp_client.ip, [LocalStorageRouterClient]) as remote:
                    path = 'etcd://127.0.0.1:2379/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, sd.storagedriver_id)
                    lsrc = remote.LocalStorageRouterClient(path)
                    lsrc.server_revision()  # 'Cheap' call to verify whether volumedriver is responsive
                client = temp_client
                logger.info('Remove Storage Driver - Guid {0} - Storage Router {1} with IP {2} is online'.format(storage_driver.guid, sr.name, sr.ip))
            except UnableToConnectException:
                if sr == storage_router:
                    logger.info('Remove Storage Driver - Guid {0} - Storage Router {1} with IP {2} is offline'.format(storage_driver.guid, sr.name, sr.ip))
                    storage_router_online = False
                else:
                    raise RuntimeError('Not all StorageRouters are reachable')
            except Exception, ex:
                if 'ClusterNotReachableException' in str(ex):
                    if len(sr_sd_map) != 1:
                        raise RuntimeError('Not all StorageDrivers are reachable, please (re)start them and try again')
                    client = temp_client
                else:
                    raise

        if client is None:
            raise RuntimeError('Could not found any responsive node in the cluster')

        vpool_guids = set()
        pmachine_guids = set()
        for virtual_machine in VMachineList.get_customer_vmachines():
            if virtual_machine.vpool_guid is not None:
                vpool_guids.add(virtual_machine.vpool_guid)
            pmachine_guids.add(virtual_machine.pmachine_guid)

        if storage_drivers_left is False and storage_router.pmachine.guid in pmachine_guids and vpool.guid in vpool_guids and storage_router_online is True:
            raise RuntimeError('There are still vMachines served from the given Storage Driver')
        if any(vdisk for vdisk in vpool.vdisks if vdisk.storagedriver_id == storage_driver.storagedriver_id) and storage_router_online is True:
            raise RuntimeError('There are still vDisks served from the given Storage Driver')

        # Start removal
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.SHRINKING
        else:
            vpool.status = VPool.STATUSES.DELETING
        vpool.save()

        # Unconfigure management center
        vdisks = []
        errors_found = False
        if storage_router_online is True:
            logger.info('Remove Storage Driver - Guid {0} - Checking management center'.format(storage_driver.guid))
            try:
                mgmt_center = Factory.get_mgmtcenter(pmachine=storage_router.pmachine)
                if mgmt_center and mgmt_center.is_host_configured_for_vpool(vpool.guid, storage_router.pmachine.ip):
                    logger.info('Remove Storage Driver - Guid {0} - Unconfiguring host with IP {1}'.format(storage_driver.guid, storage_router.pmachine.ip))
                    mgmt_center.unconfigure_vpool_for_host(vpool.guid, storage_drivers_left is False, storage_router.pmachine.ip)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Unconfiguring failed with error: {1}'.format(storage_driver.guid, ex))
                errors_found = True
        # Migrate vDisks if node is offline
        else:
            logger.info('Remove Storage Driver - Guid {0} - Checking migration'.format(storage_driver.guid))
            available_storage_drivers = []
            for sd in vpool.storagedrivers:
                if sd != storage_driver and sd.storagerouter not in storage_routers_offline:
                    logger.info('Remove Storage Driver - Guid {0} - Available Storage Driver for migration - {1}'.format(storage_driver.guid, sd.name))
                    available_storage_drivers.append(str(sd.name))
            if available_storage_drivers:
                for vdisk in vpool.vdisks:
                    vdisk.invalidate_dynamics(['info', 'storagedriver_id'])
                    if vdisk.storagedriver_id == '':
                        logger.info('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - Migration required'.format(storage_driver.guid, vdisk.guid, vdisk.name))
                        vdisks.append(vdisk)
                        try:
                            vdisk.storagedriver_client.migrate(str(vdisk.volume_id), available_storage_drivers[random.randint(0, len(available_storage_drivers) - 1)], True)
                        except Exception as ex:
                            logger.error('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - Migration failed with error: {3}'.format(storage_driver.guid, vdisk.guid, vdisk.name, ex))
                            errors_found = True
                        vdisk.invalidate_dynamics(['info', 'storagedriver_id'])
                        if vdisk.storagedriver_id:
                            try:
                                logger.info('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - Ensuring MDS safety after migration'.format(storage_driver.guid, vdisk.guid, vdisk.name))
                                MDSServiceController.ensure_safety(vdisk=vdisk,
                                                                   excluded_storagerouters=[storage_router] + storage_routers_offline)
                            except Exception as ex:  # We don't put errors_found to True, because ensure safety could possibly succeed later on
                                logger.error('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - Ensuring MDS safety failed with error: {3}'.format(storage_driver.guid, vdisk.guid, vdisk.name, ex))
                    else:
                        logger.info('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - No migration required'.format(storage_driver.guid, vdisk.guid, vdisk.name))

        # Unconfigure or reconfigure the MDSes
        logger.info('Remove Storage Driver - Guid {0} - Reconfiguring MDSes'.format(storage_driver.guid))
        mds_services_to_remove = [mds_service for mds_service in vpool.mds_services if mds_service.service.storagerouter_guid == storage_router.guid]
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                vdisk.invalidate_dynamics(['info', 'storagedriver_id'])
                if vdisk.storagedriver_id:
                    try:
                        logger.info('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - Ensuring MDS safety'.format(storage_driver.guid, vdisk.guid, vdisk.name))
                        MDSServiceController.ensure_safety(vdisk=vdisk,
                                                           excluded_storagerouters=[storage_router] + storage_routers_offline)
                    except Exception as ex:
                        logger.error('Remove Storage Driver - Guid {0} - Virtual Disk {1} {2} - Ensuring MDS safety failed with error: {3}'.format(storage_driver.guid, vdisk.guid, vdisk.name, ex))

        arakoon_cluster_name = str(EtcdConfiguration.get('/ovs/framework/arakoon_clusters|voldrv'))
        config = ArakoonClusterConfig(arakoon_cluster_name)
        config.load_config()
        arakoon_node_configs = []
        offline_node_ips = [sr.ip for sr in storage_routers_offline]
        for node in config.nodes:
            if node.ip in offline_node_ips or (node.ip == storage_router.ip and storage_router_online is False):
                continue
            arakoon_node_configs.append(ArakoonNodeConfig(str(node.name), str(node.ip), node.client_port))
        logger.info('Remove Storage Driver - Guid {0} - Arakoon node configs - \n{1}'.format(storage_driver.guid, '\n'.join([str(config) for config in arakoon_node_configs])))
        vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), arakoon_cluster_name, arakoon_node_configs)

        # Disable and stop DTL, voldrv and albaproxy services
        if storage_router_online is True:
            dtl_service = 'dtl_{0}'.format(vpool.name)
            voldrv_service = 'volumedriver_{0}'.format(vpool.name)
            albaproxy_service = 'albaproxy_{0}'.format(vpool.name)
            client = SSHClient(storage_router, username='root')

            for service in [voldrv_service, dtl_service]:
                try:
                    if ServiceManager.has_service(service, client=client):
                        logger.info('Remove Storage Driver - Guid {0} - Disabling service {1}'.format(storage_driver.guid, service))
                        ServiceManager.disable_service(service, client=client)
                        logger.info('Remove Storage Driver - Guid {0} - Stopping service {1}'.format(storage_driver.guid, service))
                        ServiceManager.stop_service(service, client=client)
                        logger.info('Remove Storage Driver - Guid {0} - Removing service {1}'.format(storage_driver.guid, service))
                        ServiceManager.remove_service(service, client=client)
                except Exception as ex:
                    logger.error('Remove Storage Driver - Guid {0} - Disabling/stopping service {1} failed with error: {2}'.format(storage_driver.guid, service, ex))
                    errors_found = True

            if storage_drivers_left is False:
                try:
                    if ServiceManager.has_service(albaproxy_service, client=client):
                        logger.info('Remove Storage Driver - Guid {0} - Starting Alba proxy'.format(storage_driver.guid))
                        ServiceManager.start_service(albaproxy_service, client=client)
                        tries = 10
                        running = False
                        port = storage_driver.alba_proxy.service.ports[0]
                        while running is False and tries > 0:
                            logger.info('Remove Storage Driver - Guid {0} - Waiting for the Alba proxy to start up'.format(storage_driver.guid))
                            tries -= 1
                            time.sleep(10 - tries)
                            try:
                                client.run('alba proxy-statistics --host 127.0.0.1 --port {0}'.format(port))
                                running = True
                            except CalledProcessError as ex:
                                logger.info('Remove Storage Driver - Guid {0} - Fetching alba proxy-statistics failed with error (but ignoring): {1}'.format(storage_driver.guid, ex))
                        if running is False:
                            raise RuntimeError('Alba proxy failed to start')
                        logger.info('Remove Storage Driver - Guid {0} - Alba proxy running'.format(storage_driver.guid))

                    logger.info('Remove Storage Driver - Guid {0} - Destroying filesystem and erasing node configs'.format(storage_driver.guid))
                    with Remote(client.ip, [LocalStorageRouterClient], username='root') as remote:
                        path = 'etcd://127.0.0.1:2379/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, storage_driver.storagedriver_id)
                        storagedriver_client = remote.LocalStorageRouterClient(path)
                        try:
                            storagedriver_client.destroy_filesystem()
                        except RuntimeError as rte:
                            # If backend has already been deleted, we cannot delete the filesystem anymore --> storage leak!!!
                            # @TODO: Find better way for catching this error
                            if 'MasterLookupResult.Error' not in rte.message:
                                raise

                    # noinspection PyArgumentList
                    vrouter_clusterregistry.erase_node_configs()
                except RuntimeError as ex:
                    logger.error('Remove Storage Driver - Guid {0} - Destroying filesystem and erasing node configs failed with error: {1}'.format(storage_driver.guid, ex))
                    errors_found = True
                try:
                    if ServiceManager.has_service(albaproxy_service, client=client):
                        logger.info('Remove Storage Driver - Guid {0} - Stopping service {1}'.format(storage_driver.guid, albaproxy_service))
                        ServiceManager.stop_service(albaproxy_service, client=client)
                        logger.info('Remove Storage Driver - Guid {0} - Removing service {1}'.format(storage_driver.guid, albaproxy_service))
                        ServiceManager.remove_service(albaproxy_service, client=client)
                except Exception as ex:
                    logger.error('Remove Storage Driver - Guid {0} - Disabling/stopping service {1} failed with error: {2}'.format(storage_driver.guid, albaproxy_service, ex))
                    errors_found = True

            # Clean up vPool on KVM host
            if storage_router.pmachine.hvtype == 'KVM':
                logger.info('Remove Storage Driver - Guid {0} - Removing vPool from KVM host'.format(storage_driver.guid))
                # 'Name                 State      Autostart '
                # '-------------------------------------------'
                # ' vpool1               active     yes'
                # ' vpool2               active     no'
                command = 'virsh pool-list --all'
                logger.info('Remove Storage Driver - Guid {0} - Removing vPool from KVM host - Executing command: {1}'.format(storage_driver.guid, command))
                vpool_overview = client.run(command).splitlines()
                vpool_overview.pop(1)  # Pop   ---------------
                vpool_overview.pop(0)  # Pop   Name   State   Autostart
                for vpool_info in vpool_overview:
                    vpool_name = vpool_info.split()[0].strip()
                    if vpool.name == vpool_name:
                        try:
                            command = 'virsh pool-destroy {0}'.format(vpool.name)
                            logger.info('Remove Storage Driver - Guid {0} - Removing vPool from KVM host - Executing command: {1}'.format(storage_driver.guid, command))
                            client.run(command)
                        except Exception as ex:
                            logger.error('Remove Storage Driver - Guid {0} - Removing vPool from KVM host - Destroying vPool failed with error: {1}'.format(storage_driver.guid, ex))
                            errors_found = True
                        try:
                            command = 'virsh pool-undefine {0}'.format(vpool.name)
                            logger.info('Remove Storage Driver - Guid {0} - Removing vPool from KVM host - Executing command: {1}'.format(storage_driver.guid, command))
                            client.run(command)
                        except Exception as ex:
                            logger.error('Remove Storage Driver - Guid {0} - Removing vPool from KVM host - Undefine vPool failed with error: {1}'.format(storage_driver.guid, ex))
                            errors_found = True
                        break

        # Reconfigure volumedriver arakoon cluster
        try:
            if storage_drivers_left is True:
                logger.info('Remove Storage Driver - Guid {0} - Reconfiguring volumedriver arakoon cluster'.format(storage_driver.guid))
                node_configs = []
                for sd in vpool.storagedrivers:
                    if sd != storage_driver and sd.storagerouter not in storage_routers_offline:
                        node_configs.append(ClusterNodeConfig(str(sd.storagedriver_id),
                                                              str(sd.cluster_ip),
                                                              sd.ports[0],
                                                              sd.ports[1],
                                                              sd.ports[2]))
                logger.info('Remove Storage Driver - Guid {0} - Node configs - \n{1}'.format(storage_driver.guid, '\n'.join([str(config) for config in node_configs])))
                vrouter_clusterregistry.set_node_configs(node_configs)
                srclient = StorageDriverClient.load(vpool)
                for sd in vpool.storagedrivers:
                    if sd != storage_driver and sd.storagerouter not in storage_routers_offline:
                        logger.info('Remove Storage Driver - Guid {0} - Storage Driver {1} {2} - Updating cluster node configs'.format(storage_driver.guid, sd.guid, sd.name))
                        srclient.update_cluster_node_configs(str(sd.storagedriver_id))
        except Exception as ex:
            logger.error('Remove Storage Driver - Guid {0} - Reconfiguring volumedriver arakoon cluster failed with error: {1}'.format(storage_driver.guid, ex))
            errors_found = True

        # Removing MDS services
        logger.info('Remove Storage Driver - Guid {0} - Removing MDS services'.format(storage_driver.guid))
        for mds_service in mds_services_to_remove:
            # All MDSServiceVDisk object should have been deleted above
            try:
                logger.info('Remove Storage Driver - Guid {0} - Remove MDS service (number {1}) for Storage Router with IP {2}'.format(storage_driver.guid, mds_service.number, storage_router.ip))
                MDSServiceController.remove_mds_service(mds_service=mds_service,
                                                        vpool=vpool,
                                                        reconfigure=False,
                                                        allow_offline=not storage_router_online)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Removing MDS service failed with error: {1}'.format(storage_driver.guid, ex))
                errors_found = True

        # Clean up directories and files
        dirs_to_remove = []
        for sd_partition in storage_driver.partitions:
            dirs_to_remove.append(sd_partition.path)
            sd_partition.delete()

        if storage_router_online is True:
            # Cleanup directories/files
            logger.info('Remove Storage Driver - Guid {0} - Deleting vPool related directories and files'.format(storage_driver.guid))
            machine_id = System.get_my_machine_id(client)
            dirs_to_remove.append(storage_driver.mountpoint)
            dirs_to_remove.append('{0}/{1}'.format(EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|rsp'.format(machine_id)), vpool.name))

            files_to_remove = []
            if vpool.backend_type.code == 'alba':
                config_tree = '/ovs/vpools/{0}/proxies/{1}'.format(vpool.guid, storage_driver.alba_proxy.guid)
                EtcdConfiguration.delete(config_tree)
            if storage_router.pmachine.hvtype == 'VMWARE' and EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|vmware_mode'.format(machine_id)) == 'ganesha':
                files_to_remove.append('{0}/storagedriver/storagedriver/{1}_ganesha.conf'.format(EtcdConfiguration.get('/ovs/framework/paths|cfgdir'), vpool.name))

            for file_name in files_to_remove:
                try:
                    if file_name and client.file_exists(file_name):
                        client.file_delete(file_name)
                        logger.info('Remove Storage Driver - Guid {0} - Removed file {1} on Storage Router with IP {2}'.format(storage_driver.guid, file_name, client.ip))
                except Exception as ex:
                    logger.error('Remove Storage Driver - Guid {0} - Removing file {1} failed with error: {2}'.format(storage_driver.guid, file_name, ex))
                    errors_found = True

            try:
                mountpoints = StorageRouterController._get_mountpoints(client)
                for dir_name in dirs_to_remove:
                    if dir_name and client.dir_exists(dir_name) and dir_name not in mountpoints and dir_name != '/':
                        client.dir_delete(dir_name)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Failed to retrieve mountpoint information or delete directories, error: {1}'.format(storage_driver.guid, ex))
                errors_found = True

            logger.info('Remove Storage Driver - Guid {0} - Synchronizing disks with reality'.format(storage_driver.guid))
            try:
                DiskController.sync_with_reality(storage_router.guid)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Synchronizing disks with reality failed with error: {1}'.format(storage_driver.guid, ex))
                errors_found = True

        EtcdConfiguration.delete('/ovs/vpools/{0}/hosts/{1}'.format(vpool.guid, storage_driver.storagedriver_id))

        # Model cleanup
        logger.info('Remove Storage Driver - Guid {0} - Cleaning up model'.format(storage_driver.guid))
        if storage_driver.alba_proxy is not None:
            logger.info('Remove Storage Driver - Guid {0} - Removing alba proxy service from model'.format(storage_driver.guid))
            service = storage_driver.alba_proxy.service
            storage_driver.alba_proxy.delete()
            service.delete()
        storage_driver.delete(abandon=['logs'])  # Detach from the log entries

        if storage_drivers_left is False:
            EtcdConfiguration.delete('/ovs/vpools/{0}'.format(vpool.guid))
            try:
                logger.info('Remove Storage Driver - Guid {0} - Removing virtual disks from model'.format(storage_driver.guid))
                for vdisk in vpool.vdisks:
                    for junction in vdisk.mds_services:
                        junction.delete()
                    vdisk.delete()
                logger.info('Remove Storage Driver - Guid {0} - Removing vPool from model'.format(storage_driver.guid))
                vpool.delete()
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Cleaning up vdisks from the model failed with error: {1}'.format(storage_driver.guid, ex))
                errors_found = True
                vpool.status = VPool.STATUSES.FAILURE
                vpool.save()
        else:
            if storage_router.guid in vpool.metadata:
                vpool.metadata.pop(storage_router.guid)
                vpool.save()
            logger.info('Remove Storage Driver - Guid {0} - Checking DTL for all virtual disks in vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - DTL checkup failed for vPool {1} with guid {2} with error: {3}'.format(storage_driver.guid, vpool.name, vpool.guid, ex))

        logger.info('Remove Storage Driver - Guid {0} - Running MDS checkup'.format(storage_driver.guid))
        try:
            MDSServiceController.mds_checkup()
        except Exception as ex:
            logger.error('Remove Storage Driver - Guid {0} - MDS checkup failed with error: {1}'.format(storage_driver.guid, ex))

        if errors_found is True:
            if storage_drivers_left is True:
                vpool.status = VPool.STATUSES.FAILURE
                vpool.save()
            raise RuntimeError('1 or more errors occurred while trying to remove the storage driver. Please check /var/log/ovs/lib.log for more information')
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_version_info')
    def get_version_info(storagerouter_guid):
        """
        Returns version information regarding a given StorageRouter
        :param storagerouter_guid: Storage Router guid to get version information for
        :type storagerouter_guid: str

        :return: Version information
        :rtype: dict
        """
        return {'storagerouter_guid': storagerouter_guid,
                'versions': PackageManager.get_versions()}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_support_info')
    def get_support_info(storagerouter_guid):
        """
        Returns support information regarding a given StorageRouter
        :param storagerouter_guid: Storage Router guid to get support information for
        :type storagerouter_guid: str

        :return: Support information
        :rtype: dict
        """
        return {'storagerouter_guid': storagerouter_guid,
                'nodeid': System.get_my_machine_id(),
                'clusterid': EtcdConfiguration.get('/ovs/framework/cluster_id'),
                'enabled': EtcdConfiguration.get('/ovs/framework/support|enabled'),
                'enablesupport': EtcdConfiguration.get('ovs/framework/support|enablesupport')}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_support_metadata')
    def get_support_metadata():
        """
        Returns support metadata for a given storagerouter. This should be a routed task!
        """
        return SupportAgent().get_heartbeat_data()

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_logfiles')
    def get_logfiles(local_storagerouter_guid):
        """
        Collects logs, moves them to a web-accessible location and returns log tgz's filename
        :param local_storagerouter_guid: Storage Router guid to retrieve log files on
        :type local_storagerouter_guid: str

        :return: Name of tgz containing the logs
        :rtype: str
        """
        this_client = SSHClient('127.0.0.1', username='root')
        logfile = this_client.run('ovs collect logs').strip()
        logfilename = logfile.split('/')[-1]

        storagerouter = StorageRouter(local_storagerouter_guid)
        webpath = '/opt/OpenvStorage/webapps/frontend/downloads'
        client = SSHClient(storagerouter, username='root')
        client.dir_create(webpath)
        client.file_upload('{0}/{1}'.format(webpath, logfilename), logfile)
        client.run('chmod 666 {0}/{1}'.format(webpath, logfilename))
        return logfilename

    @staticmethod
    @celery.task(name='ovs.storagerouter.configure_support')
    def configure_support(enable, enable_support):
        """
        Configures support on all StorageRouters
        :param enable: If True support agent will be enabled and started, else disabled and stopped
        :type enable: bool

        :param enable_support: If False openvpn will be stopped
        :type enable_support: bool

        :return: True
        :rtype: bool
        """
        clients = []
        try:
            for storagerouter in StorageRouterList.get_storagerouters():
                clients.append((SSHClient(storagerouter), SSHClient(storagerouter, username='root')))
        except UnableToConnectException:
            raise RuntimeError('Not all StorageRouters are reachable')
        EtcdConfiguration.set('/ovs/framework/support|enabled', enable)
        EtcdConfiguration.set('/ovs/framework/support|enablesupport', enable_support)
        for ovs_client, root_client in clients:
            if enable_support is False:
                root_client.run('service openvpn stop')
                root_client.file_delete('/etc/openvpn/ovs_*')
            if enable is True:
                if not ServiceManager.has_service(StorageRouterController.SUPPORT_AGENT, client=root_client):
                    ServiceManager.add_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                    ServiceManager.enable_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                ServiceManager.restart_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
            else:
                if ServiceManager.has_service(StorageRouterController.SUPPORT_AGENT, client=root_client):
                    ServiceManager.stop_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                    ServiceManager.remove_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
        return True

    @staticmethod
    @celery.task(name='ovs.storagerouter.check_s3')
    def check_s3(host, port, accesskey, secretkey):
        """
        Validates whether connection to a given S3 backend can be made
        :param host: Host to check
        :type host: str

        :param port: Port on which to check
        :type port: int

        :param accesskey: Access key to be used for connection
        :type accesskey: str

        :param secretkey: Secret key to be used for connection
        :type secretkey: str

        :return: True if check was successful, False otherwise
        :rtype: bool
        """
        try:
            import boto
            import boto.s3.connection
            backend = boto.connect_s3(aws_access_key_id=accesskey,
                                      aws_secret_access_key=secretkey,
                                      port=port,
                                      host=host,
                                      is_secure=(port == 443),
                                      calling_format=boto.s3.connection.OrdinaryCallingFormat())
            backend.get_all_buckets()
            return True
        except Exception as ex:
            logger.exception('Error during S3 check: {0}'.format(ex))
            return False

    @staticmethod
    @celery.task(name='ovs.storagerouter.check_mtpt')
    def check_mtpt(name):
        """
        Checks whether a given mountpoint for vPool is in use
        :param name: Name of the mountpoint to check
        :type name: str

        :return: True if mountpoint not in use else False
        :rtype: bool
        """
        mountpoint = '/mnt/{0}'.format(name)
        if not os.path.exists(mountpoint):
            return True
        return check_output('sudo -s ls -al {0} | wc -l'.format(mountpoint), shell=True).strip() == '3'

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_update_status')
    def get_update_status(storagerouter_ip):
        """
        Checks for new updates
        :param storagerouter_ip: IP of the Storage Router to check for updates
        :type storagerouter_ip: str

        :return: Update status for specified storage router
        :rtype: dict
        """
        # Check plugin requirements
        root_client = SSHClient(storagerouter_ip,
                                username='root')
        required_plugin_params = {'name': (str, None),             # Name of a subpart of the plugin and is used for translation in html. Eg: alba:packages.SDM
                                  'version': (str, None),          # Available version to be installed
                                  'namespace': (str, None),        # Name of the plugin and is used for translation in html. Eg: ALBA:packages.sdm
                                  'services': (list, str),         # Services which the plugin depends upon and should be stopped during update
                                  'packages': (list, str),         # Packages which contain the plugin code and should be updated
                                  'downtime': (list, tuple),       # Information about crucial services which will go down during the update
                                  'prerequisites': (list, tuple)}  # Information about prerequisites which are unmet (eg running vms for storage driver update)
        package_map = {}
        plugin_functions = Toolbox.fetch_hooks('update', 'metadata')
        for function in plugin_functions:
            output = function(root_client)
            if not isinstance(output, dict):
                raise ValueError('Update cannot continue. Failed to retrieve correct plugin information ({0})'.format(function.func_name))

            for key, value in output.iteritems():
                for out in value:
                    Toolbox.verify_required_params(required_plugin_params, out)
                if key not in package_map:
                    package_map[key] = []
                package_map[key] += value

        # Update apt (only our ovs apt repo)
        PackageManager.update(client=root_client)

        # Compare installed and candidate versions
        return_value = {'upgrade_ongoing': os.path.exists('/etc/upgrade_ongoing')}
        for gui_name, package_information in package_map.iteritems():
            return_value[gui_name] = []
            for package_info in package_information:
                version = package_info['version']
                if version:
                    gui_down = 'watcher-framework' in package_info['services'] or 'nginx' in package_info['services']
                    info_added = False
                    for index, item in enumerate(return_value[gui_name]):
                        if item['name'] == package_info['name']:
                            return_value[gui_name][index]['downtime'].extend(package_info['downtime'])
                            info_added = True
                            if gui_down is True and return_value[gui_name][index]['gui_down'] is False:
                                return_value[gui_name][index]['gui_down'] = True
                    if info_added is False:  # Some plugins can have same package dependencies as core and we only want to show each package once in GUI (Eg: Arakoon for core and ALBA)
                        return_value[gui_name].append({'to': version,
                                                       'name': package_info['name'],
                                                       'gui_down': gui_down,
                                                       'downtime': package_info['downtime'],
                                                       'namespace': package_info['namespace'],
                                                       'prerequisites': package_info['prerequisites']})
        return return_value

    @staticmethod
    @add_hooks('update', 'metadata')
    def get_metadata_framework(client):
        """
        Retrieve packages and services on which the framework depends
        :param client: SSHClient on which to retrieve the metadata
        :type client: SSHClient

        :return: List of dictionaries which contain services to restart,
                                                    packages to update,
                                                    information about potential downtime
                                                    information about unmet prerequisites
        :rtype: list
        """
        this_sr = StorageRouterList.get_by_ip(client.ip)
        srs = StorageRouterList.get_storagerouters()
        downtime = []
        fwk_cluster_name = EtcdConfiguration.get('/ovs/framework/arakoon_clusters|ovsdb')
        metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=fwk_cluster_name)
        if metadata is None:
            raise ValueError('Expected exactly 1 arakoon cluster of type {0}, found None'.format(ServiceType.ARAKOON_CLUSTER_TYPES.FWK))

        if metadata.internal is True:
            ovsdb_cluster = [ser.storagerouter_guid for sr in srs for ser in sr.services if ser.type.name == ServiceType.SERVICE_TYPES.ARAKOON and ser.name == 'arakoon-ovsdb']
            downtime = [('ovs', 'ovsdb', None)] if len(ovsdb_cluster) < 3 and this_sr.guid in ovsdb_cluster else []

        ovs_info = PackageManager.verify_update_required(packages=['openvstorage-core', 'openvstorage-webapps', 'openvstorage-cinder-plugin'],
                                                         services=['watcher-framework', 'memcached'],
                                                         client=client)
        arakoon_info = PackageManager.verify_update_required(packages=['arakoon'],
                                                             services=['arakoon-ovsdb'],
                                                             client=client)

        return {'framework': [{'name': 'ovs',
                               'version': ovs_info['version'],
                               'services': ovs_info['services'],
                               'packages': ovs_info['packages'],
                               'downtime': [],
                               'namespace': 'ovs',
                               'prerequisites': []},
                              {'name': 'arakoon',
                               'version': arakoon_info['version'],
                               'services': arakoon_info['services'],
                               'packages': arakoon_info['packages'],
                               'downtime': downtime,
                               'namespace': 'ovs',
                               'prerequisites': []}]}

    @staticmethod
    @add_hooks('update', 'metadata')
    def get_metadata_volumedriver(client):
        """
        Retrieve packages and services on which the volumedriver depends
        :param client: SSHClient on which to retrieve the metadata
        :type client: SSHClient

        :return: List of dictionaries which contain services to restart,
                                                    packages to update,
                                                    information about potential downtime
                                                    information about unmet prerequisites
        :rtype: list
        """
        running_vms = False
        for vpool in VPoolList.get_vpools():
            for vdisk in vpool.vdisks:
                if vdisk.vmachine_guid is None:
                    continue
                if vdisk.vmachine.hypervisor_status in ['RUNNING', 'PAUSED']:
                    running_vms = True
                    break
            if running_vms is True:
                break

        srs = StorageRouterList.get_storagerouters()
        this_sr = StorageRouterList.get_by_ip(client.ip)
        downtime = []
        sd_cluster_name = EtcdConfiguration.get('/ovs/framework/arakoon_clusters|voldrv')
        metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=sd_cluster_name)
        if metadata is None:
            raise ValueError('Expected exactly 1 arakoon cluster of type {0}, found None'.format(ServiceType.ARAKOON_CLUSTER_TYPES.SD))

        if metadata.internal is True:
            voldrv_cluster = [ser.storagerouter_guid for sr in srs for ser in sr.services if ser.type.name == ServiceType.SERVICE_TYPES.ARAKOON and ser.name == 'arakoon-voldrv']
            downtime = [('ovs', 'voldrv', None)] if len(voldrv_cluster) < 3 and this_sr.guid in voldrv_cluster else []

        alba_proxies = []
        alba_downtime = []
        for sr in srs:
            for service in sr.services:
                if service.type.name == ServiceType.SERVICE_TYPES.ALBA_PROXY and service.storagerouter_guid == this_sr.guid:
                    alba_proxies.append(service.alba_proxy)
                    alba_downtime.append(('ovs', 'proxy', service.alba_proxy.storagedriver.vpool.name))

        prerequisites = [('ovs', 'vmachine', None)] if running_vms is True else []
        volumedriver_services = ['ovs-volumedriver_{0}'.format(sd.vpool.name)
                                 for sd in this_sr.storagedrivers]
        volumedriver_services.extend(['ovs-dtl_{0}'.format(sd.vpool.name)
                                      for sd in this_sr.storagedrivers])
        voldrv_info = PackageManager.verify_update_required(packages=['volumedriver-base', 'volumedriver-server'],
                                                            services=volumedriver_services,
                                                            client=client)
        alba_info = PackageManager.verify_update_required(packages=['alba'],
                                                          services=[service.service.name for service in alba_proxies],
                                                          client=client)
        arakoon_info = PackageManager.verify_update_required(packages=['arakoon'],
                                                             services=['arakoon-voldrv'],
                                                             client=client)

        return {'volumedriver': [{'name': 'volumedriver',
                                  'version': voldrv_info['version'],
                                  'services': voldrv_info['services'],
                                  'packages': voldrv_info['packages'],
                                  'downtime': alba_downtime,
                                  'namespace': 'ovs',
                                  'prerequisites': prerequisites},
                                 {'name': 'alba',
                                  'version': alba_info['version'],
                                  'services': alba_info['services'],
                                  'packages': alba_info['packages'],
                                  'downtime': alba_downtime,
                                  'namespace': 'ovs',
                                  'prerequisites': prerequisites},
                                 {'name': 'arakoon',
                                  'version': arakoon_info['version'],
                                  'services': arakoon_info['services'],
                                  'packages': arakoon_info['packages'],
                                  'downtime': downtime,
                                  'namespace': 'ovs',
                                  'prerequisites': []}]}

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_framework')
    def update_framework(storagerouter_ip):
        """
        Launch the update_framework method in setup.py
        :param storagerouter_ip: IP of the Storage Router to update the framework packages on
        :type storagerouter_ip: str

        :return: None
        """
        root_client = SSHClient(storagerouter_ip,
                                username='root')
        root_client.run('ovs update framework')

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_volumedriver')
    def update_volumedriver(storagerouter_ip):
        """
        Launch the update_volumedriver method in setup.py
        :param storagerouter_ip: IP of the Storage Router to update the volumedriver packages on
        :type storagerouter_ip: str

        :return: None
        """
        root_client = SSHClient(storagerouter_ip,
                                username='root')
        root_client.run('ovs update volumedriver')

    @staticmethod
    @celery.task(name='ovs.storagerouter.refresh_hardware')
    def refresh_hardware(storagerouter_guid):
        """
        Refreshes all hardware related information
        :param storagerouter_guid: Guid of the Storage Router to refresh the hardware on
        :type storagerouter_guid: str

        :return: None
        """
        StorageRouterController.set_rdma_capability(storagerouter_guid)
        DiskController.sync_with_reality(storagerouter_guid)

    @staticmethod
    def set_rdma_capability(storagerouter_guid):
        """
        Check if the Storage Router has been reconfigured to be able to support RDMA
        :param storagerouter_guid: Guid of the Storage Router to check and set
        :type storagerouter_guid: str

        :return: None
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter, username='root')
        rdma_capable = False
        with Remote(client.ip, [os], username='root') as remote:
            for root, dirs, files in remote.os.walk('/sys/class/infiniband'):
                for directory in dirs:
                    ports_dir = '/'.join([root, directory, 'ports'])
                    if not remote.os.path.exists(ports_dir):
                        continue
                    for sub_root, sub_dirs, _ in remote.os.walk(ports_dir):
                        if sub_root != ports_dir:
                            continue
                        for sub_directory in sub_dirs:
                            state_file = '/'.join([sub_root, sub_directory, 'state'])
                            if remote.os.path.exists(state_file):
                                if 'ACTIVE' in client.run('cat {0}'.format(state_file)):
                                    rdma_capable = True
        storagerouter.rdma_capable = rdma_capable
        storagerouter.save()

    @staticmethod
    @celery.task(name='ovs.storagerouter.configure_disk')
    @ensure_single(task_name='ovs.storagerouter.configure_disk', mode='CHAINED', global_timeout=1800)
    def configure_disk(storagerouter_guid, disk_guid, partition_guid, offset, size, roles):
        """
        Configures a partition
        :param storagerouter_guid: Guid of the Storage Router to configure a disk on
        :type storagerouter_guid: str

        :param disk_guid: Guid of the disk to configure
        :type disk_guid: str

        :param partition_guid: Guid of the partition on the disk
        :type partition_guid: str

        :param offset: Offset for the partition
        :type offset: int

        :param size: Size of the partition
        :type size: int

        :param roles: Roles assigned to the partition
        :type roles: list

        :return: None
        """
        storagerouter = StorageRouter(storagerouter_guid)
        for role in roles:
            if role not in DiskPartition.ROLES or role == DiskPartition.ROLES.BACKEND:
                raise RuntimeError('Invalid role specified: {0}'.format(role))
        DiskController.sync_with_reality(storagerouter_guid)
        disk = Disk(disk_guid)
        if disk.storagerouter_guid != storagerouter_guid:
            raise RuntimeError('The given Disk is not on the given StorageRouter')
        if partition_guid is None:
            logger.debug('Creating new partition - Offset: {0} bytes - Size: {1} bytes - Roles: {2}'.format(offset, size, roles))
            with Remote(storagerouter.ip, [DiskTools], username='root') as remote:
                remote.DiskTools.create_partition(disk_path=disk.path,
                                                  disk_size=disk.size,
                                                  partition_start=offset,
                                                  partition_size=size)
            DiskController.sync_with_reality(storagerouter_guid)
            disk = Disk(disk_guid)
            end_point = offset + size
            partition = None
            for part in disk.partitions:
                if offset < part.offset + part.size and end_point > part.offset:
                    partition = part
                    break

            if partition is None:
                raise RuntimeError('No new partition detected on disk {0} after having created 1'.format(disk.name))
            logger.debug('Partition created')
        else:
            logger.debug('Using existing partition')
            partition = DiskPartition(partition_guid)
            if partition.disk_guid != disk_guid:
                raise RuntimeError('The given DiskPartition is not on the given Disk')
        if partition.filesystem is None or partition_guid is None:
            logger.debug('Creating filesystem')
            with Remote(storagerouter.ip, [DiskTools], username='root') as remote:
                remote.DiskTools.make_fs(partition.path)
                DiskController.sync_with_reality(storagerouter_guid)
                partition = DiskPartition(partition.guid)
                if partition.filesystem not in ['ext4', 'xfs']:
                    raise RuntimeError('Unexpected filesystem')
            logger.debug('Filesystem created')
        if partition.mountpoint is None:
            logger.debug('Configuring mountpoint')
            with Remote(storagerouter.ip, [DiskTools], username='root') as remote:
                counter = 1
                mountpoint = None
                while True:
                    mountpoint = '/mnt/{0}{1}'.format('ssd' if disk.is_ssd else 'hdd', counter)
                    counter += 1
                    if not remote.DiskTools.mountpoint_exists(mountpoint):
                        break
                logger.debug('Found mountpoint: {0}'.format(mountpoint))
                remote.DiskTools.add_fstab(partition.path, mountpoint, partition.filesystem)
                remote.DiskTools.mount(mountpoint)
                DiskController.sync_with_reality(storagerouter_guid)
                partition = DiskPartition(partition.guid)
                if partition.mountpoint != mountpoint:
                    raise RuntimeError('Unexpected mountpoint')
            logger.debug('Mountpoint configured')
        partition.roles = roles
        partition.save()
        logger.debug('Partition configured')

    @staticmethod
    def _get_free_ports(client, ports_in_use, number):
        """
        Gets `number` free ports ports that are not in use and not reserved
        """
        machine_id = System.get_my_machine_id(client)
        port_range = EtcdConfiguration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
        ports = System.get_free_ports(port_range, ports_in_use, number, client)

        return ports if number != 1 else ports[0]

    @staticmethod
    def _check_scrub_partition_present():
        """
        Checks whether at least 1 scrub partition is present on any Storage Router
        :return: boolean
        """
        for storage_router in StorageRouterList.get_storagerouters():
            for disk in storage_router.disks:
                for partition in disk.partitions:
                    if DiskPartition.ROLES.SCRUB in partition.roles:
                        return True
        return False

    @staticmethod
    def _get_mountpoints(client):
        """
        Retrieve the mountpoints
        :param client: SSHClient to retrieve the mountpoints on
        :return: List of mountpoints
        """
        mountpoints = []
        for mountpoint in client.run('mount -v').strip().splitlines():
            mp = mountpoint.split(' ')[2] if len(mountpoint.split(' ')) > 2 else None
            if mp and not mp.startswith('/dev') and not mp.startswith('/proc') and not mp.startswith('/sys') and not mp.startswith('/run') and not mp.startswith('/mnt/alba-asd') and mp != '/':
                mountpoints.append(mp)
        return mountpoints

    @staticmethod
    def _retrieve_alba_connection_info(backend_info):
        """
        Retrieve the backend connection information
        :param backend_info: ALBA backend connection information
        :type backend_info: dict

        :return: ALBA backend connection information
        :rtype: dict
        """
        connection_host = backend_info['host']
        if connection_host == '':
            clients = ClientList.get_by_types('INTERNAL', 'CLIENT_CREDENTIALS')
            oauth_client = None
            for current_client in clients:
                if current_client.user.group.name == 'administrators':
                    oauth_client = current_client
                    break
            if oauth_client is None:
                raise RuntimeError('Could not find INTERNAL CLIENT_CREDENTIALS client in administrator group.')

            local = True
            connection_host = StorageRouterList.get_masters()[0].ip
            connection_port = 443
            connection_username = oauth_client.client_id
            connection_password = oauth_client.client_secret
        else:
            local = False
            connection_port = backend_info['port']
            connection_username = backend_info['username']
            connection_password = backend_info['password']

        return {'host': connection_host,
                'port': connection_port,
                'client_id': connection_username,
                'client_secret': connection_password,
                'local': local}

    @staticmethod
    def _retrieve_alba_arakoon_config(backend_guid, ovs_client):
        """
        Retrieve the ALBA Arakoon configuration
        :param backend_guid: Guid of the ALBA backend
        :type backend_guid: str

        :param ovs_client: OVS client object
        :type ovs_client: OVSClient

        :return: Arakoon configuration information
        :rtype: dict
        """
        task_id = ovs_client.get('/alba/backends/{0}/get_config_metadata'.format(backend_guid))
        successful, arakoon_config = ovs_client.wait_for_task(task_id, timeout=300)
        if successful is False:
            raise RuntimeError('Could not load metadata from environment {0}'.format(ovs_client.ip))
        return arakoon_config

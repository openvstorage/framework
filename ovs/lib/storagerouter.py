# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
import re
import copy
import uuid
import json
import time
import random
from ConfigParser import RawConfigParser
from subprocess import check_output, CalledProcessError

from ovs.celery_run import celery
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_albaproxy import AlbaProxy
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service as DalService
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
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.disk import DiskTools
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration, StorageDriverClient
from ovs.extensions.support.agent import SupportAgent
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import add_hooks
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
    ARAKOON_CLUSTER_ID_VOLDRV = 'voldrv'
    SUPPORT_AGENT = 'support-agent'
    PARTITION_DEFAULT_USAGES = {DiskPartition.ROLES.DB: (40, 20),  # 1st number is exact size in GiB, 2nd number is percentage (highest of the 2 will be taken)
                                DiskPartition.ROLES.SCRUB: (0, 0)}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_metadata')
    def get_metadata(storagerouter_guid):
        """
        Gets physical information about the machine this task is running on
        :param storagerouter_guid: Storage Router guid to retrieve the metadata for
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
                for storagedriver_partition in disk_partition.storagedrivers:
                    claimed_space += storagedriver_partition.size if storagedriver_partition.size is not None else 0

                shared = False
                for role in disk_partition.roles:
                    size = disk_partition.size if disk_partition.size is not None else 0
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

        for service in ServiceTypeList.get_by_name('Arakoon').services:
            if service.name == 'arakoon-ovsdb':
                continue
            for partition in partitions[DiskPartition.ROLES.DB]:
                if service.storagerouter_guid == partition['storagerouter_guid']:
                    partition['in_use'] = True
        for service in ServiceTypeList.get_by_name('MetadataServer').services:
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
        """
        sd_config_params = (dict, {'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                   'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                   'dedupe_mode': (str, StorageDriverClient.VPOOL_DEDUPE_MAP.keys()),
                                   'write_buffer': (int, {'min': 128, 'max': 10240}),
                                   'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys()),
                                   'cache_strategy': (str, StorageDriverClient.VPOOL_CACHE_MAP.keys())})
        required_params = {'vpool_name': (str, Toolbox.regex_vpool),
                           'storage_ip': (str, Toolbox.regex_ip),
                           'storagerouter_ip': (str, Toolbox.regex_ip),
                           'integratemgmt': (bool, None),
                           'readcache_size': (int, {'min': 1, 'max': 10240}),
                           'writecache_size': (int, {'min': 1, 'max': 10240})}
        required_params_for_new_vpool = {'type': (str, ['local', 'distributed', 'alba', 'ceph_s3', 'amazon_s3', 'swift_s3']),
                                         'config_params': sd_config_params,
                                         'connection_host': (str, Toolbox.regex_ip, False),
                                         'connection_port': (int, None),
                                         'connection_backend': (dict, None),
                                         'connection_username': (str, None),
                                         'connection_password': (str, None)}
        required_params_for_new_distributed_vpool = {'type': (str, ['local', 'distributed', 'alba', 'ceph_s3', 'amazon_s3', 'swift_s3']),
                                                     'config_params': sd_config_params}

        ###############
        # VALIDATIONS #
        ###############
        ip = parameters['storagerouter_ip']
        vpool_name = parameters['vpool_name']

        client = SSHClient(ip)
        unique_id = System.get_my_machine_id(client)

        # 1. Check parameters
        if not isinstance(parameters, dict):
            raise ValueError('Parameters should be of type "dict"')
        Toolbox.verify_required_params(required_params, parameters)

        # 2. Check vPool name validity
        vpool = VPoolList.get_vpool_by_name(vpool_name)
        name_regex = "^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$"
        if not re.match(name_regex, vpool_name):
            raise RuntimeError("Invalid name for vpool")

        # 3. Check parameters for new vPool
        backend_type = BackendTypeList.get_backend_type_by_code(parameters['type'])
        if vpool is None:
            sco_size = parameters['config_params']['sco_size']
            write_buffer = parameters['config_params']['write_buffer']
            if (sco_size == 128 and write_buffer < 256) or not (128 <= write_buffer <= 10240):
                raise ValueError('Incorrect storagedriver configuration settings specified')

            if parameters['type'] in ['local', 'distributed']:
                Toolbox.verify_required_params(required_params_for_new_distributed_vpool, parameters)
            else:
                Toolbox.verify_required_params(required_params_for_new_vpool, parameters)

        # 4. Check backend type existence
        if backend_type.code not in ['alba', 'distributed', 'ceph_s3', 'amazon_s3', 'swift_s3', 'local']:
            raise ValueError('Unsupported backend type specified: "{0}"'.format(backend_type.code))

        # 5. Check storagerouter existence
        storagerouter = None
        for current_storagerouter in StorageRouterList.get_storagerouters():
            if current_storagerouter.ip == ip and current_storagerouter.machine_id == unique_id:
                storagerouter = current_storagerouter
                break
        if storagerouter is None:
            raise RuntimeError('Could not find Storage Router with given IP address')

        # 6. Check duplicate vPool name
        storagedriver = None
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
                    # The vPool is already added to this Storage Router and this might be a cleanup/recovery
                    storagedriver = vpool_storagedriver
            all_storagerouters += [sd.storagerouter for sd in vpool.storagedrivers]

        # 7. Check storagerouter connectivity
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

        # 8. Check over-allocation for read, write cache
        metadata = StorageRouterController.get_metadata(storagerouter.guid)
        shared_size_available = metadata['shared_size']
        readcache_size_available = metadata['readcache_size']
        readcache_size_requested = parameters['readcache_size'] * 1024 ** 3
        writecache_size_available = metadata['writecache_size']
        writecache_size_requested = parameters['writecache_size'] * 1024 ** 3
        if readcache_size_requested > readcache_size_available + shared_size_available:
            raise ValueError('Too much space request for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.READ,
                                                                                                                           (readcache_size_available + shared_size_available) / 1024.0 ** 3,
                                                                                                                           readcache_size_requested / 1024.0 ** 3))
        if writecache_size_requested > writecache_size_available + shared_size_available:
            raise ValueError('Too much space request for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.WRITE,
                                                                                                                           (writecache_size_available + shared_size_available) / 1024.0 ** 3,
                                                                                                                           writecache_size_requested / 1024.0 ** 3))
        if readcache_size_requested + writecache_size_requested > readcache_size_available + writecache_size_available + shared_size_available:
            raise ValueError('Too much space request. Available: {0:.2f} GiB, Requested: {1:.2f} GiB'.format((readcache_size_available + writecache_size_available + shared_size_available) / 1024.0 ** 3,
                                                                                                             (readcache_size_requested + writecache_size_requested) / 1024.0 ** 3))

        # 9. Check partition role presence
        arakoon_service_found = False
        for service in ServiceTypeList.get_by_name('Arakoon').services:
            if service.name == 'arakoon-voldrv':
                arakoon_service_found = True
                break

        error_messages = []
        if StorageRouterController._check_scrub_partition_present() is False:
            error_messages.append('At least 1 Storage Router must have a {0} partition'.format(DiskPartition.ROLES.SCRUB))

        partition_info = metadata['partitions']
        for required_role in [DiskPartition.ROLES.READ, DiskPartition.ROLES.WRITE]:
            if required_role not in partition_info:
                error_messages.append('Missing required partition role {0}'.format(required_role))
            elif len(partition_info[required_role]) == 0:
                error_messages.append('At least 1 {0} partition role is required'.format(required_role))
            else:
                total_available = [part['available'] for part in partition_info[required_role]]
                if total_available == 0:
                    error_messages.append('Not enough available space for {0}'.format(required_role))

        # 10. Check mountpoints are mounted
        for role, part_info in partition_info.iteritems():
            for part in part_info:
                if not os.path.ismount(part['mountpoint']) and part['mountpoint'] != DiskPartition.VIRTUAL_STORAGE_LOCATION:
                    error_messages.append('Mountpoint {0} is not mounted'.format(part['mountpoint']))

        if arakoon_service_found is False and (DiskPartition.ROLES.DB not in partition_info or len(partition_info[DiskPartition.ROLES.DB]) == 0):
            error_messages.append('DB partition role required')

        if error_messages:
            raise ValueError('Errors validating the partition roles:\n - {0}'.format('\n - '.join(set(error_messages))))

        # 11. Check available IP addresses
        ipaddresses = metadata['ipaddresses']
        grid_ip = client.config_read('ovs.grid.ip')
        if grid_ip in ipaddresses:
            ipaddresses.remove(grid_ip)
        if not ipaddresses:
            raise RuntimeError('No available IP addresses found suitable for Storage Router storage IP')

        ###################
        # CREATE SERVICES #
        ###################
        if arakoon_service_found is False:
            StorageDriverController.manual_voldrv_arakoon_checkup()

        root_client = ip_client_map[storagerouter.ip]['root']
        watcher_volumedriver_service = 'watcher-volumedriver'
        if not ServiceManager.has_service(watcher_volumedriver_service, client=root_client):
            ServiceManager.add_service(watcher_volumedriver_service, client=root_client)
            ServiceManager.enable_service(watcher_volumedriver_service, client=root_client)
            ServiceManager.start_service(watcher_volumedriver_service, client=root_client)

        ######################
        # START ADDING VPOOL #
        ######################
        new_vpool = False
        if vpool is None:  # Keep in mind that if the Storage Driver exists, the vPool does as well
            new_vpool = True
            vpool = VPool()
            vpool.backend_type = backend_type
            connection_host = parameters.get('connection_host', '')
            connection_port = parameters.get('connection_port', '')
            connection_username = parameters.get('connection_username', '')
            connection_password = parameters.get('connection_password', '')
            if vpool.backend_type.code in ['local', 'distributed']:
                vpool.metadata = {'backend_type': 'LOCAL'}
            elif vpool.backend_type.code == 'alba':
                if connection_host == '':
                    connection_host = StorageRouterList.get_masters()[0].ip
                    connection_port = 443
                    clients = ClientList.get_by_types('INTERNAL', 'CLIENT_CREDENTIALS')
                    oauth_client = None
                    for current_client in clients:
                        if current_client.user.group.name == 'administrators':
                            oauth_client = current_client
                            break
                    if oauth_client is None:
                        raise RuntimeError('Could not find INTERNAL CLIENT_CREDENTIALS client in administrator group.')
                    ovs_client = OVSClient(connection_host, connection_port,
                                           credentials=(oauth_client.client_id, oauth_client.client_secret),
                                           version=1)
                else:
                    ovs_client = OVSClient(connection_host, connection_port,
                                           credentials=(connection_username, connection_password),
                                           version=1)
                backend_guid = parameters['connection_backend']['backend']
                preset_name = parameters['connection_backend']['metadata']
                backend_info = ovs_client.get('/alba/backends/{0}/'.format(backend_guid), params={'contents': '_dynamics'})
                if preset_name not in [preset['name'] for preset in backend_info['presets']]:
                    raise RuntimeError('Given preset {0} is not available in backend {1}'.format(preset_name, backend_guid))
                task_id = ovs_client.get('/alba/backends/{0}/get_config_metadata'.format(backend_guid))
                successful, metadata = ovs_client.wait_for_task(task_id, timeout=300)
                if successful is False:
                    raise RuntimeError('Could not load metadata from remote environment {0}'.format(connection_host))
                vpool.metadata = {'metadata': metadata,
                                  'preset': preset_name}
            elif vpool.backend_type.code in ['ceph_s3', 'amazon_s3', 'swift_s3']:
                if vpool.backend_type.code in ['swift_s3']:
                    strict_consistency = 'false'
                    s3_connection_flavour = 'SWIFT'
                else:
                    strict_consistency = 'true'
                    s3_connection_flavour = 'S3'

                vpool.metadata = {'s3_connection_host': connection_host,
                                  's3_connection_port': connection_port,
                                  's3_connection_username': connection_username,
                                  's3_connection_password': connection_password,
                                  's3_connection_flavour': s3_connection_flavour,
                                  's3_connection_strict_consistency': strict_consistency,
                                  's3_connection_verbose_logging': 1,
                                  'backend_type': 'S3'}

            vpool.name = vpool_name
            vpool.login = connection_username
            vpool.password = connection_password
            vpool.connection = '{0}:{1}'.format(connection_host, connection_port) if connection_host else None
            vpool.description = '{0} {1}'.format(vpool.backend_type.code, vpool_name)
            vpool.rdma_enabled = parameters['config_params']['dtl_transport'] == StorageDriverClient.FRAMEWORK_DTL_TRANSPORT_RSOCKET
            vpool.save()

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
        new_storagedriver = False
        if storagedriver is None:
            ports = StorageRouterController._get_free_ports(client, model_ports_in_use, 3)
            storagedriver = StorageDriver()
            new_storagedriver = True
        else:
            ports = storagedriver.ports
        model_ports_in_use += ports

        vrouter_id = '{0}{1}'.format(vpool_name, unique_id)

        config = ArakoonClusterConfig(StorageRouterController.ARAKOON_CLUSTER_ID_VOLDRV)
        config.load_config(client)
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
        if new_storagedriver:
            node_configs.append(ClusterNodeConfig(vrouter_id, str(grid_ip), ports[0], ports[1], ports[2]))

        try:
            vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), StorageRouterController.ARAKOON_CLUSTER_ID_VOLDRV, arakoon_node_configs)
            vrouter_clusterregistry.set_node_configs(node_configs)
        except:
            if new_vpool is True:
                vpool.delete()
            raise

        filesystem_config = StorageDriverConfiguration.build_filesystem_by_hypervisor(storagerouter.pmachine.hvtype)
        filesystem_config.update({'fs_metadata_backend_arakoon_cluster_nodes': [],
                                  'fs_metadata_backend_mds_nodes': [],
                                  'fs_metadata_backend_type': 'MDS'})

        # Updating the model
        storagedriver.name = vrouter_id.replace('_', ' ')
        storagedriver.ports = ports
        storagedriver.vpool = vpool
        storagedriver.cluster_ip = grid_ip
        storagedriver.storage_ip = '127.0.0.1' if storagerouter.pmachine.hvtype == 'KVM' else parameters['storage_ip']
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
        if backend_type.code == 'alba':
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
        size = StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.DB][0] * 1024 ** 3
        percentage = db_info['available'] * StorageRouterController.PARTITION_DEFAULT_USAGES[DiskPartition.ROLES.DB][1] / 100.0
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
                                 "foc_throttle_usecs": 4000}

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
        rsppath = '{0}/{1}'.format(client.config_read('ovs.storagedriver.rsp'), vpool_name)
        dirs2create.append(sdp_dtl.path)
        dirs2create.append(sdp_fd.path)
        dirs2create.append(rsppath)
        dirs2create.append(storagedriver.mountpoint)

        if backend_type.code == 'alba' and frag_size is None:
            raise ValueError('Something went wrong trying to calculate the fragment cache size')

        config_dir = '{0}/storagedriver/storagedriver'.format(client.config_read('ovs.core.cfgdir'))
        client.dir_create(config_dir)
        alba_proxy = storagedriver.alba_proxy
        if alba_proxy is None and vpool.backend_type.code == 'alba':
            service = DalService()
            service.storagerouter = storagerouter
            service.ports = [StorageRouterController._get_free_ports(client, model_ports_in_use, 1)]
            service.name = 'albaproxy_{0}'.format(vpool_name)
            service.type = ServiceTypeList.get_by_name('AlbaProxy')
            service.save()
            alba_proxy = AlbaProxy()
            alba_proxy.service = service
            alba_proxy.storagedriver = storagedriver
            alba_proxy.save()
            config = RawConfigParser()
            for section in vpool.metadata['metadata']:
                config.add_section(section)
                for key, value in vpool.metadata['metadata'][section].iteritems():
                    config.set(section, key, value)
            cache_dir = sdp_frag.path
            root_client.dir_create(cache_dir)
            System.write_config(config, '{0}/{1}_alba.cfg'.format(config_dir, vpool_name), client)

            # manifest cache is in memory
            client.file_write('{0}/{1}_alba.json'.format(config_dir, vpool_name), json.dumps({
                'log_level': 'info',
                'port': alba_proxy.service.ports[0],
                'ips': ['127.0.0.1'],
                'manifest_cache_size': 100000,
                'fragment_cache_dir': cache_dir,
                'fragment_cache_size': frag_size,
                'albamgr_cfg_file': '{0}/{1}_alba.cfg'.format(config_dir, vpool_name)
            }))

        # Possible modes: ['classic', 'ganesha']
        volumedriver_mode = Configuration.get('ovs.storagedriver.vmware_mode') if storagerouter.pmachine.hvtype == 'VMWARE' else 'classic'
        if storagerouter.pmachine.hvtype == 'VMWARE' and volumedriver_mode == 'ganesha':
            ganesha_config = '/opt/OpenvStorage/config/storagedriver/storagedriver/{0}_ganesha.conf'.format(vpool_name)
            contents = ''
            for template in ['ganesha-core', 'ganesha-export']:
                contents += client.file_read('/opt/OpenvStorage/config/templates/{0}.conf'.format(template))
            params = {'VPOOL_NAME': vpool_name,
                      'VPOOL_MOUNTPOINT': '/mnt/{0}'.format(vpool_name),
                      'NFS_FILESYSTEM_ID': storagerouter.ip.split('.', 2)[-1]}
            for key, value in params.iteritems():
                contents = contents.replace('<{0}>'.format(key), value)
            client.file_write(ganesha_config, contents)

        if 'config_params' in parameters:  # New vPool
            config_params = parameters['config_params']
            sco_size = config_params['sco_size']
            dtl_mode = config_params['dtl_mode']
            dedupe_mode = config_params['dedupe_mode']
            dtl_transport = config_params['dtl_transport']
            cache_strategy = config_params['cache_strategy']
            tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[sco_size]
            sco_factor = float(config_params['write_buffer']) / tlog_multiplier / sco_size  # sco_factor = write buffer / tlog multiplier (default 20) / sco size (in MiB)
        else:  # Extend vPool
            sco_size = current_storage_driver_config['sco_size']
            dtl_mode = current_storage_driver_config['dtl_mode']
            dedupe_mode = current_storage_driver_config['dedupe_mode']
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

        volume_manager_config["read_cache_default_mode"] = StorageDriverClient.VPOOL_DEDUPE_MAP[dedupe_mode]
        volume_manager_config["read_cache_default_behaviour"] = StorageDriverClient.VPOOL_CACHE_MAP[cache_strategy]
        volume_manager_config["number_of_scos_in_tlog"] = tlog_multiplier
        volume_manager_config["non_disposable_scos_factor"] = sco_factor

        queue_urls = []
        for current_storagerouter in StorageRouterList.get_masters():
            queue_urls.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(Configuration.get('ovs.core.broker.protocol'),
                                                                      Configuration.get('ovs.core.broker.login'),
                                                                      Configuration.get('ovs.core.broker.password'),
                                                                      current_storagerouter.ip)})

        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_name)
        storagedriver_config.load(client)
        storagedriver_config.clean()  # Clean out obsolete values
        if vpool.backend_type.code == 'alba':
            storagedriver_config.configure_backend_connection_manager(alba_connection_host='127.0.0.1',
                                                                      alba_connection_port=alba_proxy.service.ports[0],
                                                                      alba_connection_preset=vpool.metadata['preset'],
                                                                      alba_connection_timeout=15,
                                                                      backend_type='ALBA')
        elif vpool.backend_type.code in ['local', 'distributed']:
            storagedriver_config.configure_backend_connection_manager(**local_backend_data)
        else:
            storagedriver_config.configure_backend_connection_manager(**vpool.metadata)
        storagedriver_config.configure_content_addressed_cache(clustercache_mount_points=readcaches,
                                                               read_cache_serialization_path=rsppath)
        storagedriver_config.configure_scocache(scocache_mount_points=writecaches,
                                                trigger_gap='1GB',
                                                backoff_gap='2GB')
        storagedriver_config.configure_failovercache(failovercache_path=sdp_dtl.path,
                                                     failovercache_transport=StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[dtl_transport])
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
                                                     vrouter_sco_multiplier=sco_size / 4 * 1024,  # sco multiplier = SCO size (in MiB) / cluster size (currently 4KiB),
                                                     vrouter_backend_sync_timeout_ms=5000,
                                                     vrouter_migrate_timeout_ms=5000)
        storagedriver_config.configure_volume_router_cluster(vrouter_cluster_id=vpool.guid)
        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=StorageRouterController.ARAKOON_CLUSTER_ID_VOLDRV,
                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                              dls_arakoon_cluster_id=StorageRouterController.ARAKOON_CLUSTER_ID_VOLDRV,
                                                              dls_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_file_driver(fd_cache_path=sdp_fd.path,
                                                   fd_extent_cache_capacity='1024',
                                                   fd_namespace='fd-{0}-{1}'.format(vpool_name, vpool.guid))
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=Configuration.get('ovs.core.broker.queues.storagedriver'),
                                                       events_amqp_uris=queue_urls)
        storagedriver_config.configure_threadpool_component(num_threads=16)
        storagedriver_config.save(client, reload_config=False)

        DiskController.sync_with_reality(storagerouter.guid)

        MDSServiceController.prepare_mds_service(storagerouter=storagerouter,
                                                 vpool=vpool,
                                                 fresh_only=True,
                                                 reload_config=False)

        root_client.dir_create(dirs2create)
        root_client.file_create(files2create)
        if sdp_scrub is not None:
            root_client.dir_chmod(sdp_scrub.path, 0777)  # Used by gather_scrub_work which is a celery task executed by 'ovs' user and should be able to write in it

        params = {'VPOOL_MOUNTPOINT': storagedriver.mountpoint,
                  'HYPERVISOR_TYPE': storagerouter.pmachine.hvtype,
                  'VPOOL_NAME': vpool_name,
                  'UUID': str(uuid.uuid4()),
                  'OVS_UID': check_output('id -u ovs', shell=True).strip(),
                  'OVS_GID': check_output('id -g ovs', shell=True).strip(),
                  'KILL_TIMEOUT': str(int(readcache_size / 1024.0 / 1024.0 / 6.0 + 30))}

        logger.info('volumedriver_mode: {0}'.format(volumedriver_mode))
        logger.info('backend_type: {0}'.format(vpool.backend_type.code))
        dtl_service = 'ovs-dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name='ovs-dtl', params=params, client=root_client, target_name=dtl_service)
        ServiceManager.start_service(dtl_service, client=root_client)
        if vpool.backend_type.code == 'alba':
            alba_proxy_service = 'ovs-albaproxy_{0}'.format(vpool.name)
            ServiceManager.add_service(name='ovs-albaproxy', params=params, client=root_client, target_name=alba_proxy_service)
            ServiceManager.start_service(alba_proxy_service, client=root_client)
            dependencies = [alba_proxy_service]
        else:
            dependencies = None
        if volumedriver_mode == 'ganesha':
            template_name = 'ovs-ganesha'
        else:
            template_name = 'ovs-volumedriver'
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
                raise RuntimeError('StorageDriver service failed to start (service not running)')
            tries -= 1
            time.sleep(60 - tries)
            storagedriver = StorageDriver(storagedriver.guid)
        if storagedriver.startup_counter == current_startup_counter:
            raise RuntimeError('StorageDriver service failed to start (got no event)')
        logger.debug('StorageDriver running')

        mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vpool, check_online=not offline_nodes_detected)
        for sr in all_storagerouters:
            if sr.ip not in ip_client_map:
                continue
            node_client = ip_client_map[sr.ip]['ovs']
            storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_name)
            storagedriver_config.load(node_client)
            if storagedriver_config.is_new is False:
                storagedriver_config.clean()  # Clean out obsolete values
                storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                storagedriver_config.save(node_client)

        # Everything's reconfigured, refresh new cluster configuration
        client = StorageDriverClient.load(vpool)
        for current_storagedriver in vpool.storagedrivers:
            if current_storagedriver.storagerouter.ip not in ip_client_map:
                continue
            client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id))

        # Fill vPool size
        vfs_info = os.statvfs('/mnt/{0}'.format(vpool_name))
        vpool.size = vfs_info.f_blocks * vfs_info.f_bsize
        vpool.save()

        if offline_nodes_detected is True:
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, chain_timeout=600)
            except:
                pass
            try:
                for vdisk in vpool.vdisks:
                    MDSServiceController.ensure_safety(vdisk=vdisk)
            except:
                pass
        else:
            VDiskController.dtl_checkup(vpool_guid=vpool.guid, chain_timeout=600)
            for vdisk in vpool.vdisks:
                MDSServiceController.ensure_safety(vdisk=vdisk)

        mgmt_center = Factory.get_mgmtcenter(storagerouter.pmachine)
        if mgmt_center:
            if parameters['integratemgmt'] is True:
                mgmt_center.configure_vpool_for_host(vpool.guid, storagerouter.pmachine.ip)
        else:
            logger.info('Storagerouter {0} does not have management center'.format(storagerouter.name))

    @staticmethod
    @celery.task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(storagedriver_guid, offline_storage_router_guids=None):
        """
        Removes a Storage Driver (if its the last Storage Driver for a vPool, the vPool is removed as well)
        :param storagedriver_guid: Guid of the Storage Driver to remove
        :param offline_storage_router_guids: Guids of Storage Routers which are offline and will be removed from cluster.
                                             WHETHER VPOOL WILL BE DELETED DEPENDS ON THIS
        """
        storage_driver = StorageDriver(storagedriver_guid)
        logger.info('Remove Storage Driver - Guid {0} - Deleting Storage Driver {1}'.format(storage_driver.guid, storage_driver.name))

        if offline_storage_router_guids is None:
            offline_storage_router_guids = []

        client = None
        storage_drivers_left = False

        vpool = storage_driver.vpool
        storage_router = storage_driver.storagerouter
        storage_router_online = True
        storage_routers_offline = [StorageRouter(storage_router_guid) for storage_router_guid in offline_storage_router_guids]

        # Validations
        logger.info('Remove Storage Driver - Guid {0} - Checking availability of related Storage Routers'.format(storage_driver.guid, storage_driver.name))
        for sr in [sd.storagerouter for sd in vpool.storagedrivers]:
            if sr in storage_routers_offline:
                logger.info('Remove Storage Driver - Guid {0} - Storage Router {1} with IP {2} is offline'.format(storage_driver.guid, sr.name, sr.ip))
                continue
            if sr != storage_router:
                storage_drivers_left = True
            try:
                temp_client = SSHClient(sr, username='root')
                configuration_dir = temp_client.config_read('ovs.core.cfgdir')
                with Remote(temp_client.ip, [LocalStorageRouterClient]) as remote:
                    lsrc = remote.LocalStorageRouterClient('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name))
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
                    raise RuntimeError('Not all StorageDrivers are reachable, please (re)start them and try again')
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

        config = ArakoonClusterConfig(StorageRouterController.ARAKOON_CLUSTER_ID_VOLDRV)
        config.load_config(client)
        arakoon_node_configs = []
        offline_node_ips = [sr.ip for sr in storage_routers_offline]
        for node in config.nodes:
            if node.ip in offline_node_ips or (node.ip == storage_router.ip and storage_router_online is False):
                continue
            arakoon_node_configs.append(ArakoonNodeConfig(str(node.name), str(node.ip), node.client_port))
        logger.info('Remove Storage Driver - Guid {0} - Arakoon node configs - \n{1}'.format(storage_driver.guid, '\n'.join([str(config) for config in arakoon_node_configs])))
        vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), StorageRouterController.ARAKOON_CLUSTER_ID_VOLDRV, arakoon_node_configs)

        # Disable and stop DTL, voldrv and albaproxy services
        if storage_router_online is True:
            dtl_service = 'dtl_{0}'.format(vpool.name)
            voldrv_service = 'volumedriver_{0}'.format(vpool.name)
            albaproxy_service = 'albaproxy_{0}'.format(vpool.name)
            client = SSHClient(storage_router, username='root')
            configuration_dir = client.config_read('ovs.core.cfgdir')

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
                        storagedriver_client = remote.LocalStorageRouterClient('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name))
                        storagedriver_client.destroy_filesystem()

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
            configuration_dir = client.config_read('ovs.core.cfgdir')
            dirs_to_remove.append(storage_driver.mountpoint)
            dirs_to_remove.append('{0}/{1}'.format(client.config_read('ovs.storagedriver.rsp'), vpool.name))
            files_to_remove = ['{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name)]

            if vpool.backend_type.code == 'alba':
                files_to_remove.append('{0}/storagedriver/storagedriver/{1}_alba.cfg'.format(configuration_dir, vpool.name))
                files_to_remove.append('{0}/storagedriver/storagedriver/{1}_alba.json'.format(configuration_dir, vpool.name))
            if storage_router.pmachine.hvtype == 'VMWARE' and Configuration.get('ovs.storagedriver.vmware_mode') == 'ganesha':
                files_to_remove.append('{0}/storagedriver/storagedriver/{1}_ganesha.conf'.format(configuration_dir, vpool.name))

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
                        logger.info('Remove Storage Driver - Guid {0} - Recursively removed {1} on Storage Router with IP {2}'.format(storage_driver.guid, dir_name, client.ip))
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Failed to retrieve mountpoint information or delete directories, error: {1}'.format(storage_driver.guid, ex))
                errors_found = True

            logger.info('Remove Storage Driver - Guid {0} - Synchronizing disks with reality'.format(storage_driver.guid))
            try:
                DiskController.sync_with_reality(storage_router.guid)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - Synchronizing disks with reality failed with error: {1}'.format(storage_driver.guid, ex))
                errors_found = True

        # Model cleanup
        logger.info('Remove Storage Driver - Guid {0} - Cleaning up model'.format(storage_driver.guid))
        if storage_driver.alba_proxy is not None:
            logger.info('Remove Storage Driver - Guid {0} - Removing alba proxy service from model'.format(storage_driver.guid))
            service = storage_driver.alba_proxy.service
            storage_driver.alba_proxy.delete()
            service.delete()
        storage_driver.delete(abandon=['logs'])  # Detach from the log entries

        if storage_drivers_left is False:
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
        else:
            logger.info('Remove Storage Driver - Guid {0} - Checking DTL for all virtual disks in vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, chain_timeout=600)
            except Exception as ex:
                logger.error('Remove Storage Driver - Guid {0} - DTL checkup failed for vPool {1} with guid {2} with error: {3}'.format(storage_driver.guid, vpool.name, vpool.guid, ex))

        logger.info('Remove Storage Driver - Guid {0} - Running MDS checkup'.format(storage_driver.guid))
        try:
            MDSServiceController.mds_checkup()
        except Exception as ex:
            logger.error('Remove Storage Driver - Guid {0} - MDS checkup failed with error: {1}'.format(storage_driver.guid, ex))

        if errors_found is True:
            raise RuntimeError('1 or more errors occurred while trying to remove the storage driver. Please check /var/log/ovs/lib.log for more information')

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_storagedrivers')
    def update_storagedrivers(storagedriver_guids, storagerouters, parameters):
        """
        Add/remove multiple vPools
        @param storagedriver_guids: Storage Drivers to be removed
        @param storagerouters: StorageRouters on which to add a new link
        @param parameters: Settings for new links
        """
        print 'update storagedrivers: {0}'.format(str(parameters))
        success = True
        # Add Storage Drivers
        for storagerouter_ip, storageappliance_machineid in storagerouters:
            try:
                new_parameters = copy.copy(parameters)
                new_parameters['storagerouter_ip'] = storagerouter_ip
                local_machineid = System.get_my_machine_id()
                if local_machineid == storageappliance_machineid:
                    # Inline execution, since it's on the same node (preventing deadlocks)
                    StorageRouterController.add_vpool(new_parameters)
                else:
                    # Async execution, since it has to be executed on another node
                    # @TODO: Will break in Celery 3.2, need to find another solution
                    # Requirements:
                    # - This code cannot continue until this new task is completed (as all these Storage Router
                    #   need to be handled sequentially
                    # - The wait() or get() method are not allowed anymore from within a task to prevent deadlocks
                    try:
                        _ = SSHClient(storagerouter_ip)
                    except UnableToConnectException:
                        raise RuntimeError('StorageRouter {0} is not reachable'.format(storagerouter_ip))
                    result = StorageRouterController.add_vpool.s(new_parameters).apply_async(
                        routing_key='sr.{0}'.format(storageappliance_machineid)
                    )
                    result.wait()
            except Exception as ex:
                logger.error('{0}'.format(ex))
                success = False
        # Remove Storage Drivers
        for storagedriver_guid in storagedriver_guids:
            try:
                storagedriver = StorageDriver(storagedriver_guid)
                storagerouter = storagedriver.storagerouter
                storagerouter_machineid = storagerouter.machine_id
                local_machineid = System.get_my_machine_id()
                if local_machineid == storagerouter_machineid:
                    # Inline execution, since it's on the same node (preventing deadlocks)
                    StorageRouterController.remove_storagedriver(storagedriver_guid)
                else:
                    # Async execution, since it has to be executed on another node
                    # @TODO: Will break in Celery 3.2, need to find another solution
                    # Requirements:
                    # - This code cannot continue until this new task is completed (as all these VSAs need to be
                    # handled sequentially
                    # - The wait() or get() method are not allowed anymore from within a task to prevent deadlocks
                    try:
                        _ = SSHClient(storagerouter)
                    except UnableToConnectException:
                        raise RuntimeError('StorageRouter {0} is not reachable'.format(storagerouter.ip))
                    result = StorageRouterController.remove_storagedriver.s(storagedriver_guid).apply_async(
                        routing_key='sr.{0}'.format(storagerouter_machineid)
                    )
                    result.wait()
            except Exception as ex:
                logger.error('{0}'.format(ex))
                success = False
        return success

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_version_info')
    def get_version_info(storagerouter_guid):
        """
        Returns version information regarding a given StorageRouter
        :param storagerouter_guid: Storage Router guid to get version information for
        """
        return {'storagerouter_guid': storagerouter_guid,
                'versions': PackageManager.get_versions()}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_support_info')
    def get_support_info(storagerouter_guid):
        """
        Returns support information regarding a given StorageRouter
        :param storagerouter_guid: Storage Router guid to get support information for
        """
        return {'storagerouter_guid': storagerouter_guid,
                'nodeid': Configuration.get('ovs.support.nid'),
                'clusterid': Configuration.get('ovs.support.cid'),
                'enabled': Configuration.get('ovs.support.enabled'),
                'enablesupport': Configuration.get('ovs.support.enablesupport')}

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
        :param enable_support: If False openvpn will be stopped
        """
        clients = []
        try:
            for storagerouter in StorageRouterList.get_storagerouters():
                clients.append((SSHClient(storagerouter), SSHClient(storagerouter, username='root')))
        except UnableToConnectException:
            raise RuntimeError('Not all StorageRouters are reachable')
        for ovs_client, root_client in clients:
            ovs_client.config_set('ovs.support.enabled', enable)
            ovs_client.config_set('ovs.support.enablesupport', enable_support)
            if enable_support is False:
                root_client.run('service openvpn stop')
                root_client.file_delete('/etc/openvpn/ovs_*')
            if enable is True:
                if not ServiceManager.has_service(StorageRouterController.SUPPORT_AGENT, client=root_client):
                    ServiceManager.add_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                    ServiceManager.enable_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                if not ServiceManager.get_service_status(StorageRouterController.SUPPORT_AGENT, client=root_client):
                    ServiceManager.start_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                else:
                    ServiceManager.restart_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
            else:
                if ServiceManager.has_service(StorageRouterController.SUPPORT_AGENT, client=root_client):
                    if ServiceManager.get_service_status(StorageRouterController.SUPPORT_AGENT, client=root_client):
                        ServiceManager.stop_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
                    ServiceManager.remove_service(StorageRouterController.SUPPORT_AGENT, client=root_client)
        return True

    @staticmethod
    @celery.task(name='ovs.storagerouter.check_s3')
    def check_s3(host, port, accesskey, secretkey):
        """
        Validates whether connection to a given S3 backend can be made
        :param host: Host to check
        :param port: Port on which to check
        :param accesskey: Access key to be used for connection
        :param secretkey: Secret key to be used for connection
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
        :return: List of dictionaries which contain services to restart,
                                                    packages to update,
                                                    information about potential downtime
                                                    information about unmet prerequisites
        """
        this_sr = StorageRouterList.get_by_ip(client.ip)
        srs = StorageRouterList.get_storagerouters()
        ovsdb_cluster = [ser.storagerouter_guid for sr in srs for ser in sr.services if ser.type.name == 'Arakoon' and ser.name == 'arakoon-ovsdb']
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
        :return: List of dictionaries which contain services to restart,
                                                    packages to update,
                                                    information about potential downtime
                                                    information about unmet prerequisites
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

        this_sr = StorageRouterList.get_by_ip(client.ip)
        alba_proxies = []
        alba_downtime = []
        voldrv_cluster = []
        for sr in StorageRouterList.get_storagerouters():
            for service in sr.services:
                if service.type.name == 'Arakoon' and service.name == 'arakoon-voldrv':
                    voldrv_cluster.append(service.storagerouter_guid)
                elif service.type.name == 'AlbaProxy' and service.storagerouter_guid == this_sr.guid:
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
                                  'downtime': [('ovs', 'voldrv', None)] if len(voldrv_cluster) < 3 and this_sr.guid in voldrv_cluster else [],
                                  'namespace': 'ovs',
                                  'prerequisites': []}]}

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_framework')
    def update_framework(storagerouter_ip):
        """
        Launch the update_framework method in setup.py
        :param storagerouter_ip: IP of the Storage Router to update the framework packages on
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
        """
        StorageRouterController.set_rdma_capability(storagerouter_guid)
        DiskController.sync_with_reality(storagerouter_guid)

    @staticmethod
    def set_rdma_capability(storagerouter_guid):
        """
        Check if the Storage Router has been reconfigured to be able to support RDMA
        :param storagerouter_guid: Guid of the Storage Router to check and set
        :return: None
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter, username='root')
        rdma_capable = False
        with Remote(client.ip, [os], username='root') as remote:
            for root, dirs, files in remote.os.walk('/sys/class/infiniband'):
                for directory in dirs:
                    ports_dir = remote.os.path.join(root, directory, 'ports')
                    if not remote.os.path.exists(ports_dir):
                        continue
                    for sub_root, sub_dirs, _ in remote.os.walk(ports_dir):
                        if sub_root != ports_dir:
                            continue
                        for sub_directory in sub_dirs:
                            state_file = remote.os.path.join(sub_root, sub_directory, 'state')
                            if remote.os.path.exists(state_file):
                                if 'ACTIVE' in client.run('cat {0}'.format(state_file)):
                                    rdma_capable = True
        storagerouter.rdma_capable = rdma_capable
        storagerouter.save()

    @staticmethod
    @celery.task(name='ovs.storagerouter.configure_disk')
    def configure_disk(storagerouter_guid, disk_guid, partition_guid, offset, size, roles):
        """
        Configures a partition
        :param storagerouter_guid: Guid of the Storage Router to configure a disk on
        :param disk_guid: Guid of the disk to configure
        :param partition_guid: Guid of the partition on the disk
        :param offset: Offset for the partition
        :param size: Size of the partition
        :param roles: Roles assigned to the partition
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
        port_range = client.config_read('ovs.ports.storagedriver')
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

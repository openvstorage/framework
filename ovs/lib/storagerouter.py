# Copyright 2014 Open vStorage NV
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
import copy
import uuid
import json
import time
from ConfigParser import RawConfigParser
from subprocess import check_output, CalledProcessError

from ovs.celery_run import celery
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.j_albaproxy import AlbaProxy
from ovs.dal.hybrids.service import Service as DalService
from ovs.dal.lists.clientlist import ClientList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.extensions.api.client import OVSClient
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.remote import Remote
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration, StorageDriverClient
from ovs.extensions.support.agent import SupportAgent
from ovs.extensions.packages.package import PackageManager
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import add_hooks
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vpool import VPoolController
from ovs.log.logHandler import LogHandler
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter.storagerouterclient import ClusterRegistry, ArakoonNodeConfig, ClusterNodeConfig, LocalStorageRouterClient

logger = LogHandler.get('lib', name='storagerouter')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
storagerouterclient.Logger.enableLogging()


class StorageRouterController(object):
    """
    Contains all BLL related to StorageRouter
    """

    SUPPORT_AGENT = 'support-agent'

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_physical_metadata')
    def get_physical_metadata(files, storagerouter_guid):
        """
        Gets physical information about the machine this task is running on
        """
        from ovs.lib.vpool import VPoolController

        storagerouter = StorageRouter(storagerouter_guid)
        mountpoints = check_output('mount -v', shell=True).strip().splitlines()
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2 and
                       not p.split(' ')[2].startswith('/dev') and not p.split(' ')[2].startswith('/proc') and
                       not p.split(' ')[2].startswith('/sys') and not p.split(' ')[2].startswith('/run') and
                       p.split(' ')[2] != '/' and not p.split(' ')[2].startswith('/mnt/alba-asd')]
        arakoon_mountpoint = Configuration.get('ovs.arakoon.location')
        if arakoon_mountpoint in mountpoints:
            mountpoints.remove(arakoon_mountpoint)
        # include directories chosen during ovs setup
        readcaches = [entry for entry in Configuration.get('ovs.partitions.readcaches') if entry]
        writecaches = [entry for entry in Configuration.get('ovs.partitions.writecaches') if entry]
        storage = [entry for entry in Configuration.get('ovs.partitions.storage') if entry]
        mountpoints.extend(storage)
        if storagerouter.pmachine.hvtype == 'KVM':
            ipaddresses = ['127.0.0.1']
        else:
            ipaddresses = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().splitlines()
            ipaddresses = [ip.strip() for ip in ipaddresses]
            ipaddresses.remove('127.0.0.1')
        allow_vpool = VPoolController.can_be_served_on(storagerouter_guid)
        file_existence = {}
        for check_file in files:
            file_existence[check_file] = os.path.exists(check_file) and os.path.isfile(check_file)

        logger.info('mountpoints:{0}'.format(mountpoints))
        logger.info('readcaches:{0}'.format(readcaches))
        logger.info('writecaches:{0}'.format(writecaches))

        return {'mountpoints': mountpoints,
                'readcaches': readcaches,
                'writecaches': writecaches,
                'ipaddresses': ipaddresses,
                'files': file_existence,
                'allow_vpool': allow_vpool}

    @staticmethod
    @celery.task(name='ovs.storagerouter.add_vpool')
    def add_vpool(parameters):
        """
        Add a vPool to the machine this task is running on
        """
        onread = 'CacheOnRead'
        onwrite = 'CacheOnWrite'
        deduped = 'ContentBased'
        non_deduped = 'LocationBased'
        cache_mapping = {'none': None,
                         'onread': onread,
                         'onwrite': onwrite}
        dedupe_mapping = {'dedupe': deduped,
                          'nondedupe': non_deduped}
        dtl_mode_mapping = {'sync': '',
                            'async': '',
                            'nosync': ''}
        required_params = {'vpool_name': (str, Toolbox.regex_vpool),
                           'storage_ip': (str, Toolbox.regex_ip),
                           'storagerouter_ip': (str, Toolbox.regex_ip),
                           'integratemgmt': (bool, None),
                           'mountpoint_md': (str, Toolbox.regex_mountpoint),
                           'mountpoint_bfs': (str, Toolbox.regex_mountpoint, False),
                           'mountpoint_foc': (str, Toolbox.regex_mountpoint),
                           'mountpoint_temp': (str, Toolbox.regex_mountpoint),
                           'mountpoint_readcaches': (list, Toolbox.regex_mountpoint),
                           'mountpoint_writecaches': (list, Toolbox.regex_mountpoint)}
        required_params_for_new_vpool = {'type': (str, ['local', 'distributed', 'alba', 'ceph_s3', 'amazon_s3', 'swift_s3']),
                                         'config_params': (dict, {'dtl_mode': (str, dtl_mode_mapping.keys()),
                                                                  'sco_size': (int, None),
                                                                  'dedupe_mode': (str, dedupe_mapping.keys()),
                                                                  'dtl_enabled': (bool, None),
                                                                  'dtl_location': (str, None),
                                                                  'write_buffer': (int, None, False),
                                                                  'cache_strategy': (str, cache_mapping.keys())}),
                                         'connection_host': (str, Toolbox.regex_ip, False),
                                         'connection_port': (int, None),
                                         'connection_backend': (dict, None),
                                         'connection_username': (str, None),
                                         'connection_password': (str, None)}
        required_params_for_new_local_vpool = {'type': (str, ['local', 'distributed', 'alba', 'ceph_s3', 'amazon_s3', 'swift_s3'])}

        if not isinstance(parameters, dict):
            raise ValueError('Parameters should be of type "dict"')
        Toolbox.verify_required_params(required_params, parameters)

        ip = parameters['storagerouter_ip']
        vpool_name = parameters['vpool_name']
        storage_ip = parameters['storage_ip']
        mountpoint_md = parameters['mountpoint_md']
        mountpoint_bfs = parameters['mountpoint_bfs']
        mountpoint_foc = parameters['mountpoint_foc']
        mountpoint_temp = parameters['mountpoint_temp']
        mountpoint_readcaches = parameters['mountpoint_readcaches']
        mountpoint_writecaches = parameters['mountpoint_writecaches']

        client = SSHClient(ip)
        root_client = SSHClient(ip, username='root')
        unique_id = System.get_my_machine_id(client)

        storagerouter = None
        for current_storagerouter in StorageRouterList.get_storagerouters():
            if current_storagerouter.ip == ip and current_storagerouter.machine_id == unique_id:
                storagerouter = current_storagerouter
                break
        if storagerouter is None:
            raise RuntimeError('Could not find Storage Router with given ip address')

        vpool = VPoolList.get_vpool_by_name(vpool_name)
        storagedriver = None
        current_storage_driver_config = {}
        if vpool is not None:
            current_storage_driver_config = VPoolController.get_configuration(vpool.guid)
            if vpool.backend_type.code == 'local':
                # Might be an issue, investigating whether it's on the same Storage Router or not
                if len(vpool.storagedrivers) == 1 and vpool.storagedrivers[0].storagerouter.machine_id != unique_id:
                    raise RuntimeError('A local vPool with name {0} already exists'.format(vpool_name))
            for vpool_storagedriver in vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    # The vPool is already added to this Storage Router and this might be a cleanup/recovery
                    storagedriver = vpool_storagedriver

        all_storagerouters = [storagerouter]
        if vpool is not None:
            all_storagerouters += [sd.storagerouter for sd in vpool.storagedrivers]

        # Fetch clients services
        ip_client_map = {}
        try:
            for sr in all_storagerouters:
                ip_client_map[sr.ip] = {'root': SSHClient(sr.ip, username='root'),
                                        'ovs': SSHClient(sr.ip, username='ovs')}
        except UnableToConnectException:
            raise RuntimeError('Not all StorageRouters are reachable')

        # Keep in mind that if the Storage Driver exists, the vPool does as well
        if vpool is None:
            if parameters['type'] == 'local':
                Toolbox.verify_required_params(required_params_for_new_local_vpool, parameters)
            else:
                Toolbox.verify_required_params(required_params_for_new_vpool, parameters)
            vpool = VPool()
            backend_type = BackendTypeList.get_backend_type_by_code(parameters['type'])
            vpool.backend_type = backend_type
            connection_host = parameters.get('connection_host', '')
            connection_port = parameters.get('connection_port', '')
            connection_username = parameters.get('connection_username', '')
            connection_password = parameters.get('connection_password', '')
            if vpool.backend_type.code in ['local', 'distributed']:
                vpool.metadata = {'backend_type': 'LOCAL',
                                  'local_connection_path': mountpoint_bfs}
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
            else:
                raise ValueError('Unsupported backend type specified: "{0}"'.format(vpool.backend_type.code))

            vpool.name = vpool_name
            vpool.login = connection_username
            vpool.password = connection_password
            vpool.connection = '{0}:{1}'.format(connection_host, connection_port) if connection_host else None
            vpool.description = '{0} {1}'.format(vpool.backend_type.code, vpool_name)
            vpool.save()

        if len(mountpoint_readcaches) == 0:
            raise RuntimeError('No read cache mountpoints specified')
        if len(mountpoint_writecaches) == 0:
            raise RuntimeError('No write cache mountpoints specified')

        mountpoint_fcache = mountpoint_writecaches[0]
        mountpoint_fragmentcache = mountpoint_readcaches[0] if vpool.backend_type.code == 'alba' else ''

        # Check inodes and count the usages (to divide available space later on)
        all_locations = set()
        all_mountpoints = [mountpoint_bfs, mountpoint_temp, mountpoint_md, mountpoint_foc, mountpoint_fragmentcache, mountpoint_fcache] + mountpoint_readcaches + mountpoint_writecaches
        for mountpoint in all_mountpoints[:]:
            if not mountpoint:  # Eg: when bfs mountpoint is not used, the value is ''
                all_mountpoints.remove(mountpoint)
                continue
            all_locations.add(mountpoint)

        root_client.dir_create(all_locations)

        directory_usage = {}

        root_inode = os.stat('/').st_dev
        inode_count = {}
        mountpoint_inode_mapping = {}
        for mountpoint in all_mountpoints:
            inode = os.stat(mountpoint).st_dev
            if inode not in inode_count:
                inode_count[inode] = 0
            inode_count[inode] += 1
            mountpoint_inode_mapping[mountpoint] = inode

        if vpool.backend_type.code in ['local', 'distributed']:
            root_client.dir_chmod(parameters['mountpoint_bfs'], 0777)

        fdcache = '{0}/fd_{1}'.format(mountpoint_foc, vpool_name)
        failovercache = '{0}/foc_{1}'.format(mountpoint_foc, vpool_name)
        metadatapath = '{0}/metadata_{1}'.format(mountpoint_md, vpool_name)
        tlogpath = '{0}/tlogs_{1}'.format(mountpoint_md, vpool_name)
        rsppath = '{0}/{1}'.format(client.config_read('ovs.storagedriver.rsp'), vpool_name)

        dirs2create = [failovercache, metadatapath, tlogpath, rsppath]
        files2create = list()
        readcaches = list()
        writecaches = list()
        readcache_size = 0
        frag_size = None

        # Create same inode count mapping, but for mountpoints now
        mountpoint_count_mapping = {}
        for mountpoint in set(all_mountpoints):
            inode = os.stat(mountpoint).st_dev
            mountpoint_count_mapping[mountpoint] = inode_count[inode]

        # Calculate available space for read-, write- and fragmentcache
        for mountpoint, count in mountpoint_count_mapping.iteritems():
            if mountpoint_inode_mapping[mountpoint] == root_inode:
                # Divide by 2 because we don't want to allow root running full, so we only take 50% of available space
                available_size = os.statvfs(mountpoint).f_bavail * os.statvfs(mountpoint).f_bsize / count / 2
            else:
                available_size = os.statvfs(mountpoint).f_bavail * os.statvfs(mountpoint).f_bsize / count

            if mountpoint in mountpoint_readcaches:
                if mountpoint == mountpoint_fragmentcache and vpool.backend_type.code == 'alba':
                    # Multiply by 2 again because we don't want to divide available space evenly between
                    # fragment cache and readcache
                    r_size = int(available_size * 2 * 0.88 / 1024 / 4096) * 4096  # KiB
                    frag_size = int(available_size * 2 * .10)  # Bytes
                else:
                    r_size = int(available_size * 0.98 / 1024 / 4096) * 4096
                readcache_size += r_size
                readcaches.append({'path': '{0}/read_{1}'.format(mountpoint, vpool_name),
                                   'size': '{0}KiB'.format(r_size)})
                files2create.append('{0}/read_{1}'.format(mountpoint, vpool_name))
                inode = os.stat(mountpoint).st_dev
                if inode not in directory_usage:
                    directory_usage[inode] = []
                directory_usage[inode].append({'type': 'cache',
                                               'metadata': {'type': 'read'},
                                               'size': r_size * 1024})
            elif mountpoint in mountpoint_writecaches:
                w_size = int(available_size * .98 / 1024 / 4096) * 4096
                dir2create = '{0}/sco_{1}'.format(mountpoint, vpool_name)
                writecaches.append({'path': dir2create,
                                    'size': '{0}KiB'.format(w_size)})
                dirs2create.append(dir2create)
                inode = os.stat(mountpoint).st_dev
                if inode not in directory_usage:
                    directory_usage[inode] = []
                directory_usage[inode].append({'type': 'cache',
                                               'metadata': {'type': 'write'},
                                               'size': w_size * 1024})

        if vpool.backend_type.code == 'alba' and frag_size is None:
            raise ValueError('Something went wrong trying to calculate the fragment cache size')

        logger.info('readcaches: {0}'.format(readcaches))
        logger.info('writecaches: {0}'.format(writecaches))
        logger.info('mountpoint_temp: {0}'.format(mountpoint_temp))
        logger.info('mountpoint_md: {0}'.format(mountpoint_md))
        logger.info('mountpoint_foc: {0}'.format(mountpoint_foc))
        logger.info('mountpoint_fragmentcache: {0}'.format(mountpoint_fragmentcache))
        logger.info('mountpoint_fcache: {0}'.format(mountpoint_fcache))
        logger.info('mountpoint_readcaches: {0}'.format(mountpoint_readcaches))
        logger.info('mountpoint_writecaches: {0}'.format(mountpoint_writecaches))
        logger.info('all_locations: {0}'.format(mountpoint_count_mapping.keys()))

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

        cmd = "ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1"
        ipaddresses = client.run(cmd).strip().splitlines()
        ipaddresses = [ipaddr.strip() for ipaddr in ipaddresses]
        grid_ip = client.config_read('ovs.grid.ip')
        if grid_ip in ipaddresses:
            ipaddresses.remove(grid_ip)
        if not ipaddresses:
            raise RuntimeError('No available ip addresses found suitable for Storage Router storage ip')
        if storagerouter.pmachine.hvtype == 'KVM':
            volumedriver_storageip = '127.0.0.1'
        else:
            volumedriver_storageip = storage_ip
        vrouter_id = '{0}{1}'.format(vpool_name, unique_id)
        voldrv_arakoon_cluster_id = 'voldrv'
        voldrv_arakoon_cluster = ArakoonManagementEx().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        arakoon_nodes = []
        for node_id, node_config in voldrv_arakoon_client_config.iteritems():
            arakoon_nodes.append({'node_id': node_id, 'host': node_config[0][0], 'port': node_config[1]})
        arakoon_node_configs = []
        for arakoon_node in voldrv_arakoon_client_config.keys():
            arakoon_node_configs.append(ArakoonNodeConfig(arakoon_node,
                                                          voldrv_arakoon_client_config[arakoon_node][0][0],
                                                          voldrv_arakoon_client_config[arakoon_node][1]))
        vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), voldrv_arakoon_cluster_id, arakoon_node_configs)
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
        vrouter_clusterregistry.set_node_configs(node_configs)

        # Possible modes: ['classic', 'ganesha']
        if storagerouter.pmachine.hvtype == 'VMWARE':
            volumedriver_mode = Configuration.get('ovs.storagedriver.vmware_mode')
        else:
            volumedriver_mode = 'classic'

        filesystem_config = StorageDriverConfiguration.build_filesystem_by_hypervisor(storagerouter.pmachine.hvtype)
        filesystem_config.update({'fs_metadata_backend_arakoon_cluster_nodes': [],
                                  'fs_metadata_backend_mds_nodes': [],
                                  'fs_metadata_backend_type': 'MDS'})
        queue_protocol = Configuration.get('ovs.core.broker.protocol')
        queue_login = Configuration.get('ovs.core.broker.login')
        queue_password = Configuration.get('ovs.core.broker.password')
        queue_volumerouterqueue = Configuration.get('ovs.core.broker.queues.storagedriver')
        queue_urls = []
        for current_storagerouter in StorageRouterList.get_masters():
            queue_urls.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(queue_protocol,
                                                                      queue_login,
                                                                      queue_password,
                                                                      current_storagerouter.ip)})

        # Updating the model
        storagedriver.name = vrouter_id.replace('_', ' ')
        storagedriver.ports = ports
        storagedriver.vpool = vpool
        storagedriver.cluster_ip = grid_ip
        storagedriver.storage_ip = volumedriver_storageip
        storagedriver.mountpoint = '/mnt/{0}'.format(vpool_name)
        storagedriver.description = storagedriver.name
        storagedriver.storagerouter = storagerouter
        storagedriver.storagedriver_id = vrouter_id
        storagedriver.mountpoint_md = mountpoint_md
        storagedriver.mountpoint_foc = mountpoint_foc
        storagedriver.mountpoint_bfs = mountpoint_bfs
        storagedriver.mountpoint_temp = mountpoint_temp
        storagedriver.mountpoint_readcaches = mountpoint_readcaches
        storagedriver.mountpoint_writecaches = mountpoint_writecaches
        storagedriver.mountpoint_fragmentcache = mountpoint_fragmentcache
        storagedriver.save()

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
            cache_dir = '{}/fcache_{}'.format(mountpoint_fragmentcache, vpool_name)
            root_client.dir_create(cache_dir)
            System.write_config(config, '{0}/{1}_alba.cfg'.format(config_dir, vpool_name), client)
            inode = os.stat(cache_dir).st_dev
            if inode not in directory_usage:
                directory_usage[inode] = []
            directory_usage[inode].append({'type': 'cache',
                                           'metadata': {'type': 'fragment'},
                                           'size': frag_size,
                                           'relation': ('storagedriver', storagedriver.guid)})

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

        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_name)
        storagedriver_config.load(client)
        storagedriver_config.clean()  # Clean out obsolete values
        if vpool.backend_type.code == 'alba':
            storagedriver_config.configure_backend_connection_manager(alba_connection_host='127.0.0.1',
                                                                      alba_connection_port=alba_proxy.service.ports[0],
                                                                      alba_connection_preset=vpool.metadata['preset'],
                                                                      backend_type='ALBA')
        else:
            storagedriver_config.configure_backend_connection_manager(**vpool.metadata)

        if 'config_params' in parameters:
            sco_size = parameters['config_params']['sco_size']
            if 'write_buffer' in parameters['config_params']:
                # sco_factor = write buffer (in GiB) / tlog multiplier (default 20) / sco size (in MiB)
                sco_factor = parameters['config_params']['write_buffer'] * 1024.0 / 20 / sco_size
            else:
                # Below table makes sure the write buffer is always between 1 and 5 GiG
                sco_factor = {4: 12,
                              8: 12,
                              16: 12,
                              32: 6,
                              64: 3,
                              128: 2}[sco_size]

            dedupe_mode = parameters['config_params']['dedupe_mode']
            cache_strategy = parameters['config_params']['cache_strategy']
        else:
            sco_size = current_storage_driver_config['sco_size']
            sco_factor = current_storage_driver_config['write_buffer'] * 1024.0 / 20 / sco_size
            dedupe_mode = current_storage_driver_config['dedupe_mode']
            cache_strategy = current_storage_driver_config['cache_strategy']

        storagedriver_config.configure_content_addressed_cache(clustercache_mount_points=readcaches,
                                                               read_cache_serialization_path=rsppath)
        storagedriver_config.configure_scocache(scocache_mount_points=writecaches,
                                                trigger_gap='1GB',
                                                backoff_gap='2GB')
        storagedriver_config.configure_failovercache(failovercache_path=failovercache)
        storagedriver_config.configure_filesystem(**filesystem_config)
        storagedriver_config.configure_volume_manager(clean_interval=1,
                                                      metadata_path=metadatapath,
                                                      tlog_path=tlogpath,
                                                      foc_throttle_usecs=4000,
                                                      read_cache_default_mode=dedupe_mapping[dedupe_mode],
                                                      read_cache_default_behaviour=cache_mapping[cache_strategy],
                                                      non_disposable_scos_factor=sco_factor)
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
        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=voldrv_arakoon_cluster_id,
                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_file_driver(fd_cache_path=fdcache,
                                                   fd_extent_cache_capacity='1024',
                                                   fd_namespace='fd-{0}-{1}'.format(vpool_name, vpool.guid))
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=queue_volumerouterqueue,
                                                       events_amqp_uris=queue_urls)
        storagedriver_config.configure_threadpool_component(num_threads=16)
        storagedriver_config.save(client, reload_config=False)

        DiskController.sync_with_reality(storagerouter.guid)
        for mountpoint, usage in {mountpoint_md: {'type': 'metadata',
                                                  'metadata': {}},
                                  mountpoint_foc: {'type': 'cache',
                                                   'metadata': {'type': 'foc'}},
                                  mountpoint_bfs: {'type': 'backend',
                                                   'metadata': {'type': 'local'}},
                                  mountpoint_temp: {'type': 'temp',
                                                    'metadata': {}}}.iteritems():
            if not mountpoint:
                continue
            inode = os.stat(mountpoint).st_dev
            if inode not in directory_usage:
                directory_usage[inode] = []
            usage['size'] = None
            directory_usage[inode].append(usage)
        for inode in directory_usage:
            for usage in directory_usage[inode]:
                usage['relation'] = ('storagedriver', storagedriver.guid)
        for disk in storagerouter.disks:
            for partition in disk.partitions:
                if partition.inode is not None and partition.inode in directory_usage:
                    partition.usage += directory_usage[partition.inode]
                    partition.save()

        MDSServiceController.prepare_mds_service(client, storagerouter, vpool, reload_config=False)

        dirs2create.append(storagedriver.mountpoint)
        dirs2create.append(fdcache)

        root_client.dir_create(dirs2create)
        root_client.file_create(files2create)

        params = {'VPOOL_MOUNTPOINT': storagedriver.mountpoint,
                  'HYPERVISOR_TYPE': storagerouter.pmachine.hvtype,
                  'VPOOL_NAME': vpool_name,
                  'UUID': str(uuid.uuid4()),
                  'OVS_UID': check_output('id -u ovs', shell=True).strip(),
                  'OVS_GID': check_output('id -g ovs', shell=True).strip(),
                  'KILL_TIMEOUT': str(int(readcache_size / 1024.0 / 1024.0 / 6.0 + 30))}

        logger.info('volumedriver_mode: {0}'.format(volumedriver_mode))
        logger.info('backend_type: {0}'.format(vpool.backend_type.code))
        foc_service = 'ovs-failovercache_{0}'.format(vpool.name)
        ServiceManager.add_service(name='ovs-failovercache', params=params, client=root_client, target_name=foc_service)
        ServiceManager.start_service(foc_service, client=root_client)
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

        mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool)
        for sr in all_storagerouters:
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
            client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id))

        # Fill vPool size
        vfs_info = os.statvfs('/mnt/{0}'.format(vpool_name))
        vpool.size = vfs_info.f_blocks * vfs_info.f_bsize
        vpool.save()

        for vdisk in vpool.vdisks:
            MDSServiceController.ensure_safety(vdisk)

        mgmt_center = Factory.get_mgmtcenter(storagerouter.pmachine)
        if mgmt_center:
            if parameters['integratemgmt'] is True:
                mgmt_center.configure_vpool_for_host(vpool.guid, storagerouter.pmachine.ip)
        else:
            logger.info('Storagerouter {0} does not have management center'.format(storagerouter.name))

    @staticmethod
    @celery.task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(storagedriver_guid):
        """
        Removes a StorageDriver (and, if it was the last Storage Driver for a vPool, the vPool is removed as well)
        """
        logger.info('Deleting storage driver with guid {0}'.format(storagedriver_guid))

        # Get objects & Make some checks
        storagedriver = StorageDriver(storagedriver_guid)
        storagerouter = storagedriver.storagerouter
        ip = storagerouter.ip
        pmachine = storagerouter.pmachine
        vmachines = VMachineList.get_customer_vmachines()
        pmachine_guids = [vm.pmachine_guid for vm in vmachines]
        vpool_guids = [vm.vpool_guid for vm in vmachines if vm.vpool_guid is not None]
        storagedrivers_left = False
        vpool = storagedriver.vpool

        # Validate node connectivity
        try:
            for current_storagerouter in [sd.storagerouter for sd in vpool.storagedrivers]:
                client = SSHClient(current_storagerouter, username='root')
                configuration_dir = client.config_read('ovs.core.cfgdir')
                with Remote(client.ip, [LocalStorageRouterClient]) as remote:
                    lsrc = remote.LocalStorageRouterClient('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name))
                    lsrc.server_revision()
        except UnableToConnectException:
            raise RuntimeError('Not all StorageRouters are reachable')
        except Exception, ex:
            if 'ClusterNotReachableException' in str(ex):
                raise RuntimeError('Not all StorageDrivers are reachable, please (re)start them and try again')
            else:
                raise

        # Some more checking
        for current_storagedriver in vpool.storagedrivers:
            if current_storagedriver.guid != storagedriver_guid:
                storagedrivers_left = True
        if storagedrivers_left is False and pmachine.guid in pmachine_guids and vpool.guid in vpool_guids:
            raise RuntimeError('There are still vMachines served from the given Storage Driver')
        if any(vdisk for vdisk in vpool.vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id):
            raise RuntimeError('There are still vDisks served from the given Storage Driver')

        voldrv_service = 'volumedriver_{0}'.format(vpool.name)
        foc_service = 'failovercache_{0}'.format(vpool.name)
        albaproxy_service = 'albaproxy_{0}'.format(vpool.name)
        removal_mdsservices = [mds_service for mds_service in vpool.mds_services
                               if mds_service.service.storagerouter_guid == storagerouter.guid]

        # Unconfigure or reconfigure the MDSes
        vdisks = []
        for mds in removal_mdsservices:
            for junction in mds.vdisks:
                vdisks.append(junction.vdisk)
        for vdisk in vdisks:
            if vdisk.storagedriver_id:
                MDSServiceController.ensure_safety(vdisk, [storagerouter])

        client = SSHClient(ip, username='root')
        configuration_dir = client.config_read('ovs.core.cfgdir')
        # Possible modes: ['classic', 'ganesha']
        if storagerouter.pmachine.hvtype == 'VMWARE':
            volumedriver_mode = Configuration.get('ovs.storagedriver.vmware_mode')
        else:
            volumedriver_mode = 'classic'

        voldrv_arakoon_cluster_id = 'voldrv'
        voldrv_arakoon_cluster = ArakoonManagementEx().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        arakoon_node_configs = []
        for arakoon_node in voldrv_arakoon_client_config.keys():
            arakoon_node_configs.append(ArakoonNodeConfig(arakoon_node,
                                                          voldrv_arakoon_client_config[arakoon_node][0][0],
                                                          voldrv_arakoon_client_config[arakoon_node][1]))
        vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), voldrv_arakoon_cluster_id, arakoon_node_configs)

        if ServiceManager.has_service(voldrv_service, client=client):
            ServiceManager.disable_service(voldrv_service, client=client)
            ServiceManager.stop_service(voldrv_service, client=client)
        if ServiceManager.has_service(foc_service, client=client):
            ServiceManager.disable_service(foc_service, client=client)
            ServiceManager.stop_service(foc_service, client=client)

        if not storagedrivers_left:
            try:
                if ServiceManager.has_service(albaproxy_service, client=client):
                    ServiceManager.start_service(albaproxy_service, client=client)
                    tries = 10
                    running = False
                    port = storagedriver.alba_proxy.service.ports[0]
                    while running is False and tries > 0:
                        logger.debug('Waiting for the Alba proxy to start up again...')
                        tries -= 1
                        time.sleep(10 - tries)
                        try:
                            client.run('alba proxy-statistics --host 127.0.0.1 --port {0}'.format(port))
                            running = True
                        except CalledProcessError as ex:
                            logger.debug('Got error fetching Alba proxy-statistics, ignoring. {0}'.format(ex))
                    if running is False:
                        raise RuntimeError('Alba proxy failed to start')
                    logger.debug('Alba proxy running')

                logger.debug('Destroying filesystem and erasing node configs')
                storagedriver_client = LocalStorageRouterClient('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name))
                storagedriver_client.destroy_filesystem()
                vrouter_clusterregistry.erase_node_configs()
            except RuntimeError as ex:
                logger.error('Could not destroy filesystem or erase node configs: {0}'.format(ex))
            if ServiceManager.has_service(albaproxy_service, client=client):
                ServiceManager.stop_service(albaproxy_service, client=client)

        # Unconfigure vpool on management
        logger.debug('Unconfigure vPool from MgmtCenter')
        mgmtcenter = Factory.get_mgmtcenter(storagerouter.pmachine)
        if mgmtcenter:
            mgmtcenter.unconfigure_vpool_for_host(vpool.guid, not storagedrivers_left, storagerouter.pmachine.ip)

        # KVM pool
        if pmachine.hvtype == 'KVM':
            # 'Name                 State      Autostart '
            # '-------------------------------------------'
            # ' vpool1               active     yes'
            # ' vpool2               active     no'
            vpool_overview = client.run('virsh pool-list --all').splitlines()
            vpool_overview.pop(1)  # Pop   ---------------
            vpool_overview.pop(0)  # Pop   Name   State   Autostart
            for vpool_info in vpool_overview:
                vpool_name = vpool_info.split()[0].strip()
                if vpool.name == vpool_name:
                    client.run('virsh pool-destroy {0}'.format(vpool.name))
                    try:
                        client.run('virsh pool-undefine {0}'.format(vpool.name))
                    except Exception as ex:
                        logger.info('Got error during pool-undefine: {0}'.format(ex))
                    break

        # Remove services
        services_to_remove = [voldrv_service, foc_service]
        if storagedriver.alba_proxy is not None:
            services_to_remove.append(albaproxy_service)
        for service in services_to_remove:
            if ServiceManager.has_service(service, client=client):
                ServiceManager.remove_service(service, client=client)

        # Reconfigure volumedriver
        if storagedrivers_left is True:
            node_configs = []
            for current_storagedriver in vpool.storagedrivers:
                if current_storagedriver.guid != storagedriver_guid:
                    node_configs.append(ClusterNodeConfig(str(current_storagedriver.storagedriver_id),
                                                          str(current_storagedriver.cluster_ip),
                                                          current_storagedriver.ports[0],
                                                          current_storagedriver.ports[1],
                                                          current_storagedriver.ports[2]))
            vrouter_clusterregistry.set_node_configs(node_configs)
            srclient = StorageDriverClient.load(vpool)
            for current_storagedriver in vpool.storagedrivers:
                if storagedriver.guid != current_storagedriver.guid:
                    srclient.update_cluster_node_configs(str(current_storagedriver.storagedriver_id))

        for mds_service in removal_mdsservices:
            # All MDSServiceVDisk object should have been deleted above
            MDSServiceController.remove_mds_service(mds_service, client, storagerouter, vpool, reload_config=False)

        # Cleanup directories/files
        files_to_remove = list()
        dirs_to_remove = list()
        for readcache in storagedriver.mountpoint_readcaches:
            file_name = '{0}/read_{1}'.format(readcache, vpool.name)
            files_to_remove.append(file_name)

        for writecache in storagedriver.mountpoint_writecaches:
            dir_name = '{0}/sco_{1}'.format(writecache, vpool.name)
            dirs_to_remove.append(dir_name)

        dirs_to_remove.extend(['{0}/foc_{1}'.format(storagedriver.mountpoint_foc, vpool.name),
                               '{0}/fd_{1}'.format(storagedriver.mountpoint_foc, vpool.name),
                               '{0}/fcache_{1}'.format(storagedriver.mountpoint_fragmentcache, vpool.name),
                               '{0}/metadata_{1}'.format(storagedriver.mountpoint_md, vpool.name),
                               '{0}/tlogs_{1}'.format(storagedriver.mountpoint_md, vpool.name),
                               '{0}/{1}'.format(client.config_read('ovs.storagedriver.rsp'), vpool.name)])

        files_to_remove.append('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name))
        if vpool.backend_type.code == 'alba':
            files_to_remove.append('{0}/storagedriver/storagedriver/{1}_alba.cfg'.format(configuration_dir,
                                                                                         vpool.name))
            files_to_remove.append('{0}/storagedriver/storagedriver/{1}_alba.json'.format(configuration_dir,
                                                                                          vpool.name))
        if storagerouter.pmachine.hvtype == 'VMWARE' and volumedriver_mode == 'ganesha':
            files_to_remove.append('{0}/storagedriver/storagedriver/{1}_ganesha.conf'.format(configuration_dir,
                                                                                             vpool.name))

        for file_name in files_to_remove:
            if file_name and client.file_exists(file_name):
                client.file_delete(file_name)
                logger.info('Removed file {0}'.format(file_name))

        for dir_name in dirs_to_remove:
            if dir_name and client.dir_exists(dir_name):
                client.dir_delete(dir_name)
                logger.info('Recursively removed {0}'.format(dir_name))

        # Remove top directories
        dirs2remove = []
        dirs2remove.extend(storagedriver.mountpoint_readcaches)
        dirs2remove.extend(storagedriver.mountpoint_writecaches)
        dirs2remove.append(storagedriver.mountpoint_fragmentcache)
        dirs2remove.append(storagedriver.mountpoint_foc)
        dirs2remove.append(storagedriver.mountpoint_md)
        dirs2remove.append(storagedriver.mountpoint)

        mountpoints = client.run('mount -v').strip().splitlines()
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2 and
                       not p.split(' ')[2].startswith('/dev') and not p.split(' ')[2].startswith('/proc') and
                       not p.split(' ')[2].startswith('/sys') and not p.split(' ')[2].startswith('/run') and
                       p.split(' ')[2] != '/' and not p.split(' ')[2].startswith('/mnt/alba-asd')]

        for directory in set(dirs2remove):
            if directory and directory not in mountpoints:
                client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(directory))

        DiskController.sync_with_reality(storagerouter.guid)
        for disk in storagerouter.disks:
            for partition in disk.partitions:
                partition.usage = [usage for usage in partition.usage
                                   if 'relation' not in usage or
                                   usage['relation'][0] != 'storagedriver' or
                                   usage['relation'][1] != storagedriver.guid]
                partition.save()

        # First model cleanup
        if storagedriver.alba_proxy is not None:
            service = storagedriver.alba_proxy.service
            storagedriver.alba_proxy.delete()
            service.delete()
        storagedriver.delete(abandon=['logs'])  # Detach from the log entries

        if storagedrivers_left is False:
            # Final model cleanup
            for vdisk in vpool.vdisks:
                for junction in vdisk.mds_services:
                    junction.delete()
                vdisk.delete()
            vpool.delete()

        MDSServiceController.mds_checkup()

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
            except Exception, ex:
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
            except Exception, ex:
                logger.error('{0}'.format(ex))
                success = False
        return success

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_version_info')
    def get_version_info(storagerouter_guid):
        """
        Returns version information regarding a given StorageRouter
        """
        return {'storagerouter_guid': storagerouter_guid,
                'versions': PackageManager.get_versions()}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_support_info')
    def get_support_info(storagerouter_guid):
        """
        Returns support information regarding a given StorageRouter
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
        return SupportAgent.get_heartbeat_data()

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_logfiles')
    def get_logfiles(local_storagerouter_guid):
        """
        Collects logs, moves them to a web-accessible location and returns log tgz's filename
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

        ovs_info = PackageManager.verify_update_required(packages=['openvstorage-core', 'openvstorage-webapps'],
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
        voldrv_info = PackageManager.verify_update_required(packages=['volumedriver-base', 'volumedriver-server'],
                                                            services=['watcher-volumedriver'],
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
        """
        root_client = SSHClient(storagerouter_ip,
                                username='root')
        root_client.run('ovs update framework')

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_volumedriver')
    def update_volumedriver(storagerouter_ip):
        """
        Launch the update_volumedriver method in setup.py
        """
        root_client = SSHClient(storagerouter_ip,
                                username='root')
        root_client.run('ovs update volumedriver')

    @staticmethod
    def _get_free_ports(client, ports_in_use, number):
        """
        Gets `number` free ports ports that are not in use and not reserved
        """
        port_range = client.config_read('ovs.ports.storagedriver')
        ports = System.get_free_ports(port_range, ports_in_use, number, client)

        return ports if number != 1 else ports[0]

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
StorageRouter module
"""
import copy
import os
import re
import uuid
import json
from ConfigParser import RawConfigParser
from subprocess import check_output
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
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.system import System
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.support.agent import SupportAgent
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.package import Package
from ovs.plugin.provider.service import Service as PluginService
from volumedriver.storagerouter.storagerouterclient import ClusterRegistry, ArakoonNodeConfig, ClusterNodeConfig, LocalStorageRouterClient
from ovs.log.logHandler import LogHandler
from ovs.lib.mdsservice import MDSServiceController
from ovs.extensions.openstack.oscinder import OpenStackCinder
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.extensions.api.client import OVSClient

logger = LogHandler('lib', name='storagerouter')


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
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2
                       and not p.split(' ')[2].startswith('/dev') and not p.split(' ')[2].startswith('/proc')
                       and not p.split(' ')[2].startswith('/sys') and not p.split(' ')[2].startswith('/run')
                       and p.split(' ')[2] != '/' and not p.split(' ')[2].startswith('/mnt/alba-asd')]
        arakoon_mountpoint = Configuration.get('ovs.core.db.arakoon.location')
        if arakoon_mountpoint in mountpoints:
            mountpoints.remove(arakoon_mountpoint)
        # include directories chosen during ovs setup
        readcaches = Configuration.get('ovs.vpool_partitions.readcaches').split(',')
        writecaches = Configuration.get('ovs.vpool_partitions.writecaches').split(',')
        storage = Configuration.get('ovs.vpool_partitions.storage').split(',')
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

        print 'mountpoints:{}'.format(mountpoints)
        print 'readcaches:{}'.format(readcaches)
        print 'writecaches:{}'.format(writecaches)

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
        parameters = {} if parameters is None else parameters
        ip = parameters['storagerouter_ip']
        vpool_name = parameters['vpool_name']

        if StorageRouterController._validate_ip(ip) is False:
            raise ValueError('The entered ip address is invalid')

        if not re.match('^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$', vpool_name):
            raise ValueError('Invalid vpool_name given')

        client = SSHClient(ip)
        unique_id = System.get_my_machine_id(client)

        storagerouter = None
        for current_storagerouter in StorageRouterList.get_storagerouters():
            if current_storagerouter.ip == ip and current_storagerouter.machine_id == unique_id:
                storagerouter = current_storagerouter
                break
        if storagerouter is None:
            raise RuntimeError('Could not find Storage Router with given ip address')

        vpool = VPoolList.get_vpool_by_name(vpool_name)
        if vpool:
            if 'mountpoint_readcaches' in parameters:
                new_r_caches = list()
                for mp in parameters['mountpoint_readcaches']:
                    new_r_caches.append(os.path.split(mp)[0])
                parameters['mountpoint_readcaches'] = new_r_caches

            if 'mountpoint_writecaches' in parameters:
                new_w_caches = list()
                for mp in parameters['mountpoint_writecaches']:
                    new_w_caches.append(os.path.split(mp)[0])

                parameters['mountpoint_writecaches'] = new_w_caches

                print 'new_r_caches: {}'.format(new_r_caches)
                print 'new_w_caches: {}'.format(new_w_caches)

        storagedriver = None
        if vpool is not None:
            if vpool.backend_type.code == 'local':
                # Might be an issue, investigating whether it's on the same not or not
                if len(vpool.storagedrivers) == 1 and vpool.storagedrivers[0].storagerouter.machine_id != unique_id:
                    raise RuntimeError('A local vPool with name {0} already exists'.format(vpool_name))
            for vpool_storagedriver in vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    storagedriver = vpool_storagedriver  # The vPool is already added to this Storage Router and this might be a cleanup/recovery

            # Check whether there are running machines on this vPool
            machine_guids = []
            for vdisk in vpool.vdisks:
                if vdisk.vmachine_guid is None:
                    continue
                if vdisk.vmachine_guid not in machine_guids:
                    machine_guids.append(vdisk.vmachine_guid)
                    if vdisk.vmachine.hypervisor_status in ['RUNNING', 'PAUSED']:
                        raise RuntimeError(
                            'At least one vMachine using this vPool is still running or paused. Make sure there are no active vMachines'
                        )

        all_storagerouters = [storagerouter]
        if vpool is not None:
            all_storagerouters += [sd.storagerouter for sd in vpool.storagedrivers]
        voldrv_service = 'volumedriver_{0}'.format(vpool_name)

        # Stop services
        ip_client_map = {}
        for sr in all_storagerouters:
            if PluginService.has_service(voldrv_service, ip=sr.ip):
                PluginService.disable_service(voldrv_service, ip=sr.ip)
                PluginService.stop_service(voldrv_service, ip=sr.ip)
            ip_client_map[sr.ip] = SSHClient(sr.ip)

        # Keep in mind that if the Storage Driver exists, the vPool does as well
        mountpoint_bfs = None
        directories_to_create = set()

        cmd = "cat /etc/mtab | grep ^/dev/ | cut -d ' ' -f 2"
        mountpoints = [device.strip() for device in client.run(cmd).strip().splitlines()]
        mountpoints.remove('/')

        print 'mountpoints: {}'.format(mountpoints)

        if vpool is None:
            vpool = VPool()
            supported_backends = client.config_read('ovs.storagedriver.backends').split(',')
            if 'rest' in supported_backends:
                supported_backends.remove('rest')  # REST is not supported for now
            backend_type = BackendTypeList.get_backend_type_by_code(parameters['type'])
            vpool.backend_type = backend_type
            connection_host = connection_port = connection_username = connection_password = None
            if vpool.backend_type.code in ['local', 'distributed']:
                vpool.metadata = {'backend_type': 'LOCAL'}
                mountpoint_bfs = parameters['mountpoint_bfs']
                directories_to_create.add(mountpoint_bfs)
                vpool.metadata['local_connection_path'] = mountpoint_bfs
            elif vpool.backend_type.code == 'alba':
                if parameters['connection_host'] == '':
                    connection_host = Configuration.get('ovs.grid.ip')
                    connection_port = 443
                    oauth_client = ClientList.get_by_types('INTERNAL', 'CLIENT_CREDENTIALS')[0]
                    ovs_client = OVSClient(connection_host, connection_port,
                                           credentials=(oauth_client.client_id, oauth_client.client_secret))
                else:
                    connection_host = parameters['connection_host']
                    connection_port = parameters['connection_port']
                    connection_username = parameters['connection_username']
                    connection_password = parameters['connection_password']
                    ovs_client = OVSClient(connection_host, connection_port,
                                           credentials=(connection_username, connection_password))
                task_id = ovs_client.get('/alba/backends/{0}/get_config_metadata'.format(parameters['connection_backend']))
                successful, metadata = ovs_client.wait_for_task(task_id, timeout=300)
                if successful is False:
                    raise RuntimeError('Could not load metadata from remote environment {0}'.format(connection_host))
                vpool.metadata = metadata
            elif vpool.backend_type.code == 'rest':
                connection_host = parameters['connection_host']
                connection_port = parameters['connection_port']
                rest_connection_timeout_secs = parameters['connection_timeout']
                vpool.metadata = {'rest_connection_host': connection_host,
                                  'rest_connection_port': connection_port,
                                  'buchla_connection_log_level': "0",
                                  'rest_connection_verbose_logging': rest_connection_timeout_secs,
                                  'rest_connection_metadata_format': "JSON",
                                  'backend_type': 'REST'}
            elif vpool.backend_type.code in ['ceph_s3', 'amazon_s3', 'swift_s3']:
                connection_host = parameters['connection_host']
                connection_port = parameters['connection_port']
                connection_username = parameters['connection_username']
                connection_password = parameters['connection_password']
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
            vpool.description = "{} {}".format(vpool.backend_type.code, vpool_name)
            vpool.login = connection_username
            vpool.password = connection_password
            if not connection_host:
                vpool.connection = None
            else:
                vpool.connection = '{}:{}'.format(connection_host, connection_port)
            vpool.save()

        # Connection information is Storage Driver related information
        new_storagedriver = False
        if storagedriver is None:
            storagedriver = StorageDriver()
            new_storagedriver = True

        mountpoint_temp = parameters['mountpoint_temp']
        mountpoint_md = parameters['mountpoint_md']
        mountpoint_readcaches = parameters['mountpoint_readcaches']
        mountpoint_writecaches = parameters['mountpoint_writecaches']
        mountpoint_foc = parameters['mountpoint_foc']

        if not mountpoint_writecaches:
            raise RuntimeError('No write cache mountpoints specified')

        mountpoint_fragmentcache = mountpoint_writecaches[0]
        mountpoint_fcache = mountpoint_writecaches[0]

        all_locations = list()
        directories_to_create.add(mountpoint_temp)
        all_locations.append(mountpoint_temp)
        directories_to_create.add(mountpoint_md)
        all_locations.append(mountpoint_md)
        directories_to_create.add(mountpoint_foc)
        all_locations.append(mountpoint_foc)
        directories_to_create.add(mountpoint_fragmentcache)
        all_locations.append(mountpoint_fragmentcache)
        directories_to_create.add(mountpoint_fcache)
        all_locations.append(mountpoint_fcache)

        print 'mountpoint_temp: {}'.format(mountpoint_temp)
        print 'mountpoint_md: {}'.format(mountpoint_md)
        print 'mountpoint_foc: {}'.format(mountpoint_foc)
        print 'mountpoint_fragmentcache: {}'.format(mountpoint_fragmentcache)
        print 'mountpoint_fcache: {}'.format(mountpoint_fcache)
        print 'mp_rcs: {}'.format(mountpoint_readcaches)
        print 'mp_wcs: {}'.format(mountpoint_writecaches)

        def match_clustersize(size):
            # int type in KiB
            return int(int(size / 1024 / 4096) * 4096)

        for mp in mountpoint_readcaches:
            directories_to_create.add(str(mp))
            all_locations.append(str(mp))
        for mp in mountpoint_writecaches:
            directories_to_create.add(str(mp))
            all_locations.append(str(mp))

        print 'directories_to_create" {}'.format(directories_to_create)
        print 'all_locations: {}'.format(all_locations)

        client.dir_create(directories_to_create)

        if vpool.backend_type.code in ['local', 'distributed']:
            client.dir_chmod(parameters['mountpoint_bfs'], '0777')

        location_sizing = dict()
        for mp in all_locations:
            location = os.stat(mp).st_dev
            if location not in location_sizing:
                location_sizing[location] = {'size': os.statvfs(mp).f_bavail * os.statvfs(mp).f_bsize,
                                             'count': 1}
            else:
                location_sizing[location]['count'] += 1
        print location_sizing

        available_size = dict()
        for key, values in location_sizing.iteritems():
            available_size[key] = values['size'] / values['count']
        print available_size

        # @todo: put on all write caches?
        fdcache = '{}/fd_{}'.format(mountpoint_foc, vpool_name)
        failovercache = '{}/foc_{}'.format(mountpoint_foc, vpool_name)
        metadatapath = '{}/metadata_{}'.format(mountpoint_md, vpool_name)
        tlogpath = '{}/tlogs_{}'.format(mountpoint_md, vpool_name)
        rsppath = '/var/rsp/{}'.format(vpool_name)

        dirs2create = list()
        dirs2create.extend([failovercache, metadatapath, tlogpath, rsppath,
                           client.config_read('ovs.storagedriver.readcache.serialization.path')])
        files2create = list()
        readcaches = list()
        writecaches = list()
        readcache_size = 0
        r_count = 1
        w_count = 1

        unique_locations = set()
        for mp in directories_to_create:
            unique_locations.add(mp)

        for mp in unique_locations:
            if mp in mountpoint_readcaches:
                r_size = available_size[os.stat(mp).st_dev]
                readcache_size += match_clustersize(r_size * .98)
                readcaches.append({'path': '{}/read{}_{}'.format(mp, r_count, vpool_name),
                                   'size': '{0}KiB'.format(match_clustersize(r_size * .98))})
                files2create.append('{}/read{}_{}'.format(mp, r_count, vpool_name))
                r_count += 1
            if mp in mountpoint_writecaches:
                dirs2create.append('{}/sco{}_{}'.format(mp, w_count, vpool_name))
                w_size = available_size[os.stat(mp).st_dev]
                writecaches.append({'path': '{}/sco{}_{}'.format(mp, w_count, vpool_name),
                                    'size': '{0}KiB'.format(match_clustersize(w_size * .98))})
                dirs2create.append('{}/sco{}_{}'.format(mp, w_count, vpool_name))
                w_count += 1

        print 'readcaches: {}'.format(readcaches)
        print 'writecaches: {}'.format(writecaches)

        model_ports_in_use = []
        for port_storagedriver in StorageDriverList.get_storagedrivers():
            if port_storagedriver.storagerouter_guid == storagerouter.guid:
                # Local storagedrivers
                model_ports_in_use += port_storagedriver.ports
                if port_storagedriver.alba_proxy is not None:
                    model_ports_in_use.append(port_storagedriver.alba_proxy.service.ports[0])
        if new_storagedriver:
            ports = StorageRouterController._get_free_ports(client, model_ports_in_use, 3)
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
            volumedriver_storageip = parameters['storage_ip']
        vrouter_id = '{0}{1}'.format(vpool_name, unique_id)

        vrouter_config = {'vrouter_id': vrouter_id,
                          'vrouter_redirect_timeout_ms': '5000',
                          'vrouter_routing_retries': 10,
                          'vrouter_volume_read_threshold': 1024,
                          'vrouter_volume_write_threshold': 1024,
                          'vrouter_file_read_threshold': 1024,
                          'vrouter_file_write_threshold': 1024,
                          'vrouter_min_workers': 4,
                          'vrouter_max_workers': 16}
        voldrv_arakoon_cluster_id = str(client.config_read('ovs.storagedriver.db.arakoon.clusterid'))
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
            node_configs.append(ClusterNodeConfig(vrouter_id, grid_ip, ports[0], ports[1], ports[2]))
        vrouter_clusterregistry.set_node_configs(node_configs)

        filesystem_config = StorageDriverConfiguration.build_filesystem_by_hypervisor(storagerouter.pmachine.hvtype)
        filesystem_config.update({'fs_metadata_backend_arakoon_cluster_nodes': [],
                                  'fs_metadata_backend_mds_nodes': [],
                                  'fs_metadata_backend_type': 'MDS'})
        readcache_serialization_path = client.config_read('ovs.storagedriver.readcache.serialization.path')
        queue_protocol = Configuration.get('ovs.core.broker.protocol')
        queue_login = Configuration.get('ovs.core.broker.login')
        queue_password = Configuration.get('ovs.core.broker.password')
        queue_volumerouterqueue = Configuration.get('ovs.core.broker.volumerouter.queue')
        queue_urls = []
        for current_storagerouter in StorageRouterList.get_masters():
            queue_urls.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(queue_protocol,
                                                                      queue_login,
                                                                      queue_password,
                                                                      current_storagerouter.ip)})

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
            for section in vpool.metadata:
                config.add_section(section)
                for key, value in vpool.metadata[section].iteritems():
                    config.set(section, key, value)
            config_dir = '{0}/storagedriver/storagedriver'.format(client.config_read('ovs.core.cfgdir'))
            client.dir_create(config_dir)
            cache_dir = '{}/fcache_{}'.format(mountpoint_fragmentcache, vpool_name)
            client.dir_create(cache_dir)
            System.write_config(config, '{0}/{1}_alba.cfg'.format(config_dir, vpool_name), client)

            # manifest cache is in memory
            client.file_write('{0}/{1}_alba.json'.format(config_dir, vpool_name), json.dumps({
                'log_level': 'debug',
                'port': alba_proxy.service.ports[0],
                'ips': ['127.0.0.1'],
                'manifest_cache_size': 100000,
                'fragment_cache_dir': cache_dir,
                'fragment_cache_size': available_size[os.stat(mountpoint_fragmentcache).st_dev],
                'albamgr_cfg_file': '{0}/{1}_alba.cfg'.format(config_dir, vpool_name)
            }))

        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_name)
        storagedriver_config.load(client)
        storagedriver_config.clean()  # Clean out obsolete values
        if vpool.backend_type.code == 'alba':
            storagedriver_config.configure_backend_connection_manager(alba_connection_host='127.0.0.1',
                                                                      alba_connection_port=alba_proxy.service.ports[0],
                                                                      backend_type='ALBA')
        else:
            storagedriver_config.configure_backend_connection_manager(**vpool.metadata)
        storagedriver_config.configure_content_addressed_cache(clustercache_mount_points=readcaches,
                                                               read_cache_serialization_path=readcache_serialization_path)
        storagedriver_config.configure_scocache(scocache_mount_points=writecaches,
                                                trigger_gap='1GB',
                                                backoff_gap='2GB')
        storagedriver_config.configure_failovercache(failovercache_path=failovercache)
        storagedriver_config.configure_filesystem(**filesystem_config)
        storagedriver_config.configure_volume_manager(clean_interval=1,
                                                      metadata_path=metadatapath,
                                                      tlog_path=tlogpath,
                                                      foc_throttle_usecs=4000,
                                                      read_cache_default_behaviour='CacheOnWrite',
                                                      non_disposable_scos_factor=12)
        storagedriver_config.configure_volume_router(**vrouter_config)
        storagedriver_config.configure_volume_router_cluster(vrouter_cluster_id=vpool.guid)
        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=voldrv_arakoon_cluster_id,
                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_file_driver(fd_cache_path=fdcache,
                                                   fd_extent_cache_capacity='1024',
                                                   fd_namespace='fd-{0}-{1}'.format(vpool_name, vpool.guid))
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=queue_volumerouterqueue,
                                                       events_amqp_uris=queue_urls)
        storagedriver_config.save(client, reload_config=False)

        # Updating the model
        storagedriver.storagedriver_id = vrouter_id
        storagedriver.name = vrouter_id.replace('_', ' ')
        storagedriver.description = storagedriver.name
        storagedriver.storage_ip = volumedriver_storageip
        storagedriver.cluster_ip = grid_ip
        storagedriver.ports = ports
        storagedriver.mountpoint = '/mnt/{0}'.format(vpool_name)
        storagedriver.mountpoint_temp = mountpoint_temp
        storagedriver.mountpoint_fragmentcache = mountpoint_fragmentcache
        readcache_paths = list()
        for readcache in readcaches:
            readcache_paths.append(readcache['path'])
        writecache_paths = list()
        for writecache in writecaches:
            writecache_paths.append(writecache['path'])
        storagedriver.mountpoint_readcaches = readcache_paths
        storagedriver.mountpoint_writecaches = writecache_paths
        storagedriver.mountpoint_foc = mountpoint_foc
        storagedriver.mountpoint_bfs = mountpoint_bfs
        storagedriver.mountpoint_md = mountpoint_md
        storagedriver.storagerouter = storagerouter
        storagedriver.vpool = vpool
        storagedriver.save()

        MDSServiceController.prepare_mds_service(client, storagerouter, vpool)

        mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool)
        for sr in all_storagerouters:
            node_client = ip_client_map[sr.ip]
            storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_name)
            storagedriver_config.load(node_client)
            if storagedriver_config.is_new is False:
                storagedriver_config.clean()  # Clean out obsolete values
                storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                storagedriver_config.save(node_client, reload_config=False)

        dirs2create.append(storagedriver.mountpoint)
        dirs2create.append('{0}/fd_{1}'.format(mountpoint_foc, vpool_name))

        client.dir_create(dirs2create)
        client.file_create(files2create)

        params = {'<VPOOL_MOUNTPOINT>': storagedriver.mountpoint,
                  '<HYPERVISOR_TYPE>': storagerouter.pmachine.hvtype,
                  '<VPOOL_NAME>': vpool_name,
                  '<UUID>': str(uuid.uuid4()),
                  '<OVS_UID>': check_output('id -u ovs', shell=True).strip(),
                  '<OVS_GID>': check_output('id -g ovs', shell=True).strip(),
                  '<KILL_TIMEOUT>': str(int(readcache_size / 1024.0 / 1024.0 / 6.0 + 30))}

        if client.file_exists('/opt/OpenvStorage/config/templates/upstart/ovs-volumedriver.conf'):
            client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-volumedriver.conf /opt/OpenvStorage/config/templates/upstart/ovs-volumedriver_{0}.conf'.format(vpool_name))
            client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-failovercache.conf /opt/OpenvStorage/config/templates/upstart/ovs-failovercache_{0}.conf'.format(vpool_name))
            if vpool.backend_type.code == 'alba':
                client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-albaproxy.conf /opt/OpenvStorage/config/templates/upstart/ovs-albaproxy_{0}.conf'.format(vpool_name))

        PluginService.add_service(package=('openvstorage', 'volumedriver'), name='volumedriver_{0}'.format(vpool_name), command=None, stop_command=None, params=params, ip=client.ip)
        PluginService.add_service(package=('openvstorage', 'failovercache'), name='failovercache_{0}'.format(vpool_name), command=None, stop_command=None, params=params, ip=client.ip)
        if vpool.backend_type.code == 'alba':
            PluginService.add_service(package=('openvstorage', 'albaproxy'), name='albaproxy_{0}'.format(vpool_name), command=None, stop_command=None, params=params, ip=client.ip)

        if storagerouter.pmachine.hvtype == 'VMWARE':
            client.run("grep -q '/tmp localhost(ro,no_subtree_check)' /etc/exports || echo '/tmp localhost(ro,no_subtree_check)' >> /etc/exports")
            client.run('service nfs-kernel-server start')

        if storagerouter.pmachine.hvtype == 'KVM':
            client.run('virsh pool-define-as {0} dir - - - - {1}'.format(vpool_name, storagedriver.mountpoint))
            client.run('virsh pool-build {0}'.format(vpool_name))
            client.run('virsh pool-start {0}'.format(vpool_name))
            client.run('virsh pool-autostart {0}'.format(vpool_name))

        # Start services
        for sr in all_storagerouters:
            PluginService.enable_service(voldrv_service, ip=sr.ip)
            PluginService.start_service(voldrv_service, ip=sr.ip)

        # Fill vPool size
        vfs_info = os.statvfs('/mnt/{0}'.format(vpool_name))
        vpool.size = vfs_info.f_blocks * vfs_info.f_bsize
        vpool.save()

        for vdisk in vpool.vdisks:
            MDSServiceController.ensure_safety(vdisk)

        # Configure Cinder
        ovsdb = PersistentFactory.get_client()
        vpool_config_key = str('ovs_openstack_cinder_%s' % storagedriver.vpool_guid)
        if ovsdb.exists(vpool_config_key):
            # Second node gets values saved by first node
            cinder_password, cinder_user, tenant_name, controller_ip, config_cinder = ovsdb.get(vpool_config_key)
        else:
            config_cinder = parameters.get('config_cinder', False)
            cinder_password = ''
            cinder_user = ''
            tenant_name = ''
            controller_ip = ''
        if config_cinder:
            cinder_password = parameters.get('cinder_pass', cinder_password)
            cinder_user = parameters.get('cinder_user', cinder_user)
            tenant_name = parameters.get('cinder_tenant', tenant_name)
            controller_ip = parameters.get('cinder_controller', controller_ip)  # Keystone host
            if cinder_password:
                osc = OpenStackCinder(cinder_password=cinder_password,
                                      cinder_user=cinder_user,
                                      tenant_name=tenant_name,
                                      controller_ip=controller_ip)

                osc.configure_vpool(vpool_name)
                # Save values for first node to use
                ovsdb.set(vpool_config_key,
                          [cinder_password, cinder_user, tenant_name, controller_ip, config_cinder])

    @staticmethod
    @celery.task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(storagedriver_guid):
        """
        Removes a StorageDriver (and, if it was the last Storage Driver for a vPool, the vPool is removed as well)
        """
        # Get objects & Make some checks
        storagedriver = StorageDriver(storagedriver_guid)
        storagerouter = storagedriver.storagerouter
        ip = storagerouter.ip
        pmachine = storagerouter.pmachine
        vmachines = VMachineList.get_customer_vmachines()
        pmachine_guids = [vm.pmachine_guid for vm in vmachines]
        vpools_guids = [vm.vpool_guid for vm in vmachines if vm.vpool_guid is not None]

        vpool = storagedriver.vpool
        if pmachine.guid in pmachine_guids and vpool.guid in vpools_guids:
            raise RuntimeError('There are still vMachines served from the given Storage Driver')
        if any(vdisk for vdisk in vpool.vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id):
            raise RuntimeError('There are still vDisks served from the given Storage Driver')

        voldrv_service = 'volumedriver_{0}'.format(vpool.name)
        foc_service = 'failovercache_{0}'.format(vpool.name)
        albaproxy_service = 'albaproxy_{0}'.format(vpool.name)
        storagedrivers_left = False
        removal_mdsservices = [mds_service for mds_service in vpool.mds_services
                               if mds_service.service.storagerouter_guid == storagerouter.guid]

        # Unconfigure or reconfigure the MDSses
        vdisks = []
        for mds in removal_mdsservices:
            for junction in mds.vdisks:
                vdisks.append(junction.vdisk)
        for vdisk in vdisks:
            MDSServiceController.ensure_safety(vdisk, [storagerouter])

        # Stop services
        ip_client_map = {}
        for current_storagedriver in vpool.storagedrivers:
            if current_storagedriver.guid != storagedriver_guid:
                storagedrivers_left = True
            client = SSHClient(current_storagedriver.storagerouter.ip)
            if client.ip not in ip_client_map:
                ip_client_map[client.ip] = client

            if PluginService.has_service(voldrv_service, ip=client.ip):
                PluginService.disable_service(voldrv_service, ip=client.ip)
                PluginService.stop_service(voldrv_service, ip=client.ip)

        # Unconfigure Cinder
        ovsdb = PersistentFactory.get_client()
        key = str('ovs_openstack_cinder_%s' % storagedriver.vpool_guid)
        client = ip_client_map.get(ip, SSHClient(ip))
        if ovsdb.exists(key):
            cinder_password, cinder_user, tenant_name, controller_ip, _ = ovsdb.get(key)
            osc = OpenStackCinder(cinder_password=cinder_password,
                                  cinder_user=cinder_user,
                                  tenant_name=tenant_name,
                                  controller_ip=controller_ip,
                                  sshclient_ip=ip)
            osc.unconfigure_vpool(vpool.name, storagedriver.mountpoint, not storagedrivers_left)
            if not storagedrivers_left:
                ovsdb.delete(key)

        # KVM pool
        if pmachine.hvtype == 'KVM':
            if vpool.name in client.run('virsh pool-list --all'):
                client.run('virsh pool-destroy {0}'.format(vpool.name))
                try:
                    client.run('virsh pool-undefine {0}'.format(vpool.name))
                except:
                    pass  # Ignore undefine errors, since that can happen on re-entrance

        # Remove services
        services_to_remove = [voldrv_service, foc_service] + [mdsservice.service.name for mdsservice in removal_mdsservices]
        if storagedriver.alba_proxy is not None:
            services_to_remove.append(albaproxy_service)
        for service in services_to_remove:
            if PluginService.has_service(service, ip=client.ip):
                PluginService.remove_service(domain='openvstorage', name=service, ip=client.ip)

        configuration_dir = client.config_read('ovs.core.cfgdir')

        voldrv_arakoon_cluster_id = str(client.config_read('ovs.storagedriver.db.arakoon.clusterid'))
        voldrv_arakoon_cluster = ArakoonManagementEx().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        arakoon_node_configs = []
        for arakoon_node in voldrv_arakoon_client_config.keys():
            arakoon_node_configs.append(ArakoonNodeConfig(arakoon_node,
                                                          voldrv_arakoon_client_config[arakoon_node][0][0],
                                                          voldrv_arakoon_client_config[arakoon_node][1]))
        vrouter_clusterregistry = ClusterRegistry(str(vpool.guid), voldrv_arakoon_cluster_id, arakoon_node_configs)
        # Reconfigure volumedriver
        if storagedrivers_left:
            node_configs = []
            for current_storagedriver in vpool.storagedrivers:
                if current_storagedriver.guid != storagedriver_guid:
                    node_configs.append(ClusterNodeConfig(str(current_storagedriver.storagedriver_id),
                                                          str(current_storagedriver.cluster_ip),
                                                          current_storagedriver.ports[0],
                                                          current_storagedriver.ports[1],
                                                          current_storagedriver.ports[2]))
            vrouter_clusterregistry.set_node_configs(node_configs)
        else:
            try:
                storagedriver_client = LocalStorageRouterClient('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir, vpool.name))
                storagedriver_client.destroy_filesystem()
                vrouter_clusterregistry.erase_node_configs()
            except RuntimeError as ex:
                print('Could not destroy filesystem or erase node configs due to error: {}'.format(ex))

        # Cleanup directories
        for readcache in storagedriver.mountpoint_readcaches:
            client.run('rm {}'.format(readcache))

        for writecache in storagedriver.mountpoint_writecaches:
            if writecache:
                client.run('rm -rf {}'.format(writecache))

        client.run('rm -rf {}/foc_{}'.format(storagedriver.mountpoint_foc, vpool.name))
        client.run('rm -rf {}/fd_{}'.format(storagedriver.mountpoint_foc, vpool.name))
        client.run('rm -rf {}/fcache_{}'.format(storagedriver.mountpoint_fragmentcache, vpool.name))
        client.run('rm -rf {}/metadata_{}'.format(storagedriver.mountpoint_md, vpool.name))
        client.run('rm -rf {}/tlogs_{}'.format(storagedriver.mountpoint_md, vpool.name))
        client.run('rm -rf /var/rsp/{}'.format(vpool.name))

        # Remove files
        for config_file in ['{0}.json'] + ['{0}.json'.format(mdsservice.service.name) for mdsservice in removal_mdsservices]:
            client.run('rm -f {0}/storagedriver/storagedriver/{1}'.format(configuration_dir, config_file.format(vpool.name)))

        # Remove top directories
        dirs2remove = list()
        for mp in storagedriver.mountpoint_readcaches:
            dirs2remove.append(os.path.dirname(mp))
        dirs2remove.extend(storagedriver.mountpoint_writecaches)
        dirs2remove.append(storagedriver.mountpoint_fragmentcache)
        dirs2remove.append(storagedriver.mountpoint_foc)
        dirs2remove.append(storagedriver.mountpoint_md)
        dirs2remove.append(storagedriver.mountpoint)

        for directory in dirs2remove:
            if directory:
                client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(directory))

        # First model cleanup
        if storagedriver.alba_proxy is not None:
            storagedriver.alba_proxy.delete()
        storagedriver.delete(abandon=True)  # Detach from the log entries
        for mds_service in removal_mdsservices:
            # All MDSServiceVDisk object should have been deleted above
            service = mds_service.service
            mds_service.delete()
            service.delete()

        if storagedrivers_left:
            # Restart leftover services
            for current_storagedriver in vpool.storagedrivers:
                if current_storagedriver.guid != storagedriver_guid:
                    if PluginService.has_service(voldrv_service, ip=current_storagedriver.storagerouter.ip):
                        PluginService.enable_service(voldrv_service, ip=current_storagedriver.storagerouter.ip)
                        PluginService.start_service(voldrv_service, ip=current_storagedriver.storagerouter.ip)
        else:
            # Final model cleanup
            vpool.delete()

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_storagedrivers')
    def update_storagedrivers(storagedriver_guids, storagerouters, parameters):
        """
        Add/remove multiple vPools
        @param storagedriver_guids: Storage Drivers to be removed
        @param storagerouters: StorageRouters on which to add a new link
        @param parameters: Settings for new links
        """
        print 'update storagedrivers: {}'.format(str(parameters))
        success = True
        # Add Storage Drivers
        for storagerouter_ip, storageapplaince_machineid in storagerouters:
            try:
                new_parameters = copy.copy(parameters)
                new_parameters['storagerouter_ip'] = storagerouter_ip
                local_machineid = System.get_my_machine_id()
                if local_machineid == storageapplaince_machineid:
                    # Inline execution, since it's on the same node (preventing deadlocks)
                    StorageRouterController.add_vpool(new_parameters)
                else:
                    # Async execution, since it has to be executed on another node
                    # @TODO: Will break in Celery 3.2, need to find another solution
                    # Requirements:
                    # - This code cannot continue until this new task is completed (as all these Storage Router
                    #   need to be handled sequentially
                    # - The wait() or get() method are not allowed anymore from within a task to prevent deadlocks
                    result = StorageRouterController.add_vpool.s(new_parameters).apply_async(
                        routing_key='sr.{0}'.format(storageapplaince_machineid)
                    )
                    result.wait()
            except:
                success = False
        # Remove Storage Drivers
        for storagedriver_guid in storagedriver_guids:
            try:
                storagedriver = StorageDriver(storagedriver_guid)
                storagerouter_machineid = storagedriver.storagerouter.machine_id
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
                    result = StorageRouterController.remove_storagedriver.s(storagedriver_guid).apply_async(
                        routing_key='sr.{0}'.format(storagerouter_machineid)
                    )
                    result.wait()
            except:
                success = False
        return success

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_version_info')
    def get_version_info(storagerouter_guid):
        """
        Returns version information regarding a given StorageRouter
        """
        return {'storagerouter_guid': storagerouter_guid,
                'versions': Package.get_versions()}

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_support_info')
    def get_support_info(storagerouter_guid):
        """
        Returns support information regarding a given StorageRouter
        """
        return {'storagerouter_guid': storagerouter_guid,
                'nodeid': Configuration.get('ovs.support.nid'),
                'clusterid': Configuration.get('ovs.support.cid'),
                'enabled': int(Configuration.get('ovs.support.enabled')) > 0,
                'enablesupport': int(Configuration.get('ovs.support.enablesupport')) > 0}

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
        storagerouter = StorageRouter(local_storagerouter_guid)
        webpath = '/opt/OpenvStorage/webapps/frontend/downloads'
        logfile = check_output('ovs collect logs', shell=True).strip()
        logfilename = logfile.split('/')[-1]
        client = SSHClient(storagerouter.ip)
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
        for storagerouter in StorageRouterList.get_storagerouters():
            client = SSHClient(storagerouter.ip)
            client.config_set('ovs.support.enabled', 1 if enable else 0)
            client.config_set('ovs.support.enablesupport', 1 if enable_support else 0)
            if enable_support is False:
                client.run('service openvpn stop')
                client.run('rm -f /etc/openvpn/ovs_*')
            if enable is True:
                if not PluginService.has_service(StorageRouterController.SUPPORT_AGENT, ip=client.ip):
                    PluginService.add_service(None, StorageRouterController.SUPPORT_AGENT, None, None, ip=client.ip)
                    PluginService.enable_service(StorageRouterController.SUPPORT_AGENT, ip=client.ip)
                if not PluginService.get_service_status(StorageRouterController.SUPPORT_AGENT, ip=client.ip):
                    PluginService.start_service(StorageRouterController.SUPPORT_AGENT, ip=client.ip)
                else:
                    PluginService.restart_service(StorageRouterController.SUPPORT_AGENT, ip=client.ip)
            else:
                if PluginService.has_service(StorageRouterController.SUPPORT_AGENT, ip=client.ip):
                    if PluginService.get_service_status(StorageRouterController.SUPPORT_AGENT, ip=client.ip):
                        PluginService.stop_service(StorageRouterController.SUPPORT_AGENT, ip=client.ip)
                    PluginService.remove_service(None, StorageRouterController.SUPPORT_AGENT, ip=client.ip)
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
    @celery.task(name='ovs.storagerouter.check_cinder')
    def check_cinder():
        """
        Checks whether cinder is running
        """
        osc = OpenStackCinder()
        return osc.is_cinder_installed

    @staticmethod
    @celery.task(name='ovs.storagerouter.valid_cinder_credentials')
    def valid_cinder_credentials(cinder_password, cinder_user, tenant_name, controller_ip):
        """
        Checks whether the cinder credentials are valid
        """
        osc = OpenStackCinder()
        return osc.valid_credentials(cinder_password, cinder_user, tenant_name, controller_ip)

    @staticmethod
    def _validate_ip(ip):
        """
        Validates an ip address
        """
        regex = '^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$'
        match = re.search(regex, ip)
        return match is not None

    @staticmethod
    def _get_free_ports(client, ports_in_use, number):
        """
        Gets `number` free ports ports that are not in use and not reserved
        """
        port_range = client.config_read('ovs.ports.storagedriver')
        ports = System.get_free_ports(port_range, ports_in_use, number, client)

        return ports if number != 1 else ports[0]

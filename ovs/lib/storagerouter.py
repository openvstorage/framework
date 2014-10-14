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

from subprocess import check_output
from ovs.celery import celery
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.extensions.generic.system import System
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.generic.sshclient import SSHClient
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.package import Package
from volumedriver.storagerouter.storagerouterclient import ClusterRegistry, ArakoonNodeConfig, ClusterNodeConfig, LocalStorageRouterClient
from ovs.log.logHandler import LogHandler

logger = LogHandler('lib', name='storagerouter')


class StorageRouterController(object):
    """
    Contains all BLL related to StorageRouter
    """

    @staticmethod
    @celery.task(name='ovs.storagerouter.get_physical_metadata')
    def get_physical_metadata(files, storagerouter_guid):
        """
        Gets physical information about the machine this task is running on
        """
        from ovs.lib.vpool import VPoolController

        storagerouter = StorageRouter(storagerouter_guid)
        mountpoints = check_output('mount -v', shell=True).strip().split('\n')
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2
                       and not p.split(' ')[2].startswith('/dev') and not p.split(' ')[2].startswith('/proc')
                       and not p.split(' ')[2].startswith('/sys') and not p.split(' ')[2].startswith('/run')
                       and p.split(' ')[2] != '/']
        arakoon_mountpoint = Configuration.get('ovs.core.db.arakoon.location')
        if arakoon_mountpoint in mountpoints:
            mountpoints.remove(arakoon_mountpoint)
        if storagerouter.pmachine.hvtype == 'KVM':
            ipaddresses = ['127.0.0.1']
        else:
            ipaddresses = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().split('\n')
            ipaddresses = [ip.strip() for ip in ipaddresses]
            ipaddresses.remove('127.0.0.1')
        xmlrpcport = Configuration.get('volumedriver.filesystem.xmlrpc.port')
        allow_vpool = VPoolController.can_be_served_on(storagerouter_guid)
        file_existence = {}
        for check_file in files:
            file_existence[check_file] = os.path.exists(check_file) and os.path.isfile(check_file)
        return {'mountpoints': mountpoints,
                'ipaddresses': ipaddresses,
                'xmlrpcport': xmlrpcport,
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

        if not re.match('^[0-9a-zA-Z]+(\-+[0-9a-zA-Z]+)*$', vpool_name):
            raise ValueError('Invalid vpool_name given')

        client = SSHClient.load(ip)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
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
        if vpool is not None:
            if vpool.type == 'LOCAL':
                # Might be an issue, investigating whether it's on the same not or not
                if len(vpool.storagedrivers) == 1 and vpool.storagedrivers[0].storagerouter.machine_id != unique_id:
                    raise RuntimeError('A local vPool with name {0} already exists'.format(vpool_name))
            for vpool_storagedriver in vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    storagedriver = vpool_storagedriver  # The vPool is already added to this Storage Router and this might be a cleanup/recovery

            # Check whether there are running machines on this vPool
            machine_guids = []
            for vdisk in vpool.vdisks:
                if vdisk.vmachine_guid not in machine_guids:
                    machine_guids.append(vdisk.vmachine_guid)
                    if vdisk.vmachine.hypervisor_status in ['RUNNING', 'PAUSED']:
                        raise RuntimeError(
                            'At least one vMachine using this vPool is still running or paused. Make sure there are no active vMachines'
                        )

        nodes = {ip}  # Set comprehension
        if vpool is not None:
            for vpool_storagedriver in vpool.storagedrivers:
                nodes.add(vpool_storagedriver.storagerouter.ip)
        nodes = list(nodes)

        services = ['volumedriver_{0}'.format(vpool_name),
                    'failovercache_{0}'.format(vpool_name)]

        # Stop services
        for node in nodes:
            node_client = SSHClient.load(node)
            for service in services:
                System.exec_remote_python(node_client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.disable_service('{0}')
""".format(service))
                System.exec_remote_python(node_client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.stop_service('{0}')
""".format(service))

        # Keep in mind that if the Storage Driver exists, the vPool does as well
        client = SSHClient.load(ip)
        mountpoint_bfs = ''
        directories_to_create = []

        if vpool is None:
            vpool = VPool()
            supported_backends = System.read_remote_config(client, 'volumedriver.supported.backends').split(',')
            if 'REST' in supported_backends:
                supported_backends.remove('REST')  # REST is not supported for now
            vpool.type = parameters['type']
            connection_host = connection_port = connection_username = connection_password = None
            if vpool.type in ['LOCAL', 'DISTRIBUTED']:
                vpool.metadata = {'backend_type': 'LOCAL'}
                mountpoint_bfs = parameters['mountpoint_bfs']
                directories_to_create.append(mountpoint_bfs)
                vpool.metadata['local_connection_path'] = mountpoint_bfs
            if vpool.type == 'REST':
                connection_host = parameters['connection_host']
                connection_port = parameters['connection_port']
                rest_connection_timeout_secs = parameters['connection_timeout']
                vpool.metadata = {'rest_connection_host': connection_host,
                                  'rest_connection_port': connection_port,
                                  'buchla_connection_log_level': "0",
                                  'rest_connection_verbose_logging': rest_connection_timeout_secs,
                                  'rest_connection_metadata_format': "JSON",
                                  'backend_type': 'REST'}
            elif vpool.type in ('CEPH_S3', 'AMAZON_S3', 'SWIFT_S3'):
                connection_host = parameters['connection_host']
                connection_port = parameters['connection_port']
                connection_username = parameters['connection_username']
                connection_password = parameters['connection_password']
                if vpool.type in ['SWIFT_S3']:
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
            vpool.description = "{} {}".format(vpool.type, vpool_name)
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
        mountpoint_readcache1 = parameters['mountpoint_readcache1']
        mountpoint_readcache2 = parameters.get('mountpoint_readcache2', '')
        mountpoint_writecache = parameters['mountpoint_writecache']
        mountpoint_foc = parameters['mountpoint_foc']

        directories_to_create.append(mountpoint_temp)
        directories_to_create.append(mountpoint_md)
        directories_to_create.append(mountpoint_readcache1)
        if mountpoint_readcache2:
            directories_to_create.append(mountpoint_readcache2)
        directories_to_create.append(mountpoint_writecache)
        directories_to_create.append(mountpoint_foc)

        client = SSHClient.load(ip)
        dir_create_script = """
import os
for directory in {0}:
    if not os.path.exists(directory):
        os.makedirs(directory)
""".format(directories_to_create)
        System.exec_remote_python(client, dir_create_script)

        read_cache1_fs = os.statvfs(mountpoint_readcache1)
        read_cache2_fs = None
        if mountpoint_readcache2:
            read_cache2_fs = os.statvfs(mountpoint_readcache2)
        write_cache_fs = os.statvfs(mountpoint_writecache)
        fdcache = '{}/fd_{}'.format(mountpoint_writecache, vpool_name)
        scocache = '{}/sco_{}'.format(mountpoint_writecache, vpool_name)
        readcache1 = '{}/read1_{}'.format(mountpoint_readcache1, vpool_name)
        files2create = [readcache1]
        if mountpoint_readcache2 and mountpoint_readcache1 != mountpoint_readcache2:
            readcache2 = '{}/read2_{}'.format(mountpoint_readcache2, vpool_name)
            files2create.append(readcache2)
        else:
            readcache2 = ''
        failovercache = '{}/foc_{}'.format(mountpoint_foc, vpool_name)
        metadatapath = '{}/metadata_{}'.format(mountpoint_md, vpool_name)
        tlogpath = '{}/tlogs_{}'.format(mountpoint_md, vpool_name)
        rsppath = '/var/rsp/{}'.format(vpool_name)
        dirs2create = [scocache, failovercache, metadatapath, tlogpath, rsppath,
                       System.read_remote_config(client, 'volumedriver.readcache.serialization.path')]

        cmd = "cat /etc/mtab | grep ^/dev/ | cut -d ' ' -f 2"
        mountpoints = [device.strip() for device in client.run(cmd).strip().split('\n')]
        mountpoints.remove('/')

        def is_partition(directory):
            for mountpoint in mountpoints:
                if directory == mountpoint:
                    return True
            return False
        # Cache sizes
        # 20% = scocache
        # 20% = failovercache (@TODO: check if this can possibly consume more than 20%)
        # 60% = readcache

        # safety values:
        readcache1_factor = 0.2
        readcache2_factor = 0.2
        writecache_factor = 0.1

        if (mountpoint_readcache1 == mountpoint_readcache2) or not mountpoint_readcache2:
            delta = set()
            delta.add(mountpoint_readcache1 if is_partition(mountpoint_readcache1) else '/dummy')
            delta.add(mountpoint_writecache if is_partition(mountpoint_writecache) else '/dummy')
            delta.add(mountpoint_foc if is_partition(mountpoint_foc) else '/dummy')
            if len(delta) == 1:
                readcache1_factor = 0.49
                writecache_factor = 0.2
            elif len(delta) == 2:
                if mountpoint_writecache == mountpoint_foc:
                    readcache1_factor = 0.98
                    writecache_factor = 0.49
                else:
                    readcache1_factor = 0.49
                    if mountpoint_readcache1 == mountpoint_writecache:
                        writecache_factor = 0.49
                    else:
                        writecache_factor = 0.98
            elif len(delta) == 3:
                readcache1_factor = 0.98
                writecache_factor = 0.98
        else:
            delta = set()
            delta.add(mountpoint_readcache1 if is_partition(mountpoint_readcache1) else '/dummy')
            delta.add(mountpoint_readcache2 if is_partition(mountpoint_readcache2) else '/dummy')
            delta.add(mountpoint_writecache if is_partition(mountpoint_writecache) else '/dummy')
            delta.add(mountpoint_foc if is_partition(mountpoint_foc) else '/dummy')
            if len(delta) == 1:
                # consider them all to be directories
                readcache1_factor = 0.24
                readcache2_factor = 0.24
                writecache_factor = 0.24
            elif len(delta) == 2:
                if mountpoint_writecache == mountpoint_foc:
                    writecache_factor = 0.24
                    if mountpoint_readcache1 == mountpoint_writecache:
                        readcache1_factor = 0.49
                        readcache2_factor = 0.98
                    else:
                        readcache1_factor = 0.98
                        readcache2_factor = 0.49
                else:
                    readcache1_factor = readcache2_factor = 0.49
                    writecache_factor = 0.49
            elif len(delta) == 3:
                if mountpoint_writecache == mountpoint_foc:
                    readcache1_factor = 0.98
                    readcache2_factor = 0.98
                    writecache_factor = 0.49
                elif mountpoint_readcache1 == mountpoint_writecache:
                    readcache1_factor = 0.49
                    readcache2_factor = 0.98
                    writecache_factor = 0.49
                elif mountpoint_readcache1 == mountpoint_foc:
                    readcache1_factor = 0.49
                    readcache2_factor = 0.98
                    writecache_factor = 0.98
                elif mountpoint_readcache2 == mountpoint_writecache:
                    readcache1_factor = 0.98
                    readcache2_factor = 0.49
                    writecache_factor = 0.49
                elif mountpoint_readcache2 == mountpoint_foc:
                    readcache1_factor = 0.98
                    readcache2_factor = 0.49
                    writecache_factor = 0.98
            elif len(delta) == 4:
                readcache1_factor = 0.98
                readcache2_factor = 0.98
                writecache_factor = 0.98

        # summarize caching on root partition (directory only)
        root_assigned = dict()
        if not is_partition(mountpoint_readcache1):
            root_assigned['readcache1_factor'] = readcache1_factor
        if not is_partition(mountpoint_readcache2):
            root_assigned['readcache2_factor'] = readcache2_factor
        if not is_partition(mountpoint_writecache):
            root_assigned['writecache_factor'] = writecache_factor
        if not is_partition(mountpoint_foc):
            root_assigned['foc_factor'] = min(readcache1_factor, readcache2_factor, writecache_factor)

        # always leave at least 20% of free space
        division_factor = 1.0
        total_size = sum(root_assigned.values()) + .02 * len(root_assigned)
        if 0.8 < total_size < 1.6:
            division_factor = 2.0
        elif 1.6 < total_size < 3.2:
            division_factor = 4.0
        elif total_size >= 3.2:
            division_factor = 8.0

        if 'readcache1_factor' in root_assigned.keys():
            readcache1_factor /= division_factor
        if 'readcache2_factor' in root_assigned.keys():
            readcache2_factor /= division_factor
        if 'writecache_factor' in root_assigned.keys():
            writecache_factor /= division_factor

        scocache_size = '{0}KiB'.format((int(write_cache_fs.f_bavail * writecache_factor / 4096) * 4096) * 4)
        if (mountpoint_readcache1 and not mountpoint_readcache2) or (mountpoint_readcache1 == mountpoint_readcache2):
            mountpoint_readcache2 = ''
            readcache1_size = '{0}KiB'.format((int(read_cache1_fs.f_bavail * readcache1_factor / 4096) * 4096) * 4)
            readcache2 = ''
            readcache2_size = '0KiB'
        else:
            readcache1_size = '{0}KiB'.format((int(read_cache1_fs.f_bavail * readcache1_factor / 4096) * 4096) * 4)
            readcache2_size = '{0}KiB'.format((int(read_cache2_fs.f_bavail * readcache2_factor / 4096) * 4096) * 4)
        if new_storagedriver:
            ports_used_in_model = [port_storagedriver.port for port_storagedriver in
                                   StorageDriverList.get_storagedrivers_by_storagerouter(storagerouter.guid)]
            vrouter_port_in_hrd = int(System.read_remote_config(client, 'volumedriver.filesystem.xmlrpc.port'))
            if vrouter_port_in_hrd in ports_used_in_model:
                vrouter_port = int(parameters['vrouter_port'])
            else:
                vrouter_port = int(vrouter_port_in_hrd)
        else:
            vrouter_port = int(storagedriver.port)

        cmd = "ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1"
        ipaddresses = client.run(cmd).strip().split('\n')
        ipaddresses = [ipaddr.strip() for ipaddr in ipaddresses]
        grid_ip = System.read_remote_config(client, 'ovs.grid.ip')
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
        voldrv_arakoon_cluster_id = str(System.read_remote_config(client, 'volumedriver.arakoon.clusterid'))
        voldrv_arakoon_cluster = ArakoonManagementEx().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
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
                                                      existing_storagedriver.port - 1,
                                                      existing_storagedriver.port,
                                                      existing_storagedriver.port + 1))
        if new_storagedriver:
            node_configs.append(ClusterNodeConfig(vrouter_id, grid_ip,
                                                  vrouter_port - 1, vrouter_port, vrouter_port + 1))
        vrouter_clusterregistry.set_node_configs(node_configs)
        readcaches = [{'path': readcache1, 'size': readcache1_size}]
        if readcache2:
            readcaches.append({'path': readcache2, 'size': readcache2_size})
        scocaches = [{'path': scocache, 'size': scocache_size}]
        filesystem_config = {'fs_backend_path': mountpoint_bfs}
        volumemanager_config = {'metadata_path': metadatapath, 'tlog_path': tlogpath}
        storagedriver_config_script = """
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration

fd_config = {{'fd_cache_path': '{11}',
              'fd_extent_cache_capacity': '1024',
              'fd_namespace' : 'fd-{0}-{12}'}}
storagedriver_configuration = StorageDriverConfiguration('{0}')
storagedriver_configuration.configure_backend({1})
storagedriver_configuration.configure_readcache({2}, Configuration.get('volumedriver.readcache.serialization.path') + '/{0}')
storagedriver_configuration.configure_scocache({3}, '1GB', '2GB')
storagedriver_configuration.configure_failovercache('{4}')
storagedriver_configuration.configure_filesystem({5})
storagedriver_configuration.configure_volumemanager({6})
storagedriver_configuration.configure_volumerouter('{12}', {7})
storagedriver_configuration.configure_arakoon_cluster('{8}', {9})
storagedriver_configuration.configure_hypervisor('{10}')
storagedriver_configuration.configure_filedriver(fd_config)
""".format(vpool_name, vpool.metadata, readcaches, scocaches, failovercache, filesystem_config,
           volumemanager_config, vrouter_config, voldrv_arakoon_cluster_id, voldrv_arakoon_client_config,
           storagerouter.pmachine.hvtype, fdcache, vpool.guid)
        System.exec_remote_python(client, storagedriver_config_script)
        remote_script = """
import os
from configobj import ConfigObj
from ovs.plugin.provider.configuration import Configuration
protocol = Configuration.get('ovs.core.broker.protocol')
login = Configuration.get('ovs.core.broker.login')
password = Configuration.get('ovs.core.broker.password')
vpool_name = {0}
uris = []
cfg = ConfigObj('/opt/OpenvStorage/config/rabbitmqclient.cfg')
main_section = cfg.get('main')
nodes = main_section['nodes'] if type(main_section['nodes']) == list else [main_section['nodes']]
for node in nodes:
    uris.append({{'amqp_uri': '{{0}}://{{1}}:{{2}}@{{3}}'.format(protocol, login, password, cfg.get(node)['location'])}})
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
queue_config = {{'events_amqp_routing_key': Configuration.get('ovs.core.broker.volumerouter.queue'),
                 'events_amqp_uris': uris}}
for config_file in os.listdir('/opt/OpenvStorage/config/voldrv_vpools'):
    this_vpool_name = config_file.replace('.json', '')
    if config_file.endswith('.json') and (vpool_name is None or vpool_name == this_vpool_name):
        storagedriver_configuration = StorageDriverConfiguration(this_vpool_name)
        storagedriver_configuration.configure_event_publisher(queue_config)
""".format(vpool_name if vpool_name is None else "'{0}'".format(vpool_name))
        System.exec_remote_python(client, remote_script)

        # Updating the model
        storagedriver.storagedriver_id = vrouter_id
        storagedriver.name = vrouter_id.replace('_', ' ')
        storagedriver.description = storagedriver.name
        storagedriver.storage_ip = volumedriver_storageip
        storagedriver.cluster_ip = grid_ip
        storagedriver.port = vrouter_port
        storagedriver.mountpoint = '/mnt/{0}'.format(vpool_name)
        storagedriver.mountpoint_temp = mountpoint_temp
        storagedriver.mountpoint_readcache1 = mountpoint_readcache1
        storagedriver.mountpoint_readcache2 = mountpoint_readcache2
        storagedriver.mountpoint_writecache = mountpoint_writecache
        storagedriver.mountpoint_foc = mountpoint_foc
        storagedriver.mountpoint_bfs = mountpoint_bfs
        storagedriver.mountpoint_md = mountpoint_md
        storagedriver.storagerouter = storagerouter
        storagedriver.vpool = vpool
        storagedriver.save()

        dirs2create.append(storagedriver.mountpoint)
        dirs2create.append(mountpoint_writecache + '/' + '/fd_' + vpool_name)
        dirs2create.append('{0}/fd_{1}'.format(mountpoint_writecache, vpool_name))

        file_create_script = """
import os
for directory in {0}:
    if not os.path.exists(directory):
        os.makedirs(directory)
for filename in {1}:
    if not os.path.exists(filename):
        open(filename, 'a').close()
""".format(dirs2create, files2create)
        System.exec_remote_python(client, file_create_script)

        voldrv_config_file = '{0}/voldrv_vpools/{1}.json'.format(System.read_remote_config(client, 'ovs.core.cfgdir'),
                                                                 vpool_name)
        log_file = '/var/log/ovs/volumedriver/{0}.log'.format(vpool_name)
        vd_cmd = '/usr/bin/volumedriver_fs -f --config-file={0} --mountpoint {1} --logrotation --logfile {2} -o big_writes -o sync_read -o allow_other -o default_permissions'.format(
            voldrv_config_file, storagedriver.mountpoint, log_file)
        if storagerouter.pmachine.hvtype == 'KVM':
            vd_stopcmd = 'umount {0}'.format(storagedriver.mountpoint)
        else:
            vd_stopcmd = 'exportfs -u *:{0}; umount {0}'.format(storagedriver.mountpoint)
        vd_name = 'volumedriver_{}'.format(vpool_name)

        log_file = '/var/log/ovs/volumedriver/foc_{0}.log'.format(vpool_name)
        fc_cmd = '/usr/bin/failovercachehelper --config-file={0} --logfile={1}'.format(voldrv_config_file, log_file)
        fc_name = 'failovercache_{0}'.format(vpool_name)

        params = {'<VPOOL_MOUNTPOINT>': storagedriver.mountpoint,
                  '<HYPERVISOR_TYPE>': storagerouter.pmachine.hvtype,
                  '<VPOOL_NAME>': vpool_name,
                  '<UUID>': str(uuid.uuid4())}

        if client.file_exists('/opt/OpenvStorage/config/templates/upstart/ovs-volumedriver.conf'):
            client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-volumedriver.conf /opt/OpenvStorage/config/templates/upstart/ovs-volumedriver_{0}.conf'.format(vpool_name))
            client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-failovercache.conf /opt/OpenvStorage/config/templates/upstart/ovs-failovercache_{0}.conf'.format(vpool_name))

        service_script = """
from ovs.plugin.provider.service import Service
Service.add_service(package=('openvstorage', 'volumedriver'), name='{0}', command='{1}', stop_command='{2}', params={5})
Service.add_service(package=('openvstorage', 'failovercache'), name='{3}', command='{4}', stop_command=None, params={5})
""".format(
            vd_name, vd_cmd, vd_stopcmd,
            fc_name, fc_cmd, params
        )
        System.exec_remote_python(client, service_script)

        if storagerouter.pmachine.hvtype == 'VMWARE':
            client.run("grep -q '/tmp localhost(ro,no_subtree_check)' /etc/exports || echo '/tmp localhost(ro,no_subtree_check)' >> /etc/exports")
            client.run('service nfs-kernel-server start')

        if storagerouter.pmachine.hvtype == 'KVM':
            client.run('virsh pool-define-as {0} dir - - - - {1}'.format(vpool_name, storagedriver.mountpoint))
            client.run('virsh pool-build {0}'.format(vpool_name))
            client.run('virsh pool-start {0}'.format(vpool_name))
            client.run('virsh pool-autostart {0}'.format(vpool_name))

        # Start services
        for node in nodes:
            node_client = SSHClient.load(node)
            for service in services:
                System.exec_remote_python(node_client, """
from ovs.plugin.provider.service import Service
Service.enable_service('{0}')
""".format(service))
                System.exec_remote_python(node_client, """
from ovs.plugin.provider.service import Service
Service.start_service('{0}')
""".format(service))

        # Fill vPool size
        vfs_info = os.statvfs('/mnt/{0}'.format(vpool_name))
        vpool.size = vfs_info.f_blocks * vfs_info.f_bsize
        vpool.save()

    @staticmethod
    @celery.task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(storagedriver_guid):
        """
        Removes a Storage Driver (and, if it was the last Storage Driver for a vPool, the vPool is removed as well)
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

        services = ['volumedriver_{0}'.format(vpool.name),
                    'failovercache_{0}'.format(vpool.name)]
        storagedrivers_left = False

        # Stop services
        for current_storagedriver in vpool.storagedrivers:
            if current_storagedriver.guid != storagedriver_guid:
                storagedrivers_left = True
            client = SSHClient.load(current_storagedriver.storagerouter.ip)
            for service in services:
                System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.disable_service('{0}')
""".format(service))
                System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.stop_service('{0}')
""".format(service))

        # KVM pool
        client = SSHClient.load(ip)
        if pmachine.hvtype == 'KVM':
            if vpool.name in client.run('virsh pool-list'):
                client.run('virsh pool-destroy {0}'.format(vpool.name))
            try:
                client.run('virsh pool-undefine {0}'.format(vpool.name))
            except:
                pass  # Ignore undefine errors, since that can happen on re-entrance

        # Remove services
        client = SSHClient.load(ip)
        for service in services:
            System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.remove_service(domain='openvstorage', name='{0}')
""".format(service))
        configuration_dir = System.read_remote_config(client, 'ovs.core.cfgdir')

        voldrv_arakoon_cluster_id = str(System.read_remote_config(client, 'volumedriver.arakoon.clusterid'))
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
                                                          current_storagedriver.port - 1, current_storagedriver.port,
                                                          current_storagedriver.port + 1))
            vrouter_clusterregistry.set_node_configs(node_configs)
        else:
            try:
                storagedriver_client = LocalStorageRouterClient('{0}/voldrv_vpools/{1}.json'.format(configuration_dir, vpool.name))
                storagedriver_client.destroy_filesystem()
                vrouter_clusterregistry.erase_node_configs()
            except RuntimeError as ex:
                print('Could not destroy filesystem or erase node configs due to error: {}'.format(ex))

        # Cleanup directories
        client = SSHClient.load(ip)
        client.run('rm -rf {}/read1_{}'.format(storagedriver.mountpoint_readcache1, vpool.name))
        if storagedriver.mountpoint_readcache2:
            client.run('rm -rf {}/read2_{}'.format(storagedriver.mountpoint_readcache2, vpool.name))
        client.run('rm -rf {}/sco_{}'.format(storagedriver.mountpoint_writecache, vpool.name))
        client.run('rm -rf {}/foc_{}'.format(storagedriver.mountpoint_foc, vpool.name))
        client.run('rm -rf {}/fd_{}'.format(storagedriver.mountpoint_writecache, vpool.name))
        client.run('rm -rf {}/metadata_{}'.format(storagedriver.mountpoint_md, vpool.name))
        client.run('rm -rf {}/tlogs_{}'.format(storagedriver.mountpoint_md, vpool.name))
        client.run('rm -rf /var/rsp/{}'.format(vpool.name))

        # Remove files
        client.run('rm -f {0}/voldrv_vpools/{1}.json'.format(configuration_dir, vpool.name))

        # Remove top directories
        client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(storagedriver.mountpoint_readcache1))
        if storagedriver.mountpoint_readcache2:
            client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(storagedriver.mountpoint_readcache2))
        client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(storagedriver.mountpoint_writecache))
        client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(storagedriver.mountpoint_foc))
        client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(storagedriver.mountpoint_md))
        client.run('if [ -d {0} ] && [ ! "$(ls -A {0})" ]; then rmdir {0}; fi'.format(storagedriver.mountpoint))

        # First model cleanup
        storagedriver.delete()

        if storagedrivers_left:
            # Restart leftover services
            for current_storagedriver in vpool.storagedrivers:
                if current_storagedriver.guid != storagedriver_guid:
                    client = SSHClient.load(current_storagedriver.storagerouter.ip)
                    for service in services:
                        System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.enable_service('{0}')
""".format(service))
                        System.exec_remote_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.start_service('{0}')
""".format(service))
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
    def _validate_ip(ip):
        regex = '^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$'
        match = re.search(regex, ip)
        return match is not None

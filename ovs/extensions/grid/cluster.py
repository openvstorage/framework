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

import json
import shutil
import socket
import os
import re
import uuid
import subprocess
from configobj import ConfigObj
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.console import Console
from ovs.plugin.provider.service import Service
from ovs.plugin.provider.package import Package
from ovs.plugin.provider.remote import Remote
from ovs.plugin.provider.tools import Tools
from ovs.plugin.provider.osis import Osis
from ovs.plugin.provider.net import Net

from ovs.manage import Control
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from JumpScale import j

class ConfigHelper(object):
    """
    Cluster Configuration Helper class
    """
    def __init__(self):
        self._grid_config_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'grid')

    def generate_arakoon_config(self, config, config_dir, extend_config):
        """
        We need to compile the
        * ovsdb.cfg/ovsdb_client.cfg
        * voldrv.cfg/voldrv_client.cfg
        * *_local_nodes.cfg is not required as does not require any updates
        """
        from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
        ArakoonManagement = ArakoonManagement()
        arakoon_clients_grid_config = {}
        node_added = False
        for cluster in ArakoonManagement.listClusters():
            cluster_config_dir = os.path.join(config_dir, 'arakoon', cluster)
            if not os.path.exists(cluster_config_dir):
                os.makedirs(cluster_config_dir)
            arakoon_cluster = ArakoonManagement.getCluster(cluster)
            arakoon_cluster_client_config = arakoon_cluster.getClientConfig()
            if config.has_key(cluster):
                if extend_config and not str(config[cluster]['name']) in arakoon_cluster.listNodes():
                    arakoon_cluster.addNode(name = str(config[cluster]['name']),
                                            ip = config[cluster]['ip'],
                                            clientPort = int(config[cluster]['client_port']),
                                            messagingPort = int(config[cluster]['messaging_port']),
                                            logLevel = config[cluster]['log_level'],
                                            logDir = config[cluster]['log_dir'],
                                            home = config[cluster]['home'],
                                            tlogDir = config[cluster]['tlog_dir'],
                                            wrapper = None,
                                            isLearner = False,
                                            targets = None,
                                            isLocal = False,
                                            logConfig = None,
                                            batchedTransactionConfig = None,
                                            tlfDir = None,
                                            headDir = None,
                                            configFilename = os.path.join(cluster_config_dir, '{}.cfg'.format(cluster)))
                    node_added = True
                arakoon_cluster_client_config.update({config[cluster]['name'] : ([config[cluster]['ip'],], config[cluster]['client_port'])})
            arakoon_clients_grid_config[cluster] = arakoon_cluster_client_config
            arakoon_cluster.writeClientConfig(config = arakoon_cluster_client_config,
                                              configFilename = os.path.join(cluster_config_dir, '{}_client.cfg'.format(cluster)))
            arakoon_local_node_config = ConfigObj(os.path.join(cluster_config_dir, '{}_local_nodes.cfg'.format(cluster)))
            arakoon_local_node_config.update({'global': {'cluster': str(config[cluster]['name'])}})
            arakoon_local_node_config.write()
            if not node_added:
                shutil.copyfile(os.path.join(arakoon_cluster._getConfigFilePath(), '{}.cfg'.format(cluster)), os.path.join(cluster_config_dir, '{}.cfg'.format(cluster)))

        return arakoon_clients_grid_config


    def generate_grid_config(self, filename, config, config_dir, extend_config):
        grid_client_config = []
        client_grid_config_file = os.path.join(config_dir, filename)
        client_config_file = os.path.join(Configuration.get('ovs.core.cfgdir'), filename)
        if os.path.exists(client_grid_config_file):
            os.remove(client_grid_config_file)
        ini = ConfigObj(client_config_file)
        grid_ini = ConfigObj(client_grid_config_file)
        if 'main' in ini and 'nodes' in ini.get('main'):
            nodes = ini.get('main')['nodes'] if type(ini.get('main')['nodes']) == list else [ini.get('main')['nodes'],]
        else:
            nodes = []
        all_nodes = list(nodes)
        if extend_config and not config['name'] in all_nodes:
            all_nodes.append(config['name'])
        grid_ini.update({'main': {'nodes': all_nodes}})
        for node in nodes:
            grid_ini.update({node: {'location': ini.get(node)['location']}})
        if extend_config:
            grid_ini.update({config['name']: {'location': '{}:{}'.format(config['ip'], config['port'])}})
        print grid_ini
        grid_ini.write()


class Cluster(object):
    def __init__(self):
        self._config_helper = ConfigHelper()
        self.cuapi = Remote.cuisine.api
        self.localnode = socket.gethostname()
        self.template_hrd_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'grid', 'hrd', 'template')
        self.control = Control()
        self.osis_client = Osis.getClient()

    def join_rabbitmq_cluster(self, master):
        join_cmd = """from ovs.manage import Control
c = Control()
c.init_rabbitmq(cluster_to_join = {})
""".format(master)
        print self.cuapi.run('python -c "{}"'.format(join_cmd))

    def stop_ovs_service(self, service, remote=False):
        if remote:
            print self.cuapi.run('jpackage_stop -n {}'.format(service))
            return
        subprocess.call(['jpackage_stop', '-n', service])

    def start_ovs_service(self, service, remote=False):
        if remote:
            print self.client.cmd('jpackage_start -n {}'.format(service))
            return
        subprocess.call(['jpackage_start', '-n', service])

    def _create_filesystems(self):
        # Create partitions on HDD
        self.cuapi.run('parted /dev/sdb -s mklabel gpt')
        self.cuapi.run('parted /dev/sdb -s mkpart backendfs 2MB 80%')
        self.cuapi.run('parted /dev/sdb -s mkpart distribfs 80% 90%')
        self.cuapi.run('parted /dev/sdb -s mkpart tempfs 90% 100%')
        self.cuapi.run('mkfs.ext4 /dev/sdb1 -L backendfs')
        self.cuapi.run('mkfs.ext4 /dev/sdb2 -L distribfs')
        self.cuapi.run('mkfs.ext4 -q /dev/sdb3 -L tempfs')
    
        #Create partitions on SSD
        self.cuapi.run('parted /dev/sdc -s mklabel gpt')
        self.cuapi.run('parted /dev/sdc -s mkpart cache 2MB 50%')
        self.cuapi.run('parted /dev/sdc -s mkpart db 50% 75%')
        self.cuapi.run('parted /dev/sdc -s mkpart mdpath 75% 100%')
        self.cuapi.run('mkfs.ext4 /dev/sdc1 -L cache')
        self.cuapi.run('mkfs.ext4 /dev/sdc2 -L db')
        self.cuapi.run('mkfs.ext4 /dev/sdc3 -L mdpath')
        self.cuapi.run('mkdir /mnt/db')
        self.cuapi.run('mkdir /mnt/cache')
        self.cuapi.run('mkdir /mnt/md')
        self.cuapi.run('mkdir /mnt/bfs')
        self.cuapi.run('mkdir /mnt/dfs')
    
        # Add content to fstab
        new_filesystems = """
# BEGIN Open vStorage
LABEL=db        /mnt/db    ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=cache     /mnt/cache ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=mdpath    /mnt/md    ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=backendfs /mnt/bfs   ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=distribfs /mnt/dfs   ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=tempfs    /var/tmp   ext4    defaults,nobootwait,noatime,discard    0    2
# END Open vStorage
"""
        must_update = False
        fstab_content = self.cuapi.file_read('/etc/fstab')
        fstab_content_list = fstab_content.splitlines()
        if not '# BEGIN Open vStorage' in fstab_content_list:
            fstab_content += '\n'
            fstab_content += new_filesystems
            must_update = True
        if must_update:
            self.cuapi.file_write('/etc/fstab', fstab_content)
        self.cuapi.run('mountall')

    def _install_jscore(self):
        print self.cuapi.package_install('python-pip')
        print self.cuapi.run("pip install https://bitbucket.org/jumpscale/jumpscale_core/get/default.zip")
        print self.cuapi.dir_ensure("/opt/jumpscale/cfg/jsconfig/", True)
        print self.cuapi.dir_ensure("/opt/jumpscale/cfg/jpackages/", True)
        print self.cuapi.file_upload("/opt/jumpscale/cfg/jsconfig/blobstor.cfg","/opt/jumpscale/cfg/jsconfig/blobstor.cfg")
        print self.cuapi.file_upload("/opt/jumpscale/cfg/jsconfig/bitbucket.cfg", "/opt/jumpscale/cfg/jsconfig/bitbucket.cfg")
        print self.cuapi.file_upload("/opt/jumpscale/cfg/jpackages/sources.cfg","/opt/jumpscale/cfg/jpackages/sources.cfg")
        print self.cuapi.run("jpackage_update")
        print self.cuapi.run("jpackage_install -n core")

    def _push_hrds(self, remote_ip):
        
        template_hrd = [os.path.basename(item)[:-4] for item in os.listdir(self.node_hrd_dir) if item.find(".hrd")<>-1]
        hrd_to_copy = [os.path.basename(item)[:-4] for item in os.listdir("/opt/jumpscale/cfg/hrd") if item.find(".hrd")<>-1]

        self.cuapi.connect(remote_ip)
        for hrdname in template_hrd:
            print self.cuapi.file_upload("/opt/jumpscale/cfg/hrd/{}.hrd".format(hrdname), os.path.join(self.node_hrd_dir, '{}.hrd'.format(hrdname)))
            
        for hrdname in set(hrd_to_copy).difference(set(template_hrd)):
            hrd_path = os.path.join(os.sep, 'opt', 'jumpscale', 'cfg', 'hrd', '{}.hrd'.format(hrdname))
            print self.cuapi.file_upload(hrd_path, hrd_path)

    def _push_config(self, configs, node_config_dir, update_local=True, nodes=[]):
        if 'arakoon' in configs:
            arakoon_grid_config_dir = os.path.join(node_config_dir, 'arakoon')
            for dir in os.listdir(arakoon_grid_config_dir):
                active_config_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'arakoon', dir)
                for file in os.listdir(os.path.join(arakoon_grid_config_dir, dir)):
                    for node in nodes:
                        self.cuapi.connect(node)
                        node_machineid = self.cuapi.run('python -c """from JumpScale import j; print j.application.getUniqueMachineId()"""')
                        # Do not upload the local_node file to nodes for which it was not generated, only new node should get it.
                        if file == '{}_local_nodes.cfg'.format(dir) and node_config_dir.find(node_machineid) == -1: continue
                        self.cuapi.dir_ensure(active_config_dir, True)
                        src_file = os.path.join(arakoon_grid_config_dir, dir, file)
                        dst_file = os.path.join(active_config_dir, file)
                        self.cuapi.file_upload(dst_file, src_file)
                    if update_local and file != '{}_local_nodes.cfg'.format(dir):
                        shutil.copyfile(src_file, dst_file)
            configs.remove('arakoon')
        for config in configs:
            src_file = os.path.join(node_config_dir, config)
            dst_file = os.path.join(Configuration.get('ovs.core.cfgdir'), config)
            for node in nodes:
                self.cuapi.connect(node)
                self.cuapi.dir_ensure(os.path.dirname(dst_file), True)
                self.cuapi.file_upload(dst_file, src_file)
                #Add code to transfer /var/lib/rabbitmq/.erlang.cookie which is required for new node to properly join the cluster
                rabbitmq_cookie_file = os.path.join(os.sep, 'var', 'lib', 'rabbitmq', '.erlang.cookie')
                self.cuapi.dir_ensure(os.path.dirname(rabbitmq_cookie_file), True)
                self.cuapi.file_upload(rabbitmq_cookie_file, rabbitmq_cookie_file)
                self.cuapi.file_attribs(rabbitmq_cookie_file, mode=400)
            if update_local:
                shutil.copyfile(src_file, dst_file)
    
    def _remote_control_init(self, vpool_name, services, master):
        """
        Init the new node with given params
        """
        init_cmd = """from ovs.manage import Control
Control = Control()
Control.init(\'{}\',{},\'{}\')
""".format(vpool_name, services, master)
        print self.cuapi.run("source /etc/profile; /opt/OpenvStorage/bin/python -c \"{}\"".format(init_cmd))

    def _get_cluster_nodes(self):
        grid_nodes = []
        local_addresses = Net.getIpAddresses()
        local_ovs_grid_ip = Configuration.get('ovs.grid.ip')
        grid_id = Configuration.getInt('grid.id')
        osis_client_node = Osis.getClientForCategory(self.osis_client, 'system', 'node')
        for node_key in osis_client_node.list():
            node = osis_client_node.get(node_key)
            if node.gid != grid_id: continue
            ip_found = False
            for ip in node.ipaddr:
                if Net.getReachableIpAddress(ip, 80) == local_ovs_grid_ip:
                    grid_nodes.append(ip)
                    ip_found = True
                    break
            if not ip_found:
                raise RuntimeError('No suitable ip address found for node %s'%node.machineguid)
        print grid_nodes
        grid_nodes.remove(local_ovs_grid_ip)
        return grid_nodes

    def _execute_on_clusternodes(self, command, interpreter='bash', nodes=[]):
        """
        Run the command on all nodes in the cluster
        """
        for node in nodes:
            if node in self.nodes:
                self.cuapi.connect(node)
                if interpreter == 'python':
                    self.cuapi.run('/opt/OpenvStorage/bin/python -c """%s"""'%command)
                else:
                    self.cuapi.run(command)

    def initializeNode(self, vpool, remote_ip, passwd, seedpasswd):
        """
        Initialize a remote node to join the Open vStorage cluster
        
        @param vpool: name of the vpool to initialize node for
        @param remote_ip: ip addres of the new node to join
        @param passwd: password to set on the new node
        @param seedpasswd: current password of the new node
        """
        self.nodes = self._get_cluster_nodes()
        self.remote_ip = remote_ip
        configs_to_push = ['memcacheclient.cfg', 'rabbitmqclient.cfg']
        local_configs_to_push = []
        Remote.cuisine.fabric.env["password"]=passwd
        self.cuapi.connect(remote_ip)
        vpool_cache_mountpoint = Console.askString('Specify vpool cache mountpoint','/mnt/cache')
        volumedriver_backend_type = Console.askChoice(Configuration.get('volumedriver.supported.backends').split(','), 'Select type of storage backend')
        volumedriver_local_filesystem = None
        connection_host = None
        connection_port = None
        connection_username = None
        connection_password = None
        rest_connection_timeout_secs = None
        if volumedriver_backend_type == 'LOCAL':
            volumedriver_local_filesystem = Console.askString('Select mountpoint for local backend', '/mnt/bfs')
        elif volumedriver_backend_type == 'REST':
            connection_host = Console.askString('Provide REST ip address')
            connection_port = Console.askInteger('Provide REST connection port')
            rest_connection_timeout_secs = Console.askInteger('Provide desired REST connection timeout(secs)')
        elif volumedriver_backend_type == 'S3':
            connection_host = Console.askString('Specify fqdn or ip address for your S3 host')
            connection_port = Console.askInteger('Specify port for your S3 compatible host')
            connection_username = Console.askString('Specify S3 access key')
            connection_password = Console.askString('Specify S3 secret key')
        mount_vpool = Console.askYesNo('Do you want to mount the vPool?')
        create_filesystems = Console.askYesNo('Create filesystems')
           
        """
        Make sure new password gets set
        """
        node=Tools.expect.new("sh")
        node.login(remote=remote_ip,passwd=passwd,seedpasswd=seedpasswd)
          
        if create_filesystems:
            self._create_filesystems()
        Remote.cuisine.api.run('apt-get update')
        self._install_jscore()
 
        unique_machine_id = Remote.cuisine.api.run('python -c """from JumpScale import j; print j.application.getUniqueMachineId()"""')
        machine_hostname = Remote.cuisine.api.run('python -c "import socket; print socket.gethostname()"')

        """
        Retrieve info of the current environment
        In the future this might all be asked to the jumpscale grid framework
        """
        arakoon_cluster = ArakoonManagement().getCluster('ovsdb')
        arakoon_nodes = arakoon_cluster.listNodes()
        extend_config = False
        if len(arakoon_nodes) < 3:
            extend_config = True

        """
        Build hrd's for new node
        """
        self.node_hrd_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'grid', 'hrd', 'node_{}'.format(unique_machine_id))
        self.node_cfg_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'grid', 'cfg', 'node_{}'.format(unique_machine_id))
        for dir in [self.node_hrd_dir, self.node_cfg_dir]:
            if os.path.exists(dir):
                shutil.rmtree(dir)
        shutil.copytree(self.template_hrd_dir, self.node_hrd_dir)
        node_hrd = Configuration.getHRD(self.node_hrd_dir)
        node_hrd.set('ovs.grid.ip', remote_ip)
        node_hrd.set('ovs.core.memcache.localnode.name', unique_machine_id)
        node_hrd.set('ovs.core.rabbitmq.localnode.name', unique_machine_id)
        node_hrd.set('ovs.core.db.arakoon.node.name', unique_machine_id)
        node_hrd.set('volumedriver.arakoon.node.name', unique_machine_id)
        node_hrd.set('ovs.core.db.mountpoint', Configuration.get('ovs.core.db.mountpoint'))
        node_hrd.set('ovs.core.tempfs.mountpoint', Configuration.get('ovs.core.tempfs.mountpoint'))
        node_hrd.set('volumedriver.filesystem.distributed', Configuration.get('volumedriver.filesystem.distributed'))
        node_hrd.set('ovs.host.name', Console.askString('Hypervisor host name '))
        node_hrd.set('ovs.host.hypervisor', Configuration.get('ovs.host.hypervisor'))
        node_hrd.set('ovs.host.ip', Console.askString('Hypervisor host ip '))
        node_hrd.set('ovs.host.login', Console.askString('Hypervisor host administrative login '))
        node_hrd.set('ovs.host.password', Console.askString('Hypervisor host administrative password: '))
        node_hrd.set('ovs.webapps.certificate.period', Configuration.get('ovs.webapps.certificate.period'))
        node_hrd.set('grid.master.ip', Configuration.get('grid.master.ip'))
        node_hrd.set('grid.id', Configuration.get('grid.id'))
        node_hrd.set('grid.node.machineguid', unique_machine_id)
        node_openvstorage_hrd = Configuration.getHRD(os.path.join(self.node_hrd_dir, 'openvstorage-core.hrd'))
        node_openvstorage_hrd.set('volumedriver.cache.mountpoint', vpool_cache_mountpoint)
        node_openvstorage_hrd.set('volumedriver.backend.type', volumedriver_backend_type)
        node_openvstorage_hrd.set('volumedriver.connection.host', connection_host)
        node_openvstorage_hrd.set('volumedriver.connection.port', connection_port)
        node_openvstorage_hrd.set('volumedriver.connection.username', connection_username)
        node_openvstorage_hrd.set('volumedriver.connection.password', connection_password)
        node_openvstorage_hrd.set('volumedriver.rest.timeout', rest_connection_timeout_secs)
        node_openvstorage_hrd.set('volumedriver.backend.mountpoint', volumedriver_local_filesystem)
        node_openvstorage_hrd.set('volumedriver.vpool.mount', mount_vpool)
        node_openvstorage_hrd.set('volumedriver.ip.storage', Configuration.get('volumedriver.ip.storage'))

        """
        Build new grid configuration for arakoon
        """
        new_node_ovsdb_hrd = {'client_port': '8870',
                              'fsync': 'true',
                              'home': os.path.join(Configuration.get('ovs.core.db.mountpoint'), 'arakoon', 'ovsdb'),
                              'ip': remote_ip,
                              'log_dir': '/var/log/arakoon/ovsdb',
                              'log_level': 'info',
                              'messaging_port': '8871',
                              'name': unique_machine_id,
                              'tlog_dir': os.path.join(Configuration.get('ovs.core.db.mountpoint'), 'tlogs', 'ovsdb')}
        
        new_node_volumedriver_hrd = {'client_port': '8872',
                                     'fsync': 'true',
                                     'home': os.path.join(Configuration.get('ovs.core.db.mountpoint'), 'arakoon', 'voldrv'),
                                     'ip': remote_ip,
                                     'log_dir': '/var/log/arakoon/voldrv',
                                     'log_level': 'info',
                                     'messaging_port': '8873',
                                     'name': unique_machine_id,
                                     'tlog_dir': os.path.join(Configuration.get('ovs.core.db.mountpoint'), 'tlogs', 'voldrv')}
        new_node_arakoon_config = {'ovsdb' : new_node_ovsdb_hrd,
                                   'voldrv' : new_node_volumedriver_hrd}
        
        arakoon_clients = self._config_helper.generate_arakoon_config(new_node_arakoon_config, self.node_cfg_dir, extend_config)
  
        """
        Build new grid configuration for memcache
        """
        new_node_memcache_hrd = {'ip': remote_ip, 'name': unique_machine_id, 'port': '11211'}
        self._config_helper.generate_grid_config('memcacheclient.cfg', new_node_memcache_hrd, self.node_cfg_dir, extend_config)

        """
        Build new grid configuration for rabbitmq brokers
        """
        new_node_rabbitmq_broker_hrd = {'ip': remote_ip, 'name': unique_machine_id, 'port': '5672'}
        self._config_helper.generate_grid_config('rabbitmqclient.cfg', new_node_rabbitmq_broker_hrd, self.node_cfg_dir, extend_config)

        """
        Stop OVS services on all nodes
        """
        if extend_config:
            subprocess.call(['service', 'processmanager', 'stop'])
            self.stop_ovs_service('volumedriver', remote=False)
            self.stop_ovs_service('openvstorage-webapps', remote=False)
            self.stop_ovs_service('openvstorage-core', remote=False)
            subprocess.call(['jsprocess', 'start', '-n', 'elasticsearch'])
            subprocess.call(['jsprocess', 'start', '-n', 'osis'])
            stop_ovs_processes = """
service processmanager stop
jsprocess stop
"""
            self._execute_on_clusternodes(stop_ovs_processes, interpreter='bash', nodes=self.nodes)


        """
        Build new volumedriver cluster configuration for existing vpool
        Prior to doing this the local node arakoon config needs to be updated
        
        @todo check if when the remote cache dir in different in size the volumedriver json file is update
              accordingly when running init of storagerouter on remote node.
        """
        VPOOL_REGEX = re.compile('(.*)\.json')
        existing_vpools = []
        
        config_destinations = [remote_ip,]
        if extend_config:
            if self.nodes:
                config_destinations.extend(self.nodes)
            self._push_config(configs=['arakoon',], node_config_dir=self.node_cfg_dir, update_local=True, nodes=config_destinations)
        
        voldrv_arakoon_cluster_id = Configuration.get('volumedriver.arakoon.clusterid')
        voldrv_arakoon_cluster = ArakoonManagement().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        for content in os.listdir(Configuration.get('ovs.core.cfgdir')):
            pool_match = VPOOL_REGEX.match(content)
            if pool_match:
                existing_vpools.append(pool_match.groups()[0])
        
        # @todo: Only the local node is taken into account to detect wether the vpool is already existing, this should actually be on the full grid.
        vrouter_ips = list()
        for vpool_name in existing_vpools:
            vsr_configuration = VolumeStorageRouterConfiguration(vpool_name)
            vsr_configuration.load_config()
            if extend_config:
                vsr_configuration.configure_arakoon_cluster(voldrv_arakoon_cluster_id, voldrv_arakoon_client_config)
                third_node_command = """
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
vsr_configuration = VolumeStorageRouterConfiguration('%(vpool)s')
vsr_configuration.configure_arakoon_cluster('%(cluster_id)s', %(client_config)s)
""" %{'vpool': vpool_name,
      'cluster_id': voldrv_arakoon_cluster_id,
      'client_config': voldrv_arakoon_client_config}
                self._execute_on_clusternodes(third_node_command, interpreter='python', nodes=self.nodes)
            local_vrouter_cluster = dict(vsr_configuration._config_file_content['volume_router_cluster'])
            vrouter_ips = map(lambda n: n['host'], local_vrouter_cluster['vrouter_cluster_nodes'])
            for ip in Net.getIpAddresses():
                if ip in vrouter_ips:
                    vrouter_ips.remove(ip)
            if vpool_name == vpool:
                vrouter_config = {"vrouter_id": '{}{}'.format(vpool_name, unique_machine_id),
                                  "vrouter_redirect_timeout_ms": "5000",
                                  "vrouter_migrate_timeout_ms" : "5000",
                                  "vrouter_write_threshold" : 1024,
                                  "host": remote_ip,
                                  "xmlrpc_port": 12323}
                vsr_configuration.configure_volumerouter(vpool_name, vrouter_config)
                shutil.copyfile(os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json'.format(vpool)), os.path.join(self.node_cfg_dir, '{}.json'.format(vpool)))
                #@todo: Following third_node command should only be executed on nodes running this vpool
                third_node_command = """
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration
vsr_configuration = VolumeStorageRouterConfiguration('%(vpool)s')
vsr_configuration.configure_volumerouter('%(vpool)s', %(vrouter_config)s) 
"""%{'vpool': vpool_name,
     'vrouter_config': vrouter_config}
                self._execute_on_clusternodes(third_node_command, interpreter='python', nodes=vrouter_ips)
                local_configs_to_push.append('{}.json'.format(vpool))
                self.cuapi.connect(remote_ip)
                self.cuapi.dir_ensure("/etc/ceph")
                self.cuapi.file_upload("/etc/ceph/ceph.conf","/etc/ceph/ceph.conf")
                self.cuapi.file_upload("/etc/ceph/ceph.keyring", "/etc/ceph/ceph.keyring")
                self.cuapi.file_attribs("/etc/ceph/ceph.keyring", mode=644)

        """
        Update new and local node with new config
        For now we update all components on this and the new node, but after the node joined the cluster,
        we should check how many nodes are in the cluster and only reconfigure certain components based on that
        Secondly we do not only need to update the new node and ourselfs but also a 2nd node already in place when adding nr 3.
        """
        self._push_hrds(remote_ip)
        self._push_config(configs=configs_to_push, node_config_dir=self.node_cfg_dir, update_local=True, nodes=config_destinations)
        self._push_config(configs=local_configs_to_push, node_config_dir=self.node_cfg_dir, update_local=False, nodes=[remote_ip,])

        """
        Update the local and remote hosts file.
        """
        # Update hosts file
        j.system.net.updateHostsFile(hostsfile='/etc/hosts', ip=remote_ip, hostname=machine_hostname)
        third_nodes_dict = {}
        for node in self.nodes:
            if node in Net.getIpAddresses(): continue
            self.cuapi.connect(node)
            self.cuapi.run('python -c "from JumpScale import j; j.system.net.updateHostsFile(hostsfile=\'/etc/hosts\', ip=\'{}\', hostname=\'{}\')"'.format(remote_ip, machine_hostname))
            third_nodes_dict[node] = self.cuapi.run('python -c "import socket; print socket.gethostname()"')
        
        self.cuapi.connect(remote_ip)
        self.cuapi.run('python -c "from JumpScale import j; j.system.net.updateHostsFile(hostsfile=\'/etc/hosts\', ip=\'{}\', hostname=\'{}\')"'.format(Configuration.get('ovs.grid.ip'), socket.gethostname()))
        for ip,nodename in third_nodes_dict.iteritems():
            self.cuapi.run('python -c "from JumpScale import j; j.system.net.updateHostsFile(hostsfile=\'/etc/hosts\', ip=\'{}\', hostname=\'{}\')"'.format(ip, nodename))

        """
        Install the required software
        """
        self.cuapi.connect(remote_ip)
        self.cuapi.run('apt-get -y -q install python-dev')
        self.cuapi.run('jpackage_install -n openvstorage')
        
        """
        Re-init the OpenvStorage Core / Webapps / Volumedriver
        To get this working the arakoon of the volumedriver needs to be managed by the core
        otherwise volumedriver bails out cause it can't connect as there is no master
        1. existing node
        2. new node
        3. other nodes
        """
        self.control._start_package('openvstorage-core')
        for node in vrouter_ips:
            self.cuapi.connect(node)
            self.cuapi.run('jpackage_start -n openvstorage-core')
        self.cuapi.connect(remote_ip)
        self._remote_control_init(vpool, services=['openvstorage-core'], master=socket.gethostname())
        
        #@todo Configure volumerouter cluster in arakoon, will be required by one of the next releases of volumedriver
        
        self.control._start_package('openvstorage-webapps')
        for node in vrouter_ips:
            self.cuapi.connect(node)
            self.cuapi.run('jpackage_start -n openvstorage-webapps')
        self.cuapi.connect(remote_ip)
        self._remote_control_init(vpool, services=['openvstorage-webapps'], master=socket.gethostname())
        
        self.control._start_package('volumedriver')
        for node in vrouter_ips:
            self.cuapi.connect(node)
            self.cuapi.run('jpackage_start -n volumedriver')
        self.cuapi.connect(remote_ip)
        self._remote_control_init(vpool, services=['volumedriver',], master=socket.gethostname())

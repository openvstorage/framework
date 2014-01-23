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

    def generate_arakoon_config(self, config, config_dir):
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
                if not str(config[cluster]['name']) in arakoon_cluster.listNodes():
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


    def generate_grid_config(self, filename, config, config_dir):
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
        if not config['name'] in all_nodes:
            all_nodes.append(config['name'])
            grid_ini.update({'main': {'nodes': all_nodes}})
        for node in nodes:
            grid_ini.update({node: {'location': ini.get(node)['location']}})
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

    def _push_hrds(self):
        
        template_hrd = [os.path.basename(item)[:-4] for item in os.listdir(self.node_hrd_dir) if item.find(".hrd")<>-1]
        hrd_to_copy = [os.path.basename(item)[:-4] for item in os.listdir("/opt/jumpscale/cfg/hrd") if item.find(".hrd")<>-1]

        for hrdname in template_hrd:
            print self.cuapi.file_upload("/opt/jumpscale/cfg/hrd/{}.hrd".format(hrdname), os.path.join(self.node_hrd_dir, '{}.hrd'.format(hrdname)))
            
        for hrdname in set(hrd_to_copy).difference(set(template_hrd)):
            hrd_path = os.path.join(os.sep, 'opt', 'jumpscale', 'cfg', 'hrd', '{}.hrd'.format(hrdname))
            print self.cuapi.file_upload(hrd_path, hrd_path)

    def _push_config(self, configs, node_config_dir, update_local=True):
        if 'arakoon' in configs:
            arakoon_grid_config_dir = os.path.join(node_config_dir, 'arakoon')
            for dir in os.listdir(arakoon_grid_config_dir):
                active_config_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'arakoon', dir)
                self.cuapi.dir_ensure(active_config_dir, True)
                for file in os.listdir(os.path.join(arakoon_grid_config_dir, dir)):
                    src_file = os.path.join(arakoon_grid_config_dir, dir, file)
                    dst_file = os.path.join(active_config_dir, file)
                    self.cuapi.file_upload(dst_file, src_file)
                    if update_local and file != '{}_local_nodes.cfg'.format(dir):
                        shutil.copyfile(src_file, dst_file)
            configs.remove('arakoon')
        for config in configs:
            src_file = os.path.join(node_config_dir, config)
            dst_file = os.path.join(Configuration.get('ovs.core.cfgdir'), config)
            self.cuapi.file_upload(dst_file, src_file)
            if update_local:
                shutil.copyfile(src_file, dst_file)
        #Add code to transfer /var/lib/rabbitmq/.erlang.cookie which is required for new node to properly join the cluster
        rabbitmq_cookie_file = os.path.join(os.sep, 'var', 'lib', 'rabbitmq', '.erlang.cookie')
        self.cuapi.dir_ensure(os.path.dirname(rabbitmq_cookie_file), True)
        self.cuapi.file_upload(rabbitmq_cookie_file, rabbitmq_cookie_file)
        self.cuapi.file_attribs(rabbitmq_cookie_file, mode=400)
    
    def _remote_control_init(self, vpool_name, services, master):
        """
        Init the new node with given params
        """
        init_cmd = """from ovs.manage import Control
Control = Control()
Control.init(\'{}\',{},\'{}\')
""".format(vpool_name, services, master)
        print self.cuapi.run("source /etc/profile; /opt/OpenvStorage/bin/python -c \"{}\"".format(init_cmd))

    def initializeNode(self, vpool, remote_ip, passwd, seedpasswd):
        """
        Initialize a remote node to join the Open vStorage cluster
        
        @param vpool: name of the vpool to initialize node for
        @param remote_ip: ip addres of the new node to join
        @param passwd: password to set on the new node
        @param seedpasswd: current password of the new node
        """
        configs_to_push = ['memcacheclient.cfg', 'rabbitmqclient.cfg']
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
            connection_host = Console.askString('Specify fqdn or ip of your s3 host')
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
        self._install_jscore()
 
        unique_machine_id = Remote.cuisine.api.run('python -c """from JumpScale import j; print j.application.getUniqueMachineId()"""')
        machine_hostname = Remote.cuisine.api.run('python -c "import socket; print socket.gethostname()"')
#         
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
        
        arakoon_clients = self._config_helper.generate_arakoon_config(new_node_arakoon_config, self.node_cfg_dir)
  
        """
        Build new grid configuration for memcache
        """
        new_node_memcache_hrd = {'ip': remote_ip, 'name': unique_machine_id, 'port': '11211'}
        self._config_helper.generate_grid_config('memcacheclient.cfg', new_node_memcache_hrd, self.node_cfg_dir)

        """
        Build new grid configuration for rabbitmq brokers
        """
        new_node_rabbitmq_broker_hrd = {'ip': remote_ip, 'name': unique_machine_id, 'port': '5672'}
        self._config_helper.generate_grid_config('rabbitmqclient.cfg', new_node_rabbitmq_broker_hrd, self.node_cfg_dir)

        """
        Stop OVS services on all nodes
         
        @todo: We need to stop processmanager as well or disable the autorestart of the processes, otherwise this might intervene and fail the node init
        """
        subprocess.call(['service', 'processmanager', 'stop'])
        self.stop_ovs_service('volumedriver', remote=False)
        self.stop_ovs_service('openvstorage-webapps', remote=False)
        self.stop_ovs_service('openvstorage-core', remote=False)


        """
        Build new volumedriver cluster configuration for existing vpool
        Prior to doing this the local node arakoon config needs to be updated
        
        @todo check if when the remote cache dir in different in size the volumedriver json file is update
              accordingly when running init of storagerouter on remote node.
        """
        VPOOL_REGEX = re.compile('(.*)\.json')
        existing_vpools = []
        
        self._push_config(configs=['arakoon',], node_config_dir=self.node_cfg_dir, update_local=True)
        
        voldrv_arakoon_cluster_id = Configuration.get('volumedriver.arakoon.clusterid')
        voldrv_arakoon_cluster = ArakoonManagement().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        for content in os.listdir(Configuration.get('ovs.core.cfgdir')):
            pool_match = VPOOL_REGEX.match(content)
            if pool_match:
                existing_vpools.append(pool_match.groups()[0])
        
        for vpool_name in existing_vpools:
            vsr_configuration = VolumeStorageRouterConfiguration(vpool_name)
            vsr_configuration.configure_arakoon_cluster(voldrv_arakoon_cluster_id, voldrv_arakoon_client_config)
            if vpool_name == vpool:
                vrouter_config = {"vrouter_id": '{}{}'.format(vpool_name, unique_machine_id),
                                  "vrouter_redirect_timeout_ms": "5000",
                                  "vrouter_migrate_timeout_ms" : "5000",
                                  "vrouter_write_threshold" : 1024,
                                  "host": remote_ip,
                                  "xmlrpc_port": 12323}
                vsr_configuration.configure_volumerouter(vpool_name, vrouter_config)
                shutil.copyfile(os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json'.format(vpool)), os.path.join(self.node_cfg_dir, '{}.json'.format(vpool)))
                configs_to_push.append('{}.json'.format(vpool))

        """
        Update new and local node with new config
        For now we update all components on this and the new node, but after the node joined the cluster,
        we should check how many nodes are in the cluster and only reconfigure certain components based on that
        Secondly we do not only need to update the new node and ourselfs but also a 2nd node already in place when adding nr 3.
        """
        self._push_hrds()
        self._push_config(configs=configs_to_push, node_config_dir=self.node_cfg_dir, update_local=True)

        """
        Update the local and remote hosts file.
        """
        # Add content to fstab
        j.system.net.updateHostsFile(hostsfile='/etc/hosts', ip=remote_ip, hostname=machine_hostname)
        Remote.cuisine.api.run('python -c "from JumpScale import j; j.system.net.updateHostsFile(hostsfile=\'/etc/hosts\', ip=\'{}\', hostname=\'{}\')"'.format(Configuration.get('ovs.grid.ip'), socket.gethostname()))

        """
        Install the required software
        """
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
        self._remote_control_init(vpool_name, services=['openvstorage-core'], master=socket.gethostname())
        self.control._start_package('openvstorage-webapps')
        self._remote_control_init(vpool_name, services=['openvstorage-webapps'], master=socket.gethostname())
        #self.control.init(vpool_name, services=['volumedriver',])
        self.control._start_package('volumedriver')
        self._remote_control_init(vpool_name, services=['volumedriver',], master=socket.gethostname())

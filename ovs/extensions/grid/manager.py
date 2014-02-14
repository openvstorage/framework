#!/usr/bin/python2
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
Contains grid management functionality
"""

import ConfigParser
import base64
import getpass
import hashlib
import os
import sys
import urllib2
import re
import platform

from configobj import ConfigObj
from optparse import OptionParser
from random import choice
from string import lowercase
from subprocess import check_output
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.net import Net
from ovs.plugin.provider.osis import Osis


class Manager(object):
    """
    Contains grid management functionality
    """

    @staticmethod
    def install_node(ip, password, create_filesystems=True, clean=False):
        """
        Installs the Open vStorage software on a (remote) node.
        """

        # Load client, local or remote
        is_local = Client.is_local(ip)
        client = Client.load(ip, password, bypass_local=True)

        if clean:
            Manager._clean(client)
        if create_filesystems:
            Manager._create_filesystems(client)

        # Ask a bunch of questions and prepare HRD files for installation
        first_node = False
        if is_local:
            # @TODO: Try to figure out whether this is the first node or not.
            first_node = Helper.ask_yesno('Is this a first node installation?', default_value=True)
        configuration = {'openvstorage': {}}
        configuration['openvstorage']['ovs.host.hypervisor'] = 'VMWARE'
        configuration['openvstorage']['ovs.host.name'] = Helper.ask_string('Enter hypervisor hostname', default_value='esxi')
        ip, username, password = None, 'root', None
        while True:
            ip = Helper.ask_string('Enter hypervisor ip address', default_value=ip)
            username = Helper.ask_string('Enter hypervisor username', default_value=username)
            password = getpass.getpass()
            try:
                request = urllib2.Request('https://{0}/mob'.format(ip))
                auth = base64.encodestring('{0}:{1}'.format(username, password)).replace('\n', '')
                request.add_header("Authorization", "Basic %s" % auth)
                urllib2.urlopen(request).read()
                break
            except Exception as ex:
                print 'Could not connect to {0}: {1}'.format(ip, ex)
        configuration['openvstorage']['ovs.host.ip'] = ip
        configuration['openvstorage']['ovs.host.login'] = username
        configuration['openvstorage']['ovs.host.password'] = password

        configuration['openvstorage-core'] = {}
        ipaddresses = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
        ipaddresses = [ip for ip in ipaddresses if ip != '127.0.0.1']
        configuration['openvstorage-core']['ovs.grid.ip'] = Helper.ask_choice(ipaddresses,
                                                                              question='Choose public ip address',
                                                                              default_value=Helper.find_in_list(ipaddresses, ip))
        mountpoints = client.run('mount -v').strip().split('\n')
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2 and ('/mnt/' in p.split(' ')[2] or '/var' in p.split(' ')[2])]
        mountpoint = Helper.ask_choice(mountpoints,
                                       question='Select temporary FS mountpoint',
                                       default_value=Helper.find_in_list(mountpoints, 'tmp'))
        mountpoints.remove(mountpoint)
        configuration['openvstorage-core']['ovs.core.tempfs.mountpoint'] = mountpoint
        unique_id = sorted(client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0]
        configuration['openvstorage-core']['ovs.core.memcache.localnode.name'] = unique_id
        mountpoint = Helper.ask_choice(mountpoints,
                                       question='Select arakoon database mountpoint',
                                       default_value=Helper.find_in_list(mountpoints, 'db'))
        mountpoints.remove(mountpoint)
        configuration['openvstorage-core']['ovs.core.db.mountpoint'] = mountpoint
        configuration['openvstorage-core']['ovs.core.db.arakoon.node.name'] = unique_id
        mountpoint = Helper.ask_choice(mountpoints,
                                       question='Select distributed FS mountpoint',
                                       default_value=Helper.find_in_list(mountpoints, 'dfs'))
        mountpoints.remove(mountpoint)
        configuration['openvstorage-core']['volumedriver.filesystem.distributed'] = mountpoint
        mountpoint = Helper.ask_choice(mountpoints,
                                       question='Select metadata mountpoint',
                                       default_value=Helper.find_in_list(mountpoints, 'md'))
        mountpoints.remove(mountpoint)
        configuration['openvstorage-core']['volumedriver.metadata'] = mountpoint
        configuration['openvstorage-core']['volumedriver.arakoon.node.name'] = unique_id
        configuration['openvstorage-core']['ovs.core.broker.localnode.name'] = unique_id

        configuration['openvstorage-webapps'] = {}
        configuration['openvstorage-webapps']['ovs.webapps.certificate.period'] = Helper.ask_integer('GUI certificate lifetime', min_value=1, max_value=365 * 10, default_value=365)

        if is_local:
            grid_id = Helper.ask_integer('Enter grid ID (needs to be unique): ', min_value=1, max_value=32767)
            es_cluster_name = Helper.ask_string('Enter elastic search cluster name', default_value='ovses')
            es_cluster_name = '{0}_{1}'.format(es_cluster_name, grid_id)
            osis_key = ''.join(choice(lowercase) for _ in range(25))
        else:
            from ovs.plugin.provider.configuration import Configuration
            es_cluster_name = Configuration.get('elasticsearch.cluster.name')
            grid_id = Configuration.get('grid.id')
            osis_key = Configuration.get('osis.key')

        configuration['grid'] = {}
        configuration['grid']['grid.id'] = grid_id
        configuration['grid']['grid.useavahi'] = 1
        configuration['grid']['grid.node.roles'] = 'node'
        configuration['grid']['grid.node.machineid'] = unique_id

        configuration['elasticsearch'] = {}
        configuration['elasticsearch']['elasticsearch.cluster.name'] = es_cluster_name

        if first_node:
            configuration['grid_master'] = {}
            configuration['grid_master']['gridmaster.grid.id'] = grid_id
            configuration['grid_master']['gridmaster.useavahi'] = 1
            configuration['grid_master']['gridmaster.superadminpasswd'] = hashlib.sha256(''.join(choice(lowercase) for _ in range(25))).hexdigest()
        else:
            configuration['grid']['grid.master.ip'] = ''

        configuration['osis'] = {}
        configuration['osis']['osis.key'] = osis_key

        client.dir_ensure('/opt/jumpscale/cfg/hrd', True)
        if is_local:
            for filename in configuration:
                with open('/opt/jumpscale/cfg/hrd/{0}.hrd'.format(filename), 'w') as hrd:
                    hrd.write('\n'.join(['%s=%s' % i for i in configuration[filename].iteritems()]))
        else:
            for filename in configuration:
                with open('/opt/jumpscale/cfg/hrd/{0}_{1}.hr_'.format(filename, ip), 'w') as hrd:
                    hrd.write('\n'.join(['%s=%s' % i for i in configuration[filename].iteritems()]))
                # The file_upload method has the destination first, then the source. Yeah, makes sense... not.
                client.file_upload('/opt/jumpscale/cfg/hrd/{0}.hrd'.format(filename),
                                   '/opt/jumpscale/cfg/hrd/{0}_{1}.hr_'.format(filename, ip))

        # Make sure all software is up-to-date
        client.run('apt-get update')
        client.file_append('/etc/security/limits.conf', '\nroot soft core  unlimited\novs  soft core  unlimited\n')

        # Install base framework, JumpScale in this case
        install_branch = Manager._prepare_jscore(client, is_local)
        Manager._install_jscore(client, install_branch)

        # Install Open vStorage
        print 'Installing Open vStorage...'
        client.run('apt-get -y -q install python-dev')
        client.run('jpackage_install -n openvstorage')

    @staticmethod
    def init_node(ip, password, join_masters=False):
        """
        Initializes a node, making sure all required services are up and running.
        Optionally, the node can also join the masters, also participating in the arakoon and memcache
        clusters. Joining the masters will result in the services being restarted on all master nodes.
        Please note that initializing a node, joining a grid with < 3 nodes, will result in joining the
        master nodes regardless of the given parameter.
        """

        # Make sure to ALWAYS reload the client when switching targets, as Fabric seems to be singleton-ish
        is_local = Client.is_local(ip)

        arakoon_management = ArakoonManagement()
        nodes = Manager._get_cluster_nodes()  # All nodes, including the local and the new one
        arakoon_nodes = arakoon_management.getCluster('ovsdb').listNodes()
        client = Client.load(ip, password)
        new_node_hostname = client.run('hostname')
        if is_local and Configuration.get('grid.node.id') != '1':
            # If the script is executed local and there are multiple nodes, the script is executed on node 2+.
            # This is not allowed since a the existing configuration is requred to extend. On extra nodes there is no
            # current configuration to base all work on
            print 'This script can only be executed local on the first node. Subsequent nodes need to be installed from one of the master nodes.'
            sys.exit(1)
        if len(arakoon_nodes) < 3:
            print 'Insufficient master nodes'
            join_masters = True
        clusters = arakoon_management.listClusters()

        all_services = ['arakoon_ovsdb', 'arakoon_voldrv', 'memcached', 'rabbitmq', 'ovs_consumer_volumerouter',
                        'ovs_flower', 'ovs_scheduled_tasks', 'webapp_api', 'nginx', 'ovs_workers']
        arakoon_configfiles = ['/opt/OpenvStorage/config/arakoon/{0}/{0}_client.cfg'.format(cluster) for cluster in clusters]
        generic_configfiles = {'/opt/OpenvStorage/config/memcacheclient.cfg': 11211,
                               '/opt/OpenvStorage/config/rabbitmqclient.cfg': 5672}

        if join_masters:
            print 'Joining master nodes, services going down.'

            # Stop services (on all nodes)
            for node in nodes:
                node_client = Client.load(node, password)
                for service in all_services:
                    node_client.run('jsprocess disable -n {0}'.format(service))
                    node_client.run('jsprocess stop -n {0}'.format(service))

            # Fetch some information
            client = Client.load(ip, password)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
            unique_id = sorted(client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0].strip()
            remote_ips = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
            remote_ip = [ipa.strip() for ipa in nodes if ipa in remote_ips][0]

            # Configure servers, joining clusters, ...
            for cluster in clusters:
                # The Arakoon extension is not used since the config file needs to be parsed/loaded anyway to be
                # able to update it
                cfg = ConfigObj('/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'.format(cluster))
                global_section = cfg.get('global')
                cluster_nodes = global_section['cluster'] if type(global_section['cluster']) == list else [global_section['cluster'],]
                if unique_id not in cluster_nodes:
                    print "++++ IP: %s++++"%ip
                    client = Client.load(ip, password)
                    remote_config = client.file_read('/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'.format(cluster))
                    with open('/tmp/arakoon_{0}_cfg'.format(unique_id), 'w') as the_file:
                        the_file.write(remote_config)
                    remote_cfg = ConfigObj('/tmp/arakoon_{0}_cfg'.format(unique_id))
                    cluster_nodes.append(unique_id)
                    global_section['cluster'] = cluster_nodes
                    cfg.update({'global': global_section})
                    cfg.update({unique_id: remote_cfg.get(unique_id)})
                    cfg.write()
                    for node in nodes:
                        node_client = Client.load(node, password)
                        node_client.file_upload('/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'.format(cluster),
                                                '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'.format(cluster))
                arakoon_create_directories = """
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
arakoon_management = ArakoonManagement()
arakoon_cluster = arakoon_management.getCluster('%(cluster)s')
arakoon_cluster.createDirs(arakoon_cluster.listLocalNodes()[0])
"""%{'cluster': cluster}
                client.run('/opt/OpenvStorage/bin/python -c """{}"""'.format(arakoon_create_directories))

            # Update all nodes hosts file with new node and new node hosts file with all others
            for node in nodes:
                client_node = Client.load(node, password)
                update_hosts_file = """
from JumpScale import j
j.system.net.updateHostsFile(hostsfile='/etc/hosts', ip='%(ip)s', hostname='%(host)s')
"""%{'ip': ip,
     'host': new_node_hostname}
                client_node.run('python -c """{}"""'.format(update_hosts_file))
                client_node.run('jsprocess enable -n rabbitmq')
                client_node.run('jsprocess start -n rabbitmq')
                if node == ip:
                    for node in nodes:
                        client_node = Client.load(node,password)
                        node_hostname = client_node.run('hostname')
                        update_hosts_file = """
from JumpScale import j
j.system.net.updateHostsFile(hostsfile='/etc/hosts', ip='%(ip)s', hostname='%(host)s')
"""%{'ip': node,
     'host': node_hostname}
                        client = Client.load(ip,password)
                        client.run('python -c """{}"""'.format(update_hosts_file))
                


            client = Client.load(ip, password)
            client.run('rabbitmq-server -detached; rabbitmqctl stop_app; rabbitmqctl reset;')
            if not is_local:
                # Copy rabbitmq cookie
                rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'
                client.dir_ensure(os.path.dirname(rabbitmq_cookie_file), True)
                client.file_upload(rabbitmq_cookie_file, rabbitmq_cookie_file)
                client.file_attribs(rabbitmq_cookie_file, mode=400)
                # If not local, a cluster needs to be joined.
                master_client = Client.load(Configuration.get('grid.master.ip'), password)
                master_hostname = master_client.run('hostname')
                master_client.run('jsprocess enable -n rabbitmq')
                master_client.run('jsprocess start -n rabbitmq')
                client = Client.load(ip, password)
                client.run('rabbitmqctl join_cluster rabbit@{};'.format(master_hostname))
            client.run('rabbitmqctl stop;')

            # Update local client configurations
            for config in arakoon_configfiles:
                cfg = ConfigObj(config)
                global_section = cfg.get('global')
                cluster_nodes = global_section['cluster'] if type(global_section['cluster']) == list else [global_section['cluster'],]
                if unique_id not in cluster_nodes:
                    cluster_nodes.append(unique_id)
                    global_section['cluster'] = cluster_nodes
                    cfg.update({'global': global_section})
                    cfg.update({unique_id: {'ip': remote_ip,
                                            'client_port': '8870'}})
                    cfg.write()
            for config, port in generic_configfiles.iteritems():
                cfg = ConfigObj(config)
                main_section = cfg.get('main')
                generic_nodes = main_section['nodes'] if type(main_section['nodes']) == list else [main_section['nodes'],]
                if unique_id not in generic_nodes:
                    nodes.append(unique_id)
                    cfg.update({'main': {'nodes': generic_nodes}})
                    cfg.update({unique_id: {'location': '{0}:{1}'.format(remote_ip, port)}})
                    cfg.write()

            # Upload local client configurations to all nodes
            for node in nodes:
                node_client = Client.load(node, password)
                for config in arakoon_configfiles + generic_configfiles.keys():
                    node_client.file_upload(config, config)

            client = Client.load(ip, password)
            Manager._configure_nginx(client)

            # Restart services
            for node in nodes:
                node_client = Client.load(node, password)
                for service in all_services:
                    node_client.run('jsprocess enable -n {0}'.format(service))
                    node_client.run('jsprocess start -n {0}'.format(service))
            
            # If this is first node we need to load default model values.
            # @todo: Think about better detection algorithm.
            if len(nodes) == 1:
                from ovs.extensions.migration.migration import Migration
                Migration.migrate()
            
        else:
            client = Client.load(ip, password)
            # Disable master services
            client.run('jsprocess disable -n arakoon_ovsdb')
            client.run('jsprocess disable -n arakoon_voldrv')
            client.run('jsprocess disable -n memcached')
            client.run('jsprocess disable -n rabbitmq')
            client.run('jsprocess disable -n ovs_consumer_volumerouter')
            client.run('jsprocess disable -n ovs_flower')
            client.run('jsprocess disable -n ovs_scheduled_tasks')

            # Stop services
            for service in all_services:
                client.run('jsprocess stop -n {0}'.format(service))

            # The client config files can be copied from this node, since all client configurations are equal
            for config in arakoon_configfiles + generic_configfiles.keys():
                client.file_upload(config, config)
            Manager._configure_nginx(client)

            # Start other services
            client.run('jsprocess start -n webapp_api')
            client.run('jsprocess start -n nginx')
            client.run('jsprocess start -n ovs_workers')

    @staticmethod
    def init_vpool(ip, password, vpool):
        """
        Initializes a vpool on a given node
        """

        # @TODO: Not yet implemented
        # 1. Fetch vpool information
        # 2. Validate vpool info (does it exist, is it clustered, can we add it)
        # 3. Stop voldrv processes, if appropriate
        # 4. Configure volumedriver (on all appropriate nodes)
        # 5. Restart volumedriver
        raise NotImplementedError()

    @staticmethod
    def _get_cluster_nodes():
        grid_nodes = []
        local_ovs_grid_ip = Configuration.get('ovs.grid.ip')
        grid_id = Configuration.getInt('grid.id')
        osis_client_node = Osis.getClientForCategory(Osis.getClient(), 'system', 'node')
        for node_key in osis_client_node.list():
            node = osis_client_node.get(node_key)
            if node.gid != grid_id:
                continue
            ip_found = False
            for ip in node.ipaddr:
                if Net.getReachableIpAddress(ip, 22) == local_ovs_grid_ip:
                    grid_nodes.append(ip)
                    ip_found = True
                    break
            if not ip_found:
                raise RuntimeError('No suitable ip address found for node {0}'.format(node.machineguid))
        return grid_nodes

    @staticmethod
    def _configure_nginx(client):
        """
        Init nginx
        """
        # Update nginx configuration to not run in daemon mode
        nginx_content = client.file_read('/etc/nginx/nginx.conf')
        daemon_off = False
        for line in nginx_content.split('\n'):
            if re.match('^daemon off.*', line):
                daemon_off = True
                break
        if not daemon_off:
            nginx_content += '\ndaemon off;'
            client.file_write('/etc/nginx/nginx.conf', nginx_content)

        # Remove nginx default config
        client.run('rm -f /etc/nginx/sites-enabled/default')

    @staticmethod
    def _clean(client):
        """
        Cleans a previous install (dirty)
        """
        client.run('service nfs-kernel-server stop')
        client.run('pkill arakoon')
        client.run('rm -rf /usr/local/lib/python2.7/*-packages/JumpScale*')
        client.run('rm -rf /usr/local/lib/python2.7/dist-packages/jumpscale.pth')
        client.run('rm -rf /opt/jumpscale')
        client.run('rm -rf /opt/OpenvStorage')
        client.run('rm -rf /mnt/db/arakoon /mnt/db/tlogs /mnt/cache/foc /mnt/cache/sco /mnt/cache/read')

    @staticmethod
    def _create_filesystems(client):
        """
        Creates filesystems on the first two additional disks
        """
        mounted = client.run("mount | cut -d ' ' -f 1").strip().split('\n')
        # Create partitions on HDD
        if '/dev/sdb1' in mounted:
            client.run('umount /dev/sdb1')
        if '/dev/sdb2' in mounted:
            client.run('umount /dev/sdb2')
        if '/dev/sdb3' in mounted:
            client.run('umount /dev/sdb3')
        client.run('parted /dev/sdb -s mklabel gpt')
        client.run('parted /dev/sdb -s mkpart backendfs 2MB 80%')
        client.run('parted /dev/sdb -s mkpart distribfs 80% 90%')
        client.run('parted /dev/sdb -s mkpart tempfs 90% 100%')
        client.run('mkfs.ext4 -q /dev/sdb1 -L backendfs')
        client.run('mkfs.ext4 -q /dev/sdb2 -L distribfs')
        client.run('mkfs.ext4 -q /dev/sdb3 -L tempfs')

        #Create partitions on SSD
        if '/dev/sdc1' in mounted:
            client.run('umount /dev/sdc1')
        if '/dev/sdc2' in mounted:
            client.run('umount /dev/sdc2')
        if '/dev/sdc3' in mounted:
            client.run('umount /dev/sdc3')
        client.run('parted /dev/sdc -s mklabel gpt')
        client.run('parted /dev/sdc -s mkpart cache 2MB 50%')
        client.run('parted /dev/sdc -s mkpart db 50% 75%')
        client.run('parted /dev/sdc -s mkpart mdpath 75% 100%')
        client.run('mkfs.ext4 -q /dev/sdc1 -L cache')
        client.run('mkfs.ext4 -q /dev/sdc2 -L db')
        client.run('mkfs.ext4 -q /dev/sdc3 -L mdpath')

        client.run('mkdir -p /mnt/db')
        client.run('mkdir -p /mnt/cache')
        client.run('mkdir -p /mnt/md')
        client.run('mkdir -p /mnt/bfs')
        client.run('mkdir -p /mnt/dfs')

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
        fstab_content = client.file_read('/etc/fstab')
        if not '# BEGIN Open vStorage' in fstab_content:
            fstab_content += '\n'
            fstab_content += new_filesystems
            must_update = True
        if must_update:
            client.file_write('/etc/fstab', fstab_content)

        client.run('swapoff --all')
        client.run('mountall -q')

    @staticmethod
    def _prepare_jscore(client, is_local):
        """
        Prepares the system for JumpScale core installation;
        - Configure blobstore, bitbucket and sources
        """
        # Making sure all required folders are in place
        client.dir_ensure('/opt/jumpscale/cfg/jsconfig/', True)
        client.dir_ensure('/opt/jumpscale/cfg/jpackages/', True)

        # Quality mapping
        quality_mapping = {'unstable': ['test', 'default'],
                           'test': ['test', 'default'],
                           'stable': ['test', 'default']}

        if not is_local:
            if os.path.exists('/opt/jumpscale/cfg/jpackages/sources.cfg'):
                config = ConfigParser.ConfigParser()
                config.read('/opt/jumpscale/cfg/jpackages/sources.cfg')
                quality_level = config.get('openvstorage', 'qualitylevel')
            else:
                print 'If running on a remote node, the local node should be a working OVS installation'
                sys.exit(1)

            client.file_upload('/opt/jumpscale/cfg/jsconfig/bitbucket.cfg', '/opt/jumpscale/cfg/jsconfig/bitbucket.cfg')
            client.file_upload('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', '/opt/jumpscale/cfg/jsconfig/blobstor.cfg')
            client.file_upload('/opt/jumpscale/cfg/jpackages/sources.cfg', '/opt/jumpscale/cfg/jpackages/sources.cfg')
        else:
            supported_quality_levels = ['unstable', 'test', 'stable']
            quality_level = Helper.ask_choice(supported_quality_levels,
                                              question='Select qualitylevel',
                                              default_value='unstable')

            # Running local, asking user for required input
            bitbucket_username = Helper.ask_string('Provide your bitbucket username')
            bitbucket_password = getpass.getpass()
            if not os.path.exists('/opt/jumpscale/cfg/jsconfig'):
                os.makedirs('/opt/jumpscale/cfg/jsconfig')
            if not os.path.exists('/opt/jumpscale/cfg/jsconfig/bitbucket.cfg'):
                with open('/opt/jumpscale/cfg/jsconfig/bitbucket.cfg', 'w') as bitbucket:
                    bitbucket.write(
                        '[jumpscale]\nlogin = {0}\npasswd = {1}\n\n[openvstorage]\nlogin = {0}\npasswd = {1}\n'.format(
                            bitbucket_username, bitbucket_password
                        )
                    )

            jp_jumpscale_blobstor = """
[jpackages_local]
ftp =
type = local
http =
localpath = /opt/jpackagesftp
namespace = jpackages

[jpackages_remote]
ftp = ftp://publicrepo.incubaid.com
type = httpftp
http = http://publicrepo.incubaid.com
localpath =
namespace = jpackages
"""

            jp_openvstorage_blobstor = """
[jp_openvstorage]
ftp = ftp://packages.cloudfounders.com
http = http://packages.cloudfounders.com/ovs
namespace = jpackages
localpath =
type = httpftp
"""

            jp_jumpscale_repo = """
[jumpscale]
metadatafromtgz = 0
qualitylevel = %(qualityLevel)s
metadatadownload =
metadataupload =
bitbucketaccount = jumpscale
bitbucketreponame = jp_jumpscale
blobstorremote = jpackages_remote
blobstorlocal = jpackages_local
""" % {'qualityLevel': quality_mapping[quality_level][0]}

            jp_openvstorage_repo = """
[openvstorage]
metadatafromtgz = 0
qualitylevel = %(qualityLevel)s
metadatadownload = http://packages.cloudfounders.com/metadataTgz
metadataupload = file://opt/jumpscale/var/jpackages/metatars
bitbucketaccount = openvstorage
bitbucketreponame = jp_openvstorage
blobstorremote = jp_openvstorage
blobstorlocal = jpackages_local
""" % {'qualityLevel': quality_level}

            if not os.path.exists('/opt/jumpscale/cfg/jsconfig'):
                os.makedirs('/opt/jumpscale/cfg/jsconfig')
            if not os.path.exists('/opt/jumpscale/cfg/jsconfig/blobstor.cfg'):
                blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'w')
            else:
                blobstor_config = open('/opt/jumpscale/cfg/jsconfig/blobstor.cfg', 'a')
            blobstor_config.write(jp_jumpscale_blobstor)
            blobstor_config.write(jp_openvstorage_blobstor)
            blobstor_config.close()

            if not os.path.exists('/opt/jumpscale/cfg/jpackages'):
                os.makedirs('/opt/jumpscale/cfg/jpackages')
            if not os.path.exists('/opt/jumpscale/cfg/jpackages/sources.cfg'):
                jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'w')
            else:
                jp_sources_config = open('/opt/jumpscale/cfg/jpackages/sources.cfg', 'a')
            jp_sources_config.write(jp_jumpscale_repo)
            jp_sources_config.write(jp_openvstorage_repo)
            jp_sources_config.close()

        return quality_mapping[quality_level][1]

    @staticmethod
    def _install_jscore(client, install_branch):
        """
        Installs the JumpScale core package
        """
        print 'Installing JumpScale core...'
        client.package_install('python-pip')
        client.run('pip install -I https://bitbucket.org/jumpscale/jumpscale_core/get/{0}.zip'.format(install_branch))
        client.run('jpackage_update')
        client.run('jpackage_install -n core')
        print 'Done'


class Client(object):
    """
    Remote/local client
    """

    @staticmethod
    def load(ip, password, bypass_local=False):
        """
        Opens a client connection to a remote or local system
        """
        class LocalClient(object):
            """
            Provides local client functionality, having the same interface as the "Remote" client
            """

            @staticmethod
            def run(command):
                """
                Executes a command
                """
                return check_output(command, shell=True)

            @staticmethod
            def file_read(filename):
                """
                Reads a file
                """
                with open(filename, 'r') as the_file:
                    return the_file.read()

            @staticmethod
            def file_write(filename, contents):
                """
                Writes a file
                """
                with open(filename, 'w') as the_file:
                    the_file.write(contents)

            @staticmethod
            def file_append(filename, contents):
                """
                Appends content to a given file
                """
                with open(filename, 'a') as the_file:
                    the_file.write(contents)

            @staticmethod
            def package_install(package):
                """
                Installs a package
                """
                LocalClient.run('apt-get -y -q install {0}'.format(package))

            @staticmethod
            def dir_ensure(directory, recursive=False):
                """
                Ensures a dir exists
                """
                if not os.path.exists(directory):
                    if recursive:
                        os.makedirs(directory)
                    else:
                        os.mkdir(directory)

        if bypass_local and Client.is_local(ip):
            return LocalClient
        else:
            from ovs.plugin.provider.remote import Remote
            client = Remote.cuisine.api
            Remote.cuisine.fabric.env['password'] = password
            client.connect(ip)
            return client

    @staticmethod
    def is_local(ip):
        local_ip_addresses = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().split('\n')
        return ip in (rip.strip() for rip in local_ip_addresses)


class Helper(object):
    """
    This class contains various helper methods
    """

    @staticmethod
    def ask_integer(question, min_value, max_value, default_value=None, invalid_message=None):
        """
        Asks an integer to the user
        """
        if invalid_message is None:
            invalid_message = 'Invalid input please try again.'
        if default_value is not None:
            question = '{0} [{1}]: '.format(question, default_value)
        while True:
            i = raw_input(question).rstrip()
            if i == '' and default_value is not None:
                i = str(default_value)
            if not i.isdigit():
                print invalid_message
            else:
                i = int(i)
                if min_value <= i <= max_value:
                    return i
                else:
                    print invalid_message

    @staticmethod
    def ask_choice(choice_options, question=None, default_value=None):
        """
        Lets the user chose one of a set of options
        """
        if not choice_options:
            return None
        if len(choice_options) == 1:
            print "Found exactly one choice: {0}".format(choice_options[0])
            return choice_options[0]
        choice_options.sort()
        print '{0}Make a selection please: '.format('{0}. '.format(question) if question is not None else '')
        nr = 0
        default_nr = None
        for section in choice_options:
            nr += 1
            print '   {0}: {1}'.format(nr, section)
            if section == default_value:
                default_nr = nr

        result = Helper.ask_integer(question='   Select Nr: ',
                                    min_value=1,
                                    max_value=len(choice_options),
                                    default_value=default_nr)
        return choice_options[result - 1]

    @staticmethod
    def ask_string(message='', default_value=None):
        """
        Asks the user a question
        """
        default_string = ': ' if default_value is None else ' [{0}]: '.format(default_value)
        result = raw_input(str(message) + default_string).rstrip(chr(13))
        if not result and default_value is not None:
            return default_value
        return result

    @staticmethod
    def ask_yesno(message='', default_value=None):
        """
        Asks the user a yes/no question
        """
        if default_value is None:
            ynstring = ' (y/n):'
            failuremsg = "Illegal value. Press 'y' or 'n'."
        elif default_value is True:
            ynstring = ' ([y]/n)'
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        elif default_value is False:
            ynstring = ' (y/[n])'
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        else:
            raise ValueError('Invalid default value {0}'.format(default_value))
        while True:
            result = raw_input(str(message) + ynstring).rstrip(chr(13))
            if not result and default_value is not None:
                return default_value
            if result.lower() in ('y', 'yes'):
                return True
            if result.lower() in ('n', 'no'):
                return False
            print failuremsg

    @staticmethod
    def find_in_list(items, search_string):
        """
        Finds a given string in a list of items
        """
        for item in items:
            if search_string in item:
                return item
        return None


if __name__ == '__main__':
    if os.getegid() != 0:
        print 'This script should be executed as a user in the root group.'
        sys.exit(1)

    parser = OptionParser(description='Open vStorage Setup')
    parser.add_option('-n', '--no-filesystems', dest='filesystems', action="store_false", default=True,
                      help="Don't create partitions and filesystems")
    parser.add_option('-c', '--clean', dest='clean', action="store_true", default=False,
                      help="Try to clean environment before reinstalling")
    (options, args) = parser.parse_args()

    try:
        Manager.install_node('127.0.0.1', None, create_filesystems=options.filesystems, clean=options.clean)
    except KeyboardInterrupt:
        print '\nAborting'

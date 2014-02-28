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
import time

from optparse import OptionParser
from random import choice
from string import lowercase
from subprocess import check_output

ARAKOON_CONFIG_TAG = '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'
ARAKOON_CLIENTCONFIG_TAG = '/opt/OpenvStorage/config/arakoon/{0}/{0}_client.cfg'


class Manager(object):
    """
    Contains grid management functionality
    """

    @staticmethod
    def install_node(ip, password, create_extra_filesystems=False, clean=False):
        """
        Installs the Open vStorage software on a (remote) node.
        """

        # Load client, local or remote
        is_local = Client.is_local(ip)
        client = Client.load(ip, password, bypass_local=True)

        if clean:
            Manager._clean(client)
        Manager._create_filesystems(client, create_extra_filesystems)

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
        unique_id = sorted(client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0]
        configuration['openvstorage-core']['ovs.core.memcache.localnode.name'] = unique_id
        mountpoint = Helper.ask_choice(mountpoints,
                                       question='Select arakoon database mountpoint',
                                       default_value=Helper.find_in_list(mountpoints, 'db'))
        mountpoints.remove(mountpoint)
        configuration['openvstorage-core']['ovs.core.db.mountpoint'] = mountpoint
        configuration['openvstorage-core']['ovs.core.db.arakoon.node.name'] = unique_id
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

        from configobj import ConfigObj
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.lists.pmachinelist import PMachineList
        from ovs.dal.hybrids.vmachine import VMachine
        from ovs.dal.lists.vmachinelist import VMachineList
        from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
        from ovs.plugin.provider.configuration import Configuration

        if ip == '127.0.0.1':
            print 'Do not use 127.0.0.1 as ip address, use the public grid ip instead.'
            sys.exit(1)

        # Make sure to ALWAYS reload the client when switching targets, as Fabric seems to be singleton-ish
        is_local = Client.is_local(ip)
        client = Client.load(ip, password)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
        unique_id = sorted(client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0].strip()

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
        arakoon_clientconfigfiles = [ARAKOON_CLIENTCONFIG_TAG.format(cluster) for cluster in clusters]
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
            remote_ips = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
            remote_ip = [ipa.strip() for ipa in nodes if ipa in remote_ips][0]

            # Configure arakoon
            for cluster in clusters:
                # The Arakoon extension is not used since the config file needs to be parsed/loaded anyway to be
                # able to update it
                cfg = ConfigObj(ARAKOON_CONFIG_TAG.format(cluster))
                global_section = cfg.get('global')
                cluster_nodes = global_section['cluster'] if type(global_section['cluster']) == list else [global_section['cluster']]
                if unique_id not in cluster_nodes:
                    client = Client.load(ip, password)
                    remote_config = client.file_read(ARAKOON_CONFIG_TAG.format(cluster))
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
                        node_client.file_upload(ARAKOON_CONFIG_TAG.format(cluster),
                                                ARAKOON_CONFIG_TAG.format(cluster))
                client = Client.load(ip, password)
                arakoon_create_directories = """
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
arakoon_management = ArakoonManagement()
arakoon_cluster = arakoon_management.getCluster('%(cluster)s')
arakoon_cluster.createDirs(arakoon_cluster.listLocalNodes()[0])
""" % {'cluster': cluster}
                Manager._exec_python(client, arakoon_create_directories)

            # Update all nodes hosts file with new node and new node hosts file with all others
            for node in nodes:
                client_node = Client.load(node, password)
                update_hosts_file = """
from ovs.plugin.provider.net import Net
Net.updateHostsFile(hostsfile='/etc/hosts', ip='%(ip)s', hostname='%(host)s')
""" % {'ip': ip,
     'host': new_node_hostname}
                Manager._exec_python(client, update_hosts_file)
                if node != ip:
                    client_node.run('jsprocess enable -n rabbitmq')
                    client_node.run('jsprocess start -n rabbitmq')
                else:
                    for subnode in nodes:
                        client_node = Client.load(subnode, password)
                        node_hostname = client_node.run('hostname')
                        update_hosts_file = """
from ovs.plugin.provider.net import Net
Net.updateHostsFile(hostsfile='/etc/hosts', ip='%(ip)s', hostname='%(host)s')
""" % {'ip': subnode,
     'host': node_hostname}
                        client = Client.load(ip, password)
                        Manager._exec_python(client, update_hosts_file)

            # Update arakoon cluster configuration in voldrv configuration files
            for node in nodes:
                client_node = Client.load(node, password)
                update_voldrv = """
import os
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
arakoon_management = ArakoonManagement()
voldrv_arakoon_cluster_id = 'voldrv'
voldrv_arakoon_cluster = arakoon_management.getCluster(voldrv_arakoon_cluster_id)
voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
configuration_dir = Configuration.get('ovs.core.cfgdir')
if not os.path.exists('{0}/voldrv_vpools'.format(configuration_dir)):
    os.makedirs('{0}/voldrv_vpools'.format(configuration_dir))
for json_file in os.listdir('{0}/voldrv_vpools'.format(configuration_dir)):
    if json_file.endswith('.json'):
        vsr_config = VolumeStorageRouterConfiguration(json_file.replace('.json', ''))
        vsr_config.configure_arakoon_cluster(voldrv_arakoon_cluster_id, voldrv_arakoon_client_config)
"""
                Manager._exec_python(client_node, update_voldrv)

            # Join rabbitMQ clusters
            client = Client.load(ip, password)
            client.run('rabbitmq-server -detached; sleep 5; rabbitmqctl stop_app; sleep 5; rabbitmqctl reset; sleep 5; rabbitmqctl stop; sleep 5;')
            if not is_local:
                # Copy rabbitmq cookie
                rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'
                client.dir_ensure(os.path.dirname(rabbitmq_cookie_file), True)
                client.file_upload(rabbitmq_cookie_file, rabbitmq_cookie_file)
                client.file_attribs(rabbitmq_cookie_file, mode=400)
                client.run('rabbitmq-server -detached; sleep 5; rabbitmqctl stop_app; sleep 5;')
                # If not local, a cluster needs to be joined.
                master_client = Client.load(Configuration.get('grid.master.ip'), password)
                master_hostname = master_client.run('hostname')
                client = Client.load(ip, password)
                client.run('rabbitmqctl join_cluster rabbit@{}; sleep 5;'.format(master_hostname))
                client.run('rabbitmqctl stop; sleep 5;')

            # Update local client configurations
            db_client_port_mapper = dict()
            for cluster in clusters:
                db_cfg = ConfigObj(ARAKOON_CONFIG_TAG.format(cluster))
                db_client_port_mapper[cluster] = db_cfg.get(unique_id)['client_port']

            for cluster in clusters:
                cfg = ConfigObj(ARAKOON_CLIENTCONFIG_TAG.format(cluster))
                global_section = cfg.get('global')
                cluster_nodes = global_section['cluster'] if type(global_section['cluster']) == list else [global_section['cluster']]
                if unique_id not in cluster_nodes:
                    cluster_nodes.append(unique_id)
                    global_section['cluster'] = cluster_nodes
                    cfg.update({'global': global_section})
                    cfg.update({unique_id: {'ip': remote_ip,
                                            'client_port': db_client_port_mapper[cluster]}})
                    cfg.write()

            for config, port in generic_configfiles.iteritems():
                cfg = ConfigObj(config)
                main_section = cfg.get('main')
                generic_nodes = main_section['nodes'] if type(main_section['nodes']) == list else [main_section['nodes']]
                if unique_id not in generic_nodes:
                    generic_nodes.append(unique_id)
                    cfg.update({'main': {'nodes': generic_nodes}})
                    cfg.update({unique_id: {'location': '{0}:{1}'.format(remote_ip, port)}})
                    cfg.write()

            # Upload local client configurations to all nodes
            for node in nodes:
                node_client = Client.load(node, password)
                for config in arakoon_clientconfigfiles + generic_configfiles.keys():
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
            # @TODO: Think about better detection algorithm.
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
            for config in arakoon_clientconfigfiles + generic_configfiles.keys():
                client.file_upload(config, config)
            Manager._configure_nginx(client)

            # Start other services
            client.run('jsprocess start -n webapp_api')
            client.run('jsprocess start -n nginx')
            client.run('jsprocess start -n ovs_workers')

        for cluster in ['ovsdb', 'voldrv']:
            master_elected = False
            while not master_elected:
                client = arakoon_management.getCluster(cluster).getClient()
                try:
                    client.whoMaster()
                    master_elected = True
                except:
                    print "Arakoon master not yet determined for {0}".format(cluster)
                    time.sleep(1)

        # Make sure the process manager is started
        client = Client.load(ip, password)
        client.run('service processmanager start')

        # Add VSA and pMachine in the model, if they don't yet exist
        pmachine = None
        pmachine_ip = Manager._read_remote_config(client, 'ovs.host.ip')
        pmachine_hvtype = Manager._read_remote_config(client, 'ovs.host.hypervisor')
        for current_pmachine in PMachineList.get_pmachines():
            if current_pmachine.ip == pmachine_ip and current_pmachine.hvtype == pmachine_hvtype:
                pmachine = current_pmachine
                break
        if pmachine is None:
            pmachine = PMachine()
            pmachine.ip = pmachine_ip
            pmachine.username = Manager._read_remote_config(client, 'ovs.host.login')
            pmachine.password = Manager._read_remote_config(client, 'ovs.host.password')
            pmachine.hvtype = pmachine_hvtype
            pmachine.name = Manager._read_remote_config(client, 'ovs.host.name')
            pmachine.save()
        vsa = None
        for current_vsa in VMachineList.get_vsas():
            if current_vsa.ip == ip and current_vsa.machineid == unique_id:
                vsa = current_vsa
                break
        if vsa is None:
            vsa = VMachine()
            vsa.name = new_node_hostname
            vsa.is_vtemplate = False
            vsa.is_internal = True
            vsa.machineid = unique_id
            vsa.ip = ip
            vsa.save()
        vsa.pmachine = pmachine
        vsa.save()

        for node in nodes:
            node_client = Client.load(node, password)
            node_client.run('jsprocess restart -n ovs_workers')

    @staticmethod
    def init_vpool(ip, password, vpool_name):
        """
        Initializes a vpool on a given node
        """

        from ovs.dal.hybrids.vpool import VPool
        from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
        from ovs.dal.lists.vpoollist import VPoolList
        from ovs.dal.lists.vmachinelist import VMachineList
        from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
        from volumedriver.storagerouter.storagerouterclient import ClusterRegistry, ArakoonNodeConfig, ClusterNodeConfig
        from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement

        while not re.match('^[0-9a-zA-Z]+([\-_]+[0-9a-zA-Z]+)*$', vpool_name):
            print 'Invalid vPool name given. Only 0-9, a-z, A-Z, _ and - are allowed.'
            suggestion = re.sub(
                '^([\-_]*)(?P<correct>[0-9a-zA-Z]+([\-_]+[0-9a-zA-Z]+)*)([\-_]*)$',
                '\g<correct>',
                re.sub('[^0-9a-zA-Z\-_]', '_', vpool_name)
            )
            vpool_name = Helper.ask_string('Provide new vPool name', default_value=suggestion)

        client = Client.load(ip, password)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
        unique_id = sorted(client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0].strip()

        vsa = None
        for current_vsa in VMachineList.get_vsas():
            if current_vsa.ip == ip and current_vsa.machineid == unique_id:
                vsa = current_vsa
                break
        if vsa is None:
            raise RuntimeError('Could not find VSA with given ip address')

        vpool = VPoolList.get_vpool_by_name(vpool_name)
        vsr = None
        if vpool is not None:
            if vpool.backend_type == 'LOCAL':
                # Might be an issue, investigating whether it's on the same not or not
                if len(vpool.vsrs) == 1 and vpool.vsrs[0].serving_vmachine.machineid != unique_id:
                    raise RuntimeError('A local vPool with name {0} already exists'.format(vpool_name))
            for vpool_vsr in vpool.vsrs:
                if vpool_vsr.serving_vmachine_guid == vsa.guid:
                    vsr = vpool_vsr  # The vPool is already added to this VSA and this might be a cleanup/recovery

            # Check whether there are running machines on this vPool
            machine_guids = []
            for vdisk in vpool.vdisks:
                if vdisk.vmachine_guid not in machine_guids:
                    machine_guids.append(vdisk.vmachine_guid)
                    if vdisk.vmachine.hypervisor_status in ['RUNNING', 'PAUSED']:
                        raise RuntimeError('At least one vMachine using this vPool is still running or paused. Make sure there are no active vMachines')

        nodes = {ip}
        if vpool is not None:
            for vpool_vsr in vpool.vsrs:
                nodes.add(vpool_vsr.serving_vmachine.ip)
        nodes = list(nodes)

        services = ['volumedriver_{0}'.format(vpool_name),
                    'failovercache_{0}'.format(vpool_name)]

        # Stop services
        for node in nodes:
            node_client = Client.load(node, password)
            for service in services:
                node_client.run('jsprocess disable -n {0}'.format(service))
                node_client.run('jsprocess stop -n {0}'.format(service))

        # Keep in mind that if the VSR exists, the vPool does as well

        client = Client.load(ip, password)
        mountpoints = client.run('mount -v').strip().split('\n')
        mountpoints = [p.split(' ')[2] for p in mountpoints if
                       len(p.split(' ')) > 2 and ('/mnt/' in p.split(' ')[2] or '/var' in p.split(' ')[2])]

        if vpool is None:
            vpool = VPool()
            supported_backends = Manager._read_remote_config(client, 'volumedriver.supported.backends').split(',')
            if 'REST' in supported_backends:
                supported_backends.remove('REST')  # REST is not supported for now
            vpool.backend_type = Helper.ask_choice(supported_backends, 'Select type of storage backend', default_value='CEPH_S3')
            connection_host = connection_port = connection_username = connection_password = None
            if vpool.backend_type == 'LOCAL':
                vpool.backend_metadata = {'backend_type': 'LOCAL'}
            if vpool.backend_type == 'REST':
                connection_host = Helper.ask_string('Provide REST ip address')
                connection_port = Helper.ask_integer('Provide REST connection port', min_value=1, max_value=65535)
                rest_connection_timeout_secs = Helper.ask_integer('Provide desired REST connection timeout(secs)',
                                                                  min_value=0, max_value=99999)
                vpool.backend_metadata = {'rest_connection_host': connection_host,
                                          'rest_connection_port': connection_port,
                                          'buchla_connection_log_level': "0",
                                          'rest_connection_verbose_logging': rest_connection_timeout_secs,
                                          'rest_connection_metadata_format': "JSON",
                                          'backend_type': 'REST'}
            elif vpool.backend_type in ('CEPH_S3', 'AMAZON_S3', 'SWIFT_S3'):
                connection_host = Helper.ask_string('Specify fqdn or ip address for your S3 compatible host')
                connection_port = Helper.ask_integer('Specify port for your S3 compatible host', min_value=1,
                                                     max_value=65535)
                connection_username = Helper.ask_string('Specify S3 access key')
                connection_password = getpass.getpass()
                vpool.backend_metadata = {'s3_connection_host': connection_host,
                                          's3_connection_port': connection_port,
                                          's3_connection_username': connection_username,
                                          's3_connection_password': connection_password,
                                          's3_connection_verbose_logging': 1,
                                          'backend_type': 'S3'}

            vpool.name = vpool_name
            vpool.description = "{} {}".format(vpool.backend_type, vpool_name)
            vpool.backend_login = connection_username
            vpool.backend_password = connection_password
            if not connection_host:
                vpool.backend_connection = None
            else:
                vpool.backend_connection = '{}:{}'.format(connection_host, connection_port)
            vpool.save()

        # Connection information is VSR related information
        new_vsr = False
        if vsr is None:
            vsr = VolumeStorageRouter()
            new_vsr = True

        mountpoint_temp = Helper.ask_choice(mountpoints,
                                            question='Select temporary FS mountpoint',
                                            default_value=Helper.find_in_list(mountpoints, 'tmp'))
        mountpoints.remove(mountpoint_temp)
        mountpoint_dfs_default = Helper.find_in_list(mountpoints, 'local')
        if vpool.backend_type in ('CEPH_S3', 'AMAZON_S3', 'SWIFT_S3'):
            mountpoint_dfs = Helper.ask_string(message='Enter a mountpoint for the S3 backend',
                                               default_value='/mnt/dfs/{}'.format(vpool.name))
        else:
            mountpoint_dfs = Helper.ask_choice(mountpoints,
                                               question='Select distributed FS mountpoint',
                                               default_value=Helper.find_in_list(mountpoints, 'dfs'))
            mountpoints.remove(mountpoint_dfs)
        mountpoint_md = Helper.ask_choice(mountpoints,
                                          question='Select metadata mountpoint',
                                          default_value=Helper.find_in_list(mountpoints, 'md'))
        mountpoints.remove(mountpoint_md)
        mountpoint_cache = Helper.ask_choice(mountpoints,
                                             question='Select cache mountpoint',
                                             default_value=Helper.find_in_list(mountpoints, 'cache'))
        mountpoints.remove(mountpoint_cache)
        cache_fs = os.statvfs(mountpoint_cache)
        scocache = '{}/sco_{}'.format(mountpoint_cache, vpool_name)
        readcache = '{}/read_{}'.format(mountpoint_cache, vpool_name)
        failovercache = '{}/foc_{}'.format(mountpoint_cache, vpool_name)
        metadatapath = '{}/metadata_{}'.format(mountpoint_md, vpool_name)
        tlogpath = '{}/tlogs_{}'.format(mountpoint_md, vpool_name)
        dirs2create = [scocache, failovercache, metadatapath, tlogpath,
                       Manager._read_remote_config(client, 'volumedriver.readcache.serialization.path')]
        files2create = [readcache]
        # Cache sizes
        # 20% = scocache
        # 20% = failovercache (@TODO: check if this can possibly consume more than 20%)
        # 60% = readcache
        scocache_size = '{0}KiB'.format((int(cache_fs.f_bavail * 0.2 / 4096) * 4096) * 4)
        readcache_size = '{0}KiB'.format((int(cache_fs.f_bavail * 0.6 / 4096) * 4096) * 4)
        if new_vsr:
            ports_used_in_model = [vsr.port for vsr in VolumeStorageRouterList.get_volumestoragerouters_by_vsa(vsa.guid)]
            vrouter_port_in_hrd = int(Manager._read_remote_config(client, 'volumedriver.filesystem.xmlrpc.port'))
            if vrouter_port_in_hrd in ports_used_in_model:
                vrouter_port = Helper.ask_integer('Provide Volumedriver connection port (make sure port is not in use)',
                                                  min_value=1024, max_value=max(ports_used_in_model) + 3)
            else:
                vrouter_port = vrouter_port_in_hrd
        else:
            vrouter_port = vsr.port
        ipaddresses = client.run(
            "ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
        ipaddresses = [ipaddr.strip() for ipaddr in ipaddresses if ipaddr.strip() != '127.0.0.1']
        grid_ip = Manager._read_remote_config(client, 'ovs.grid.ip')
        if grid_ip in ipaddresses:
            ipaddresses.remove(grid_ip)
        if not ipaddresses:
            raise RuntimeError('No available ip addresses found suitable for volumerouter storage ip')
        volumedriver_storageip = Helper.ask_choice(ipaddresses, 'Select storage ip address for this vpool')
        vrouter_id = '{0}{1}'.format(vpool_name, unique_id)

        vrouter_config = {'vrouter_id': vrouter_id,
                          'vrouter_redirect_timeout_ms': '5000',
                          'vrouter_migrate_timeout_ms': '5000',
                          'vrouter_write_threshold': 1024}
        voldrv_arakoon_cluster_id = str(Manager._read_remote_config(client, 'volumedriver.arakoon.clusterid'))
        voldrv_arakoon_cluster = ArakoonManagement().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        arakoon_node_configs = []
        for arakoon_node in voldrv_arakoon_client_config.keys():
            arakoon_node_configs.append(ArakoonNodeConfig(arakoon_node,
                                                          voldrv_arakoon_client_config[arakoon_node][0][0],
                                                          voldrv_arakoon_client_config[arakoon_node][1]))
        vrouter_clusterregistry = ClusterRegistry(vpool_name, voldrv_arakoon_cluster_id, arakoon_node_configs)
        node_configs = []
        for existing_vsr in VolumeStorageRouterList.get_volumestoragerouters():
            if existing_vsr.vpool_guid == vpool.guid:
                node_configs.append(ClusterNodeConfig(str(existing_vsr.vsrid), str(existing_vsr.cluster_ip),
                                                      existing_vsr.port - 1,
                                                      existing_vsr.port,
                                                      existing_vsr.port + 1))
        if new_vsr:
            node_configs.append(ClusterNodeConfig(vrouter_id, grid_ip,
                                                  vrouter_port - 1, vrouter_port, vrouter_port + 1))
        vrouter_clusterregistry.set_node_configs(node_configs)

        readcaches = [{'path': readcache, 'size': readcache_size}]
        scocaches = [{'path': scocache, 'size': scocache_size}]
        filesystem_config = {'fs_backend_path': mountpoint_dfs}
        volumemanager_config = {'metadata_path': metadatapath, 'tlog_path': tlogpath}
        amqp_uri = '{}://{}:{}@{}:{}'.format(Configuration.get('ovs.core.broker.protocol'),
                                             Configuration.get('ovs.core.broker.login'),
                                             Configuration.get('ovs.core.broker.password'),
                                             Configuration.get('ovs.grid.ip'),
                                             Configuration.get('ovs.core.broker.port'))
        vsr_config_script = """
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration
vsr_configuration = VolumeStorageRouterConfiguration('{0}')
vsr_configuration.configure_backend({1})
vsr_configuration.configure_readcache({2}, Configuration.get('volumedriver.readcache.serialization.path'))
vsr_configuration.configure_scocache({3}, '1GB', '2GB')
vsr_configuration.configure_failovercache('{4}')
vsr_configuration.configure_filesystem({5})
vsr_configuration.configure_volumemanager({6})
vsr_configuration.configure_volumerouter('{0}', {7})
vsr_configuration.configure_arakoon_cluster('{8}', {9})
queue_config = {{'events_amqp_routing_key': Configuration.get('ovs.core.broker.volumerouter.queue'),
                 'events_amqp_uri': '{10}'}}
vsr_configuration.configure_event_publisher(queue_config)
""".format(vpool_name, vpool.backend_metadata, readcaches, scocaches, failovercache, filesystem_config,
           volumemanager_config, vrouter_config, voldrv_arakoon_cluster_id, voldrv_arakoon_client_config,
           amqp_uri)
        Manager._exec_python(client, vsr_config_script)

        # Updating the model
        vsr.vsrid = vrouter_id
        vsr.name = vrouter_id.replace('_', ' ')
        vsr.description = vsr.name
        vsr.storage_ip = volumedriver_storageip
        vsr.cluster_ip = grid_ip
        vsr.port = vrouter_port
        vsr.mountpoint = '/mnt/{0}'.format(vpool_name)
        vsr.mountpoint_temp = mountpoint_temp
        vsr.mountpoint_dfs = mountpoint_dfs
        vsr.serving_vmachine = vsa
        vsr.vpool = vpool
        vsr.save()

        dirs2create.append(vsr.mountpoint)
        file_create_script = """
import os
for directory in {0}:
    if not os.path.exists(directory):
        os.makedirs(directory)
for filename in {1}:
    if not os.path.exists(filename):
        open(filename, 'a').close()""".format(dirs2create, files2create)
        Manager._exec_python(client, file_create_script)

        config_file = '{0}/voldrv_vpools/{1}.json'.format(Manager._read_remote_config(client, 'ovs.core.cfgdir'), vpool_name)
        log_file = '/var/log/{0}.log'.format(vpool_name)
        vd_cmd = '/usr/bin/volumedriver_fs -f --config-file={0} --mountpoint {1} --logfile {2} -o big_writes -o uid=0 -o gid=0 -o sync_read'.format(config_file, vsr.mountpoint, log_file)
        vd_stopcmd = 'exportfs -u *:{0}; umount {0}'.format(vsr.mountpoint)
        vd_name = 'volumedriver_{}'.format(vpool_name)

        log_file = os.path.join(os.sep, 'var', 'log', 'foc_{0}.log'.format(vpool_name))
        fc_cmd = '/usr/bin/failovercachehelper --config-file={0} --logfile={1}'.format(config_file, log_file)
        fc_name = 'failovercache_{0}'.format(vpool_name)

        service_script = """
from ovs.plugin.provider.service import Service
Service.add_service(package=('openvstorage', 'volumedriver'), name='{0}', command='{1}', stop_command='{2}')
Service.add_service(package=('openvstorage', 'volumedriver'), name='{3}', command='{4}', stop_command=None)""".format(
            vd_name, vd_cmd, vd_stopcmd,
            fc_name, fc_cmd
        )
        Manager._exec_python(client, service_script)

        fstab_script_remove = """
from ovs.extensions.fs.fstab import Fstab
fstab = Fstab()
fstab.remove_config_by_directory('{0}')
"""

        fstab_script_add = """
from ovs.extensions.fs.fstab import Fstab
fstab = Fstab()
fstab.remove_config_by_directory('{0}')
fstab.add_config('{1}', '{0}', '{2}', '{3}', '{4}', '{5}')
"""

        if mountpoint_dfs_default and mountpoint_dfs_default != vsr.mountpoint_dfs:
            client.run('umount {0}'.format(mountpoint_dfs_default))
            client.run('mkdir -p {0}'.format(vsr.mountpoint_dfs))
            fstab_script = fstab_script_remove.format(mountpoint_dfs_default)
            Manager._exec_python(client, fstab_script)
        if vpool.backend_type == 'CEPH_S3':
            # If using CEPH_S3, then help setting up the ceph connection - for now
            if Helper.find_in_list(mountpoints, vsr.mountpoint_dfs):
                client.run('umount {0}'.format(vsr.mountpoint_dfs))
            ceph_ok = Manager._check_ceph(client)
            if not ceph_ok:
                # First, try to copy them over
                for vpool_vsr in vpool.vsrs:
                    if vpool_vsr.guid != vsr.guid:
                        client.dir_ensure('/etc/ceph', True)
                        for cfg_file in ['/etc/ceph/ceph.conf', '/etc/ceph/ceph.keyring']:
                            remote_client = Client.load(vpool_vsr.serving_vmachine.ip, password)
                            cfg_content = remote_client.file_read(cfg_file)
                            client = Client.load(ip, password)
                            client.file_write(cfg_file, cfg_content)
                        client.file_attribs('/etc/ceph/ceph.keyring', mode=644)
                        break
                ceph_ok = Manager._check_ceph(client)
            if not ceph_ok:
                # If not yet ok, let the user copy the files
                print Helper.boxed_message(
                    ['No or incomplete configuration files found for your Ceph S3 compatible storage backend',
                     'Now is the time to copy following files',
                     ' CEPH_SERVER:/etc/ceph/ceph.conf -> /etc/ceph/ceph.conf',
                     ' CEPH_SERVER:/etc/ceph/ceph.client.admin.keyring -> /etc/ceph/ceph.keyring',
                     'to make sure we can connect our ceph filesystem',
                     'When done continue the initialization here'])
                ceph_continue = Helper.ask_yesno('Continue initialization', default_value=False)
                if not ceph_continue:
                    raise RuntimeError('Exiting initialization')
                ceph_ok = Manager._check_ceph(client)
                if not ceph_ok:
                    raise RuntimeError('Ceph config still not ok, exiting initialization')
            fstab_script = fstab_script_add.format(vsr.mountpoint_dfs, 'id=admin,conf=/etc/ceph/ceph.conf',
                                                   'fuse.ceph', 'defaults,noatime', '0', '2')
            Manager._exec_python(client, fstab_script)
            client.run('mkdir -p {0}'.format(vsr.mountpoint_dfs))
            client.run('mount {0}'.format(vsr.mountpoint_dfs), pty=False)

        Manager.init_exportfs(client, vpool.name)

        # Start services
        for node in nodes:
            node_client = Client.load(node, password)
            for service in services:
                node_client.run('jsprocess enable -n {0}'.format(service))
                node_client.run('jsprocess start -n {0}'.format(service))

    @staticmethod
    def _check_ceph(client):
        ceph_config_dir = '/etc/ceph'
        if not client.dir_exists(ceph_config_dir):
            return False
        if not client.file_exists(os.path.join(ceph_config_dir, 'ceph.conf')) or \
                not client.file_exists(os.path.join(ceph_config_dir, 'ceph.keyring')):
            return False
        client.file_attribs(os.path.join(ceph_config_dir, 'ceph.keyring'), mode=644)
        return True

    @staticmethod
    def init_exportfs(client, vpool_name):
        """
        Configure nfs
        """
        import uuid

        vpool_mountpoint = '/mnt/{0}'.format(vpool_name)
        client.dir_ensure(vpool_mountpoint, True)
        nfs_script = """
from ovs.extensions.fs.exportfs import Nfsexports
Nfsexports().add('{0}', '*', 'rw,fsid={1},async,no_root_squash,no_subtree_check')""".format(
            vpool_mountpoint, uuid.uuid4()
        )
        Manager._exec_python(client, nfs_script)
        client.run('service nfs-kernel-server start')

    @staticmethod
    def _read_remote_config(client, key):
        """
        Reads remote configuration key
        """
        read = """
from ovs.plugin.provider.configuration import Configuration
print Configuration.get('{0}')
""".format(key)
        return Manager._exec_python(client, read)

    @staticmethod
    def _exec_python(client, script):
        """
        Executes a python script on the client inside the OVS virtualenv
        """
        return client.run('source /opt/OpenvStorage/bin/activate; python -c """{0}"""'.format(script))

    @staticmethod
    def _get_cluster_nodes():
        """
        Get nodes from Osis
        """
        from ovs.plugin.provider.net import Net
        from ovs.plugin.provider.osis import Osis
        from ovs.plugin.provider.configuration import Configuration

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
    def _create_filesystems(client, create_extra):
        """
        Creates filesystems on the first two additional disks
        """
        mounted = client.run("mount | cut -d ' ' -f 1").strip().split('\n')

        # Create partitions on SSD
        if '/dev/sdb1' in mounted:
            client.run('umount /dev/sdb1')
        if '/dev/sdb2' in mounted:
            client.run('umount /dev/sdb2')
        if '/dev/sdb3' in mounted:
            client.run('umount /dev/sdb3')
        client.run('parted /dev/sdb -s mklabel gpt')
        client.run('parted /dev/sdb -s mkpart cache 2MB 50%')
        client.run('parted /dev/sdb -s mkpart db 50% 75%')
        client.run('parted /dev/sdb -s mkpart mdpath 75% 100%')
        client.run('mkfs.ext4 -q /dev/sdb1 -L cache')
        client.run('mkfs.ext4 -q /dev/sdb2 -L db')
        client.run('mkfs.ext4 -q /dev/sdb3 -L mdpath')

        client.run('mkdir -p /mnt/db')
        client.run('mkdir -p /mnt/cache')
        client.run('mkdir -p /mnt/md')

        extra_mountpoints = ''
        if create_extra:
            # Create partitions on HDD
            if '/dev/sdc1' in mounted:
                client.run('umount /dev/sdc1')
            if '/dev/sdc2' in mounted:
                client.run('umount /dev/sdc2')
            if '/dev/sdc3' in mounted:
                client.run('umount /dev/sdc3')
            client.run('parted /dev/sdc -s mklabel gpt')
            client.run('parted /dev/sdc -s mkpart backendfs 2MB 80%')
            client.run('parted /dev/sdc -s mkpart distribfs 80% 90%')
            client.run('parted /dev/sdc -s mkpart tempfs 90% 100%')
            client.run('mkfs.ext4 -q /dev/sdc1 -L backendfs')
            client.run('mkfs.ext4 -q /dev/sdc2 -L distribfs')
            client.run('mkfs.ext4 -q /dev/sdc3 -L tempfs')

            extra_mountpoints = """
LABEL=backendfs /mnt/bfs   ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=distribfs /mnt/dfs/local   ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=tempfs    /var/tmp   ext4    defaults,nobootwait,noatime,discard    0    2
"""

            client.run('mkdir -p /mnt/bfs')
            client.run('mkdir -p /mnt/dfs/local')

        # Add content to fstab
        new_filesystems = """
# BEGIN Open vStorage
LABEL=db        /mnt/db    ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=cache     /mnt/cache ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=mdpath    /mnt/md    ext4    defaults,nobootwait,noatime,discard    0    2
{0}
# END Open vStorage
""".format(extra_mountpoints)
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
        # Tese mappings were ['unstable', 'default'] and ['default', 'default'] before
        quality_mapping = {'unstable': ['stable', 'stable'],
                           'test': ['stable', 'stable'],
                           'stable': ['stable', 'stable']}

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
            print 'Found exactly one choice: {0}'.format(choice_options[0])
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

    @staticmethod
    def boxed_message(lines, character='+', maxlength=80):
        """
        Embeds a set of lines into a box
        """
        character = str(character)  # This must be a string
        corrected_lines = []
        for line in lines:
            if len(line) > maxlength:
                linepart = ''
                for word in line.split(' '):
                    if len(linepart + ' ' + word) <= maxlength:
                        linepart += word + ' '
                    elif len(word) >= maxlength:
                        if len(linepart) > 0:
                            corrected_lines.append(linepart.strip())
                            linepart = ''
                        corrected_lines.append(word.strip())
                    else:
                        if len(linepart) > 0:
                            corrected_lines.append(linepart.strip())
                        linepart = word + ' '
                if len(linepart) > 0:
                    corrected_lines.append(linepart.strip())
            else:
                corrected_lines.append(line)
        maxlen = len(max(corrected_lines, key=len))
        newlines = [character * (maxlen + 10)]
        for line in corrected_lines:
            newlines.append('{0}  {1}{2}  {3}'.format(character * 3, line, ' ' * (maxlen - len(line)),
                                                      character * 3))
        newlines.append(character * (maxlen + 10))
        return '\n'.join(newlines)


if __name__ == '__main__':
    if os.getegid() != 0:
        print 'This script should be executed as a user in the root group.'
        sys.exit(1)

    parser = OptionParser(description='Open vStorage Setup')
    parser.add_option('-e', '--extra-filesystems', dest='filesystems', action='store_true', default=False,
                      help="Create extra filesystems on third disk for backend-, distributed- and temporary FS")
    parser.add_option('-c', '--clean', dest='clean', action='store_true', default=False,
                      help='Try to clean environment before reinstalling')
    (options, args) = parser.parse_args()

    try:
        Manager.install_node('127.0.0.1', None, create_extra_filesystems=options.filesystems, clean=options.clean)
    except KeyboardInterrupt:
        print '\nAborting'

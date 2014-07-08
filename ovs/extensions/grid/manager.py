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
import uuid

from optparse import OptionParser
from random import choice
from string import lowercase, digits
from subprocess import check_output

ARAKOON_CONFIG_TAG = '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'
ARAKOON_CLIENTCONFIG_TAG = '/opt/OpenvStorage/config/arakoon/{0}/{0}_client.cfg'

ELASTICSEARCH_DEB = 'elasticsearch-1.1.1.deb'
ELASTICSEARCH_URL = 'https://download.elasticsearch.org/elasticsearch/elasticsearch/{0}'.format(ELASTICSEARCH_DEB)

KIBANA_VERSION = 'kibana-3.0.1'
KIBANA_URL = 'https://download.elasticsearch.org/kibana/kibana/{0}.tar.gz'.format(KIBANA_VERSION)

LOGSTASH_DEB = 'logstash_1.4.0-1-c82dc09_all.deb'
LOGSTASH_URL = 'https://download.elasticsearch.org/logstash/logstash/packages/debian/{0}'.format(LOGSTASH_DEB)


class Manager(object):
    """
    Contains grid management functionality
    """

    @staticmethod
    def replace_param_in_config(client, config_file, old_value, new_value, add=False):
        if client.file_exists(config_file):
            contents = client.file_read(config_file)
            if new_value in contents and new_value.find(old_value) > 0:
                pass
            elif old_value in contents:
                contents = contents.replace(old_value, new_value)
            else:
                if add:
                    contents += new_value + '\n'
            client.file_write(config_file, contents)

    @staticmethod
    def install_node(ip, create_extra_filesystems=False, clean=False, version=None):
        """
        Installs the Open vStorage software on a (remote) node.
        """

        if not os.geteuid() == 0:
            print 'Please run this script as root'
            sys.exit(1)
        if Manager._validate_ip(ip) is False:
            print 'The entered ip address is invalid'
            sys.exit(1)
        if isinstance(create_extra_filesystems, bool) is False or isinstance(clean, bool) is False:
            print 'Some arguments contain invalid data'
            sys.exit(1)
        if version is not None and not isinstance(version, basestring):
            print 'Illegal version specified'

        # Load client, local or remote
        is_local = Client.is_local(ip)
        password = None
        if is_local is False:
            print 'Enter the root password for: {0}'.format(ip)
            password = getpass.getpass()

        client = Client.load(ip, password, bypass_local=True)

        Manager.replace_param_in_config(client,
                                        '/etc/ssh/sshd_config',
                                        'AcceptEnv',
                                        '#AcceptEnv')
        Manager.replace_param_in_config(client,
                                        '/etc/ssh/sshd_config',
                                        'UseDNS yes',
                                        'UseDNS no',
                                        add=True)
        client.run('service ssh restart')

        client = Client.load(ip, password, bypass_local=True)

        client.run('apt-get update')
        client.run('apt-get install lsscsi')

        if clean:
            Manager._clean(client)
        Manager._create_filesystems(client, create_extra_filesystems)

        possible_hypervisor = None
        try:
            module = client.run('lsmod | grep kvm').strip()
        except:
            module = ''
        if module != '':
            possible_hypervisor = 'KVM'
        else:
            try:
                disktypes = client.run('dmesg | grep VMware').strip()
            except:
                disktypes = ''
            if disktypes != '':
                possible_hypervisor = 'VMWARE'
        hypervisor = Helper.ask_choice(['VMWARE', 'KVM'], question='Which hypervisor will be backing this VSA?', default_value=possible_hypervisor)

        ipaddresses = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
        ipaddresses = [found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1']

        # Ask a bunch of questions and prepare HRD files for installation
        first_node = False
        if is_local:
            # @TODO: Try to figure out whether this is the first node or not.
            first_node = Helper.ask_yesno('Is this a first node installation?', default_value=True)
        configuration = {'openvstorage': {}}
        configuration['openvstorage']['ovs.host.hypervisor'] = hypervisor
        default_name = 'esxi' if hypervisor == 'VMWARE' else 'kvm'
        configuration['openvstorage']['ovs.host.name'] = Helper.ask_string('Enter hypervisor hostname', default_value=default_name)
        hypervisor_ip, username, hypervisor_password = None, 'root', None
        if hypervisor == 'VMWARE':
            while True:
                hypervisor_ip = Helper.ask_string('Enter hypervisor ip address', default_value=ip)
                username = Helper.ask_string('Enter hypervisor username', default_value=username)
                hypervisor_password = getpass.getpass()
                try:
                    request = urllib2.Request('https://{0}/mob'.format(hypervisor_ip))
                    auth = base64.encodestring('{0}:{1}'.format(username, hypervisor_password)).replace('\n', '')
                    request.add_header("Authorization", "Basic %s" % auth)
                    urllib2.urlopen(request).read()
                    break
                except Exception as ex:
                    print 'Could not connect to {0}: {1}'.format(hypervisor_ip, ex)
        elif hypervisor == 'KVM':
            # In case of KVM, the VSA is the pMachine, so credentials are shared.
            hypervisor_ip = Helper.ask_choice(ipaddresses,
                                              question='Choose hypervisor public ip address',
                                              default_value=Helper.find_in_list(ipaddresses, ip))
            username = client.run('whoami').strip()
            hypervisor_password = password
        configuration['openvstorage']['ovs.host.ip'] = hypervisor_ip
        configuration['openvstorage']['ovs.host.login'] = username
        configuration['openvstorage']['ovs.host.password'] = hypervisor_password

        configuration['openvstorage-core'] = {}
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
        install_branch, ovs_version = Manager._prepare_jscore(client, is_local)
        Manager._install_jscore(client, install_branch)

        if version is not None:
            ovs_version = version

        client.run('apt-get -y -q install libvirt0 python-libvirt virtinst')
        client.run("if crontab -l | grep -q 'ntpdate'; then true; else crontab -l | { cat; echo '0 * * * * /usr/sbin/ntpdate pool.ntp.org'; } | crontab -; fi")

        # Install Open vStorage
        print 'Installing Open vStorage...'
        client.run('apt-get -y -q install python-dev')
        client.run('jpackage_install -n openvstorage -v {0}'.format(ovs_version))
        client.run('. /opt/OpenvStorage/bin/activate; pip install amqp==1.4.1')
        client.run('. /opt/OpenvStorage/bin/activate; pip install suds-jurko==0.5')
        client.run('. /opt/OpenvStorage/bin/activate; pip install pysnmp==4.2.5')

        client.run('apt-get -y -q install libev4')
        client.run('jpackage_install -n arakoon -v 1.7.2')

        # update elasticsearch
        client.run('apt-get -y -q install openjdk-7-jre')
        client.run('jsprocess -n elasticsearch disable')
        client.run('jsprocess -n elasticsearch stop')
        client.run('mv /etc/elasticsearch/elasticsearch.yml /var/tmp/')
        client.run('cd /root/; rm -f {0}; wget -c {1}'.format(ELASTICSEARCH_DEB, ELASTICSEARCH_URL))
        client.run('dpkg -i /root/{0}'.format(ELASTICSEARCH_DEB))
        client.run('mv /var/tmp/elasticsearch.yml /etc/elasticsearch/')
        Manager.replace_param_in_config(client,
                                        '/opt/jumpscale/cfg/startup/jumpscale__elasticsearch.hrd',
                                        'process.cmd=/opt/jumpscale/apps/elasticsearch/bin/elasticsearch',
                                        'process.cmd=/usr/share/elasticsearch/bin/elasticsearch')
        client.run('jsprocess -n elasticsearch disable')
        client.run('jsprocess -n elasticsearch stop')
        client.run('mkdir -p /opt/data/elasticsearch/work')
        client.run('chown -R elasticsearch:elasticsearch /opt/data/elasticsearch*')

        config_file = '/etc/elasticsearch/elasticsearch.yml'
        Manager.replace_param_in_config(client,
                                        config_file,
                                        '<CLUSTER_NAME>',
                                        es_cluster_name,
                                        add=False)
        Manager.replace_param_in_config(client,
                                        config_file,
                                        '<NODE_NAME>',
                                        client.run('hostname'))
        public_ip = configuration['openvstorage-core']['ovs.grid.ip']
        Manager.replace_param_in_config(client,
                                        config_file,
                                        '<NETWORK_PUBLISH>',
                                        public_ip)
        client.run('service elasticsearch restart')

        client.run('cd /root; rm -f {0}; wget -c {1}'.format(LOGSTASH_DEB, LOGSTASH_URL))
        client.run('dpkg -i /root/{0}'.format(LOGSTASH_DEB))
        client.run('usermod -a -G adm logstash')
        client.run("echo 'manual' >/etc/init/logstash-web.override")
        Manager.replace_param_in_config(client,
                                        '/etc/logstash/conf.d/indexer.conf',
                                        '<CLUSTER_NAME>',
                                        es_cluster_name)

        client.run('cd /root; wget -c {0}'.format(KIBANA_URL))
        client.run('cd /root; gunzip /root/{0}.tar.gz'.format(KIBANA_VERSION))
        client.run('cd /root; tar xvf /root/{0}.tar'.format(KIBANA_VERSION))
        status = client.run('service logstash status')
        if 'stop' in status:
            client.run('service logstash start')
        else:
            client.run('service logstash restart')

    @staticmethod
    def init_node(ip, join_masters=False):
        """
        Initializes a node, making sure all required services are up and running.
        Optionally, the node can also join the masters, also participating in the arakoon and memcache
        clusters. Joining the masters will result in the services being restarted on all master nodes.
        Please note that initializing a node, joining a grid with < 3 nodes, will result in joining the
        master nodes regardless of the given parameter.
        """

        def _update_es_configuration(es_client, value):
            # update elasticsearch configuration
            config_file = '/etc/elasticsearch/elasticsearch.yml'
            es_client.run('service elasticsearch stop')
            Manager.replace_param_in_config(es_client,
                                            config_file,
                                            '<IS_POTENTIAL_MASTER>',
                                            value)
            Manager.replace_param_in_config(es_client,
                                            config_file,
                                            '<IS_DATASTORE>',
                                            value)
            es_client.run('service elasticsearch start')
            es_client.run('service logstash restart')

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

        if Manager._validate_ip(ip) is False:
            print 'The entered ip address is invalid'
            sys.exit(1)

        if isinstance(join_masters, bool) is False:
            print 'Some arguments contain invalid data'
            sys.exit(1)

        nodes = Manager._get_cluster_nodes()  # All nodes, including the local and the new one

        # Generate RSA keypairs
        print 'Setting up key authentication.'
        print 'Enter root password for: {0}'.format(ip)
        local_password = getpass.getpass()
        client = Client.load(ip, local_password)
        root_ssh_folder = '{0}/.ssh'.format(client.run('echo ~'))
        ovs_ssh_folder = '{0}/.ssh'.format(client.run('su - ovs -c "echo ~"'))
        private_key_filename = '{0}/id_rsa'
        public_key_filename = '{0}/id_rsa.pub'
        authorized_keys_filename = '{0}/authorized_keys'
        known_hosts_filename = '{0}/known_hosts'
        # Generate keys for root
        client.dir_ensure(root_ssh_folder)
        client.run("ssh-keygen -t rsa -b 4096 -f {0} -N ''".format(private_key_filename.format(root_ssh_folder)))
        # Generate keys for ovs
        client.run('su - ovs -c "mkdir -p {0}"'.format(ovs_ssh_folder))
        client.run('su - ovs -c "ssh-keygen -t rsa -b 4096 -f {0} -N \'\'"'.format(private_key_filename.format(ovs_ssh_folder)))
        root_public_key = client.file_read(public_key_filename.format(root_ssh_folder)).strip()
        ovs_public_key = client.file_read(public_key_filename.format(ovs_ssh_folder)).strip()
        root_authorized_keys = ''
        ovs_authorized_keys = ''
        for node in nodes:
            if node != ip:
                print 'Enter the root password for: {0}'.format(node)
                node_password = getpass.getpass()
            else:
                node_password = local_password
            node_client = Client.load(node, node_password)
            for user, folder in [('root', root_ssh_folder), ('ovs', ovs_ssh_folder)]:
                node_client.run('su - {0} -c "touch {1}"'.format(user, known_hosts_filename.format(folder)))
                node_client.run('su - {0} -c "chmod 600 {1}; chown {0}:{0} {1}"'.format(user, known_hosts_filename.format(folder)))
                node_client.run('su - {0} -c "echo \'\' > {1}"'.format(user, known_hosts_filename.format(folder)))
                for subnode in nodes:
                    node_client.run('su - {0} -c "ssh-keyscan -H {1} >> {2}"'.format(user, subnode, known_hosts_filename.format(folder)))
            root_authorized_keys += node_client.file_read(public_key_filename.format(root_ssh_folder))
            ovs_authorized_keys += node_client.file_read(public_key_filename.format(ovs_ssh_folder))
            # Root keys
            if node_client.file_exists(authorized_keys_filename.format(root_ssh_folder)):
                node_authorized_keys = node_client.file_read(authorized_keys_filename.format(root_ssh_folder))
            else:
                node_authorized_keys = ''
            changed = False
            if root_public_key not in node_authorized_keys:
                node_authorized_keys += root_public_key + '\n'
                changed = True
            if ovs_public_key not in node_authorized_keys:
                node_authorized_keys += ovs_public_key + '\n'
                changed = True
            if changed:
                for user, folder in [('root', root_ssh_folder), ('ovs', ovs_ssh_folder)]:
                    client.run('su - {0} -c "touch {1}"'.format(user, authorized_keys_filename.format(folder)))
                    node_client.run('su - {0} -c "chmod 600 {1}; chown {0}:{0} {1}"'.format(user, authorized_keys_filename.format(folder)))
                    node_client.file_write(authorized_keys_filename.format(folder), node_authorized_keys)
        client = Client.load(ip, local_password)
        client.file_write(authorized_keys_filename.format(root_ssh_folder), root_authorized_keys + ovs_authorized_keys)
        client.file_write(authorized_keys_filename.format(ovs_ssh_folder), root_authorized_keys + ovs_authorized_keys)

        print 'Starting initialization...'
        # Make sure to ALWAYS reload the client when switching targets, as Fabric seems to be singleton-ish
        is_local = Client.is_local(ip)
        client = Client.load(ip)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
        unique_id = sorted(client.run("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'").strip().split('\n'))[0].strip()

        arakoon_management = ArakoonManagement()
        arakoon_nodes = arakoon_management.getCluster('ovsdb').listNodes()
        if unique_id in arakoon_nodes:
            arakoon_nodes.remove(unique_id)
        client = Client.load(ip)
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

        model_services = ['arakoon_ovsdb', 'memcached', 'arakoon_voldrv']
        master_services = ['rabbitmq', 'ovs_scheduled_tasks']
        extra_services = ['webapp_api', 'nginx', 'ovs_workers', 'ovs_consumer_volumerouter']
        all_services = model_services + master_services + extra_services
        arakoon_clientconfigfiles = [ARAKOON_CLIENTCONFIG_TAG.format(cluster) for cluster in clusters]
        generic_configfiles = {'/opt/OpenvStorage/config/memcacheclient.cfg': 11211,
                               '/opt/OpenvStorage/config/rabbitmqclient.cfg': 5672}

        # Workaround for JumpScale process manager issue where the processmanager will restart
        # processes even when they are disabled.
        for node in nodes:
            node_client = Client.load(node)
            processes = node_client.run("ps aux | grep jumpscale | grep process").split('\n')
            if len(processes) > 0:
                for process in processes:
                    if 'processmanager' in process:
                        while '  ' in process:
                            process = process.replace('  ', ' ')
                        pid = process.split(' ')[1]
                        node_client.run('kill -9 {0}'.format(pid))

        is_master = False
        if join_masters:
            print 'Joining master nodes, services going down.'

            # Stop services (on all nodes)
            for node in nodes:
                node_client = Client.load(node)
                for service in all_services:
                    node_client.run('jsprocess disable -n {0}'.format(service))
                    node_client.run('jsprocess stop -n {0}'.format(service))

            # Fetch some information
            client = Client.load(ip)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
            remote_ips = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
            remote_ip = [ipa.strip() for ipa in remote_ips if ipa.strip() in nodes][0]

            # Configure arakoon
            for cluster in clusters:
                # The Arakoon extension is not used since the config file needs to be parsed/loaded anyway to be
                # able to update it
                cfg = ConfigObj(ARAKOON_CONFIG_TAG.format(cluster))
                global_section = cfg.get('global')
                cluster_nodes = global_section['cluster'] if type(global_section['cluster']) == list else [global_section['cluster']]
                if unique_id not in cluster_nodes:
                    client = Client.load(ip)
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
                        node_client = Client.load(node)
                        node_client.file_upload(ARAKOON_CONFIG_TAG.format(cluster),
                                                ARAKOON_CONFIG_TAG.format(cluster))
                client = Client.load(ip)
                arakoon_create_directories = """
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
arakoon_management = ArakoonManagement()
arakoon_cluster = arakoon_management.getCluster('%(cluster)s')
arakoon_cluster.createDirs(arakoon_cluster.listLocalNodes()[0])
""" % {'cluster': cluster}
                Manager._exec_python(client, arakoon_create_directories)

            # Update all nodes hosts file with new node and new node hosts file with all others
            for node in nodes:
                client_node = Client.load(node)
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
                        client_node = Client.load(subnode)
                        node_hostname = client_node.run('hostname')
                        update_hosts_file = """
from ovs.plugin.provider.net import Net
Net.updateHostsFile(hostsfile='/etc/hosts', ip='%(ip)s', hostname='%(host)s')
""" % {'ip': subnode,
       'host': node_hostname}
                        client = Client.load(ip)
                        Manager._exec_python(client, update_hosts_file)

            # Join rabbitMQ clusters
            client = Client.load(ip)
            client.run('rabbitmq-server -detached; sleep 5; rabbitmqctl stop_app; sleep 5; rabbitmqctl reset; sleep 5; rabbitmqctl stop; sleep 5;')
            if not is_local:
                # Copy rabbitmq cookie
                rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'
                client.dir_ensure(os.path.dirname(rabbitmq_cookie_file), True)
                client.file_upload(rabbitmq_cookie_file, rabbitmq_cookie_file)
                client.file_attribs(rabbitmq_cookie_file, mode=400)
                client.run('rabbitmq-server -detached; sleep 5; rabbitmqctl stop_app; sleep 5;')
                # If not local, a cluster needs to be joined.
                master_client = Client.load(Configuration.get('grid.master.ip'))
                master_hostname = master_client.run('hostname')
                client = Client.load(ip)
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
                node_client = Client.load(node)
                for config in arakoon_clientconfigfiles + generic_configfiles.keys():
                    node_client.file_upload(config, config)

            # Update arakoon cluster configuration in voldrv configuration files
            for node in nodes:
                client_node = Client.load(node)
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

            # Update possible volumedrivers with new amqp configuration.
            # On each node, it will loop trough all already configured vpools and update their amqp connection
            # info with those of the new rabbitmq client configuration file.
            for node in nodes:
                node_client = Client.load(node)
                Manager._configure_amqp_to_volumedriver(node_client)

            client = Client.load(ip)
            Manager._configure_nginx(client)

            # Restart services
            for node in nodes:
                node_client = Client.load(node)
                for service in model_services:
                    node_client.run('jsprocess enable -n {0}'.format(service))
                    node_client.run('jsprocess start -n {0}'.format(service))
            is_master = True

            # If this is first node we need to load default model values.
            # @TODO: Think about better detection algorithm.
            if len(nodes) == 1:
                from ovs.extensions.migration.migration import Migration
                Migration.migrate()
            else:
                # we might need to disable running logstash-web on 2+ node
                # client.run('rm /etc/init/logstash-web.conf')
                pass
            client = Client.load(ip)
            _update_es_configuration(client, 'true')

        else:
            client = Client.load(ip)
            # Disable master and model services
            for service in master_services + model_services:
                client.run('jsprocess disable -n {0}'.format(service))
            # Stop services
            for service in all_services:
                client.run('jsprocess stop -n {0}'.format(service))

            # The client config files can be copied from this node, since all client configurations are equal
            for config in arakoon_clientconfigfiles + generic_configfiles.keys():
                client.file_upload(config, config)
            Manager._configure_nginx(client)

            client = Client.load(ip)
            _update_es_configuration(client, 'false')

        client = Client.load(ip)
        client.run('mkdir -p /opt/OpenvStorage/webapps/frontend/logging')
        client.run('service logstash restart')

        Manager.replace_param_in_config(client,
                                        '/root/{0}/config.js'.format(KIBANA_VERSION),
                                        'http://"+window.location.hostname+":9200',
                                        'http://' + ip + ':9200')
        client.run('cp /root/{0}/app/dashboards/guided.json /root/{0}/app/dashboards/default.json'.format(KIBANA_VERSION, KIBANA_VERSION))
        client.run('cp -R /root/{0}/* /opt/OpenvStorage/webapps/frontend/logging'.format(KIBANA_VERSION))
        client.run('chown -R ovs:ovs /opt/OpenvStorage/webapps/frontend/logging')

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

        # Add VSA and pMachine in the model, if they don't yet exist
        client = Client.load(ip)
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
            vsa.ip = Manager._read_remote_config(client, 'ovs.grid.ip')
            vsa.save()
        vsa.pmachine = pmachine
        vsa.save()

        if is_master is True:
            for node in nodes:
                node_client = Client.load(node)
                for service in master_services + extra_services:
                    node_client.run('jsprocess enable -n {0}'.format(service))
                    node_client.run('jsprocess start -n {0}'.format(service))
            # Enable HA for the rabbitMQ queues
            client = Client.load(ip)
            client.run('rabbitmqctl set_policy ha-all "^(volumerouter|ovs_.*)$" \'{"ha-mode":"all"}\'')
        else:
            for node in nodes:
                node_client = Client.load(node)
                for service in extra_services:
                    node_client.run('jsprocess enable -n {0}'.format(service))
                    node_client.run('jsprocess start -n {0}'.format(service))

        for node in nodes:
            node_client = Client.load(node)
            try:
                node_client.run('service processmanager start')
            except:
                pass
            node_client.run('jsprocess restart -n ovs_workers')

            node_client.run('jsprocess enable -n ovs_snmp_server')
            node_client.run('jsprocess start -n ovs_snmp_server')

    @staticmethod
    def init_vpool(ip, vpool_name, parameters=None):
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

        parameters = {} if parameters is None else parameters

        if Manager._validate_ip(ip) is False:
            print 'The entered ip address is invalid'
            sys.exit(1)

        if isinstance(parameters, dict) is False:
            print 'Some arguments contain invalid data'
            sys.exit(1)

        while not re.match('^[0-9a-zA-Z]+(\-+[0-9a-zA-Z]+)*$', vpool_name):
            print 'Invalid vPool name given. Only 0-9, a-z, A-Z and - are allowed.'
            suggestion = re.sub(
                '^([\-_]*)(?P<correct>[0-9a-zA-Z]+([\-_]+[0-9a-zA-Z]+)*)([\-_]*)$',
                '\g<correct>',
                re.sub('[^0-9a-zA-Z\-_]', '_', vpool_name)
            )
            vpool_name = Helper.ask_string('Provide new vPool name', default_value=suggestion)

        client = Client.load(ip)  # Make sure to ALWAYS reload the client, as Fabric seems to be singleton-ish
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
            node_client = Client.load(node)
            for service in services:
                Manager._exec_python(node_client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.disable_service('{0}')
""".format(service))
                Manager._exec_python(node_client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.stop_service('{0}')
""".format(service))

        # Keep in mind that if the VSR exists, the vPool does as well

        client = Client.load(ip)
        mountpoints = client.run('mount -v').strip().split('\n')
        mountpoints = [p.split(' ')[2] for p in mountpoints if
                       len(p.split(' ')) > 2 and ('/mnt/' in p.split(' ')[2] or '/var' in p.split(' ')[2])]
        mountpoint_bfs = ''
        directories_to_create = []

        if vpool is None:
            vpool = VPool()
            supported_backends = Manager._read_remote_config(client, 'volumedriver.supported.backends').split(',')
            if 'REST' in supported_backends:
                supported_backends.remove('REST')  # REST is not supported for now
            vpool.backend_type = parameters.get('backend_type') or Helper.ask_choice(supported_backends, 'Select type of storage backend', default_value='CEPH_S3')
            connection_host = connection_port = connection_username = connection_password = None
            if vpool.backend_type in ['LOCAL', 'DISTRIBUTED']:
                vpool.backend_metadata = {'backend_type': 'LOCAL'}
                mountpoint_bfs = parameters.get('mountpoint_bfs') or Helper.ask_string('Specify {0} storage backend directory'.format(vpool.backend_type.lower()))
                directories_to_create.append(mountpoint_bfs)
                if vpool.backend_type == 'DISTRIBUTED':
                    vpool.backend_metadata['local_connection_path'] = mountpoint_bfs
            if vpool.backend_type == 'REST':
                connection_host = parameters.get('connection_host') or Helper.ask_string('Provide REST ip address')
                connection_port = parameters.get('connection_port') or Helper.ask_integer('Provide REST connection port', min_value=1, max_value=65535)
                rest_connection_timeout_secs = parameters.get('connection_timeout') or Helper.ask_integer('Provide desired REST connection timeout(secs)',
                                                                                                          min_value=0, max_value=99999)
                vpool.backend_metadata = {'rest_connection_host': connection_host,
                                          'rest_connection_port': connection_port,
                                          'buchla_connection_log_level': "0",
                                          'rest_connection_verbose_logging': rest_connection_timeout_secs,
                                          'rest_connection_metadata_format': "JSON",
                                          'backend_type': 'REST'}
            elif vpool.backend_type in ('CEPH_S3', 'AMAZON_S3', 'SWIFT_S3'):
                connection_host = parameters.get('connection_host') or Helper.ask_string('Specify fqdn or ip address for your S3 compatible host')
                connection_port = parameters.get('connection_port') or Helper.ask_integer('Specify port for your S3 compatible host: ', min_value=1,
                                                                                          max_value=65535)
                connection_username = parameters.get('connection_username') or Helper.ask_string('Specify S3 access key')
                connection_password = parameters.get('connection_password') or getpass.getpass()
                strict_consistency = 'false' if vpool.backend_type in ['SWIFT_S3'] else 'true'
                vpool.backend_metadata = {'s3_connection_host': connection_host,
                                          's3_connection_port': connection_port,
                                          's3_connection_username': connection_username,
                                          's3_connection_password': connection_password,
                                          's3_connection_flavour': 'S3',
                                          's3_connection_strict_consistency': strict_consistency,
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

        mountpoint_temp = parameters.get('mountpoint_temp') or Helper.ask_choice(mountpoints,
                                                                                 question='Select temporary FS mountpoint',
                                                                                 default_value=Helper.find_in_list(mountpoints, 'tmp'))
        if mountpoint_temp in mountpoints:
            mountpoints.remove(mountpoint_temp)
        mountpoint_md = parameters.get('mountpoint_md') or Helper.ask_choice(mountpoints,
                                                                             question='Select metadata mountpoint',
                                                                             default_value=Helper.find_in_list(mountpoints, 'md'))
        if mountpoint_md in mountpoints:
            mountpoints.remove(mountpoint_md)
        mountpoint_cache = parameters.get('mountpoint_cache') or Helper.ask_choice(mountpoints,
                                                                                   question='Select cache mountpoint',
                                                                                   default_value=Helper.find_in_list(mountpoints, 'cache'))
        if mountpoint_cache in mountpoints:
            mountpoints.remove(mountpoint_cache)

        directories_to_create.append(mountpoint_temp)
        directories_to_create.append(mountpoint_md)
        directories_to_create.append(mountpoint_cache)

        client = Client.load(ip)
        dir_create_script = """
import os
for directory in {0}:
    if not os.path.exists(directory):
        os.makedirs(directory)""".format(directories_to_create)
        Manager._exec_python(client, dir_create_script)

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
            ports_used_in_model = [port_vsr.port for port_vsr in VolumeStorageRouterList.get_volumestoragerouters_by_vsa(vsa.guid)]
            vrouter_port_in_hrd = int(Manager._read_remote_config(client, 'volumedriver.filesystem.xmlrpc.port'))
            if vrouter_port_in_hrd in ports_used_in_model:
                vrouter_port = int(parameters.get('vrouter_port')) or Helper.ask_integer('Provide Volumedriver connection port (make sure port is not in use)',
                                                                                    min_value=1024, max_value=max(ports_used_in_model) + 3)
            else:
                vrouter_port = int(vrouter_port_in_hrd)
        else:
            vrouter_port = int(vsr.port)

        ipaddresses = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().split('\n')
        ipaddresses = [ipaddr.strip() for ipaddr in ipaddresses]
        grid_ip = Manager._read_remote_config(client, 'ovs.grid.ip')
        if grid_ip in ipaddresses:
            ipaddresses.remove(grid_ip)
        if not ipaddresses:
            raise RuntimeError('No available ip addresses found suitable for volumerouter storage ip')
        if vsa.pmachine.hvtype == 'KVM':
            volumedriver_storageip = '127.0.0.1'
        else:
            volumedriver_storageip = parameters.get('storage_ip') or Helper.ask_choice(ipaddresses, 'Select storage ip address for this vpool')
        vrouter_id = '{0}{1}'.format(vpool_name, unique_id)

        vrouter_config = {'vrouter_id': vrouter_id,
                          'vrouter_redirect_timeout_ms': '5000',
                          'vrouter_routing_retries': 10,
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
        filesystem_config = {'fs_backend_path': mountpoint_bfs}
        volumemanager_config = {'metadata_path': metadatapath, 'tlog_path': tlogpath}
        vsr_config_script = """
from ovs.plugin.provider.configuration import Configuration
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration

fd_config = {{'fd_cache_path': '{11}/fd_{0}',
              'fd_extent_cache_capacity': '1024',
              'fd_namespace' : 'fd-{0}',
              'fd_policy_id' : ''}}
vsr_configuration = VolumeStorageRouterConfiguration('{0}')
vsr_configuration.configure_backend({1})
vsr_configuration.configure_readcache({2}, Configuration.get('volumedriver.readcache.serialization.path'))
vsr_configuration.configure_scocache({3}, '1GB', '2GB')
vsr_configuration.configure_failovercache('{4}')
vsr_configuration.configure_filesystem({5})
vsr_configuration.configure_volumemanager({6})
vsr_configuration.configure_volumerouter('{0}', {7})
vsr_configuration.configure_arakoon_cluster('{8}', {9})
vsr_configuration.configure_hypervisor('{10}')
vsr_configuration.configure_filedriver(fd_config)
""".format(vpool_name, vpool.backend_metadata, readcaches, scocaches, failovercache, filesystem_config,
           volumemanager_config, vrouter_config, voldrv_arakoon_cluster_id, voldrv_arakoon_client_config,
           vsa.pmachine.hvtype, mountpoint_cache)
        Manager._exec_python(client, vsr_config_script)
        Manager._configure_amqp_to_volumedriver(client, vpool_name)

        # Updating the model
        vsr.vsrid = vrouter_id
        vsr.name = vrouter_id.replace('_', ' ')
        vsr.description = vsr.name
        vsr.storage_ip = volumedriver_storageip
        vsr.cluster_ip = grid_ip
        vsr.port = vrouter_port
        vsr.mountpoint = '/mnt/{0}'.format(vpool_name)
        vsr.mountpoint_temp = mountpoint_temp
        vsr.mountpoint_cache = mountpoint_cache
        vsr.mountpoint_bfs = mountpoint_bfs
        vsr.mountpoint_md = mountpoint_md
        vsr.serving_vmachine = vsa
        vsr.vpool = vpool
        vsr.save()

        dirs2create.append(vsr.mountpoint)
        dirs2create.append(mountpoint_cache + '/fd_' + vpool_name)

        file_create_script = """
import os
for directory in {0}:
    if not os.path.exists(directory):
        os.makedirs(directory)
for filename in {1}:
    if not os.path.exists(filename):
        open(filename, 'a').close()""".format(dirs2create, files2create)
        Manager._exec_python(client, file_create_script)

        voldrv_config_file = '{0}/voldrv_vpools/{1}.json'.format(Manager._read_remote_config(client, 'ovs.core.cfgdir'), vpool_name)
        log_file = '/var/log/ovs/volumedriver/{0}.log'.format(vpool_name)
        vd_cmd = '/usr/bin/volumedriver_fs -f --config-file={0} --mountpoint {1} --logrotation --logfile {2} -o big_writes -o sync_read -o allow_other -o default_permissions'.format(voldrv_config_file, vsr.mountpoint, log_file)
        if vsa.pmachine.hvtype == 'KVM':
            vd_stopcmd = 'umount {0}'.format(vsr.mountpoint)
        else:
            vd_stopcmd = 'exportfs -u *:{0}; umount {0}'.format(vsr.mountpoint)
        vd_name = 'volumedriver_{}'.format(vpool_name)

        log_file = '/var/log/ovs/volumedriver/foc_{0}.log'.format(vpool_name)
        fc_cmd = '/usr/bin/failovercachehelper --config-file={0} --logfile={1}'.format(voldrv_config_file, log_file)
        fc_name = 'failovercache_{0}'.format(vpool_name)

        params = {'<VPOOL_MOUNTPOINT>': vsr.mountpoint,
                  '<HYPERVISOR_TYPE>': vsa.pmachine.hvtype,
                  '<VPOOL_NAME>': vpool_name,
                  '<UUID>': str(uuid.uuid4())}

        if client.file_exists('/opt/OpenvStorage/config/templates/upstart/ovs-volumedriver.conf'):
            client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-volumedriver.conf /opt/OpenvStorage/config/templates/upstart/ovs-volumedriver_{0}.conf'.format(vpool_name))
            client.run('cp -f /opt/OpenvStorage/config/templates/upstart/ovs-failovercache.conf /opt/OpenvStorage/config/templates/upstart/ovs-failovercache_{0}.conf'.format(vpool_name))

        service_script = """
from ovs.plugin.provider.service import Service
Service.add_service(package=('openvstorage', 'volumedriver'), name='{0}', command='{1}', stop_command='{2}', params={5})
Service.add_service(package=('openvstorage', 'failovercache'), name='{3}', command='{4}', stop_command=None, params={5})""".format(
            vd_name, vd_cmd, vd_stopcmd,
            fc_name, fc_cmd, params
        )
        Manager._exec_python(client, service_script)

        if vsa.pmachine.hvtype == 'VMWARE':
            client.run("grep -q '/tmp localhost(ro,no_subtree_check)' /etc/exports || echo '/tmp localhost(ro,no_subtree_check)' >> /etc/exports")
            client.run('service nfs-kernel-server start')

        if vsa.pmachine.hvtype == 'KVM':
            client.run('virsh pool-define-as {0} dir - - - - {1}'.format(vpool_name, vsr.mountpoint))
            client.run('virsh pool-build {0}'.format(vpool_name))
            client.run('virsh pool-start {0}'.format(vpool_name))
            client.run('virsh pool-autostart {0}'.format(vpool_name))

        # Start services
        for node in nodes:
            node_client = Client.load(node)
            for service in services:
                Manager._exec_python(node_client, """
from ovs.plugin.provider.service import Service
Service.enable_service('{0}')
""".format(service))
                Manager._exec_python(node_client, """
from ovs.plugin.provider.service import Service
Service.start_service('{0}')
""".format(service))

        # Fill vPool size
        vfs_info = os.statvfs('/mnt/{0}'.format(vpool_name))
        vpool.size = vfs_info.f_blocks * vfs_info.f_bsize
        vpool.save()

    @staticmethod
    def remove_vpool(vsr_guid):
        """
        Removes a VSA-vPool link (VSR). If it's the last VSR for the vPool, the vPool will be completely removed
        """
        from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
        from ovs.dal.lists.vmachinelist import VMachineList
        from volumedriver.storagerouter.storagerouterclient import LocalStorageRouterClient, ClusterRegistry, ArakoonNodeConfig, ClusterNodeConfig
        from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement

        # Get objects & Make some checks
        vsr = VolumeStorageRouter(vsr_guid)
        vmachine = vsr.serving_vmachine
        ip = vmachine.ip
        pmachine = vmachine.pmachine
        vmachines = VMachineList.get_customer_vmachines()
        pmachine_guids = [vm.pmachine_guid for vm in vmachines]
        vpools_guids = [vm.vpool_guid for vm in vmachines if vm.vpool_guid is not None]

        vpool = vsr.vpool
        if pmachine.guid in pmachine_guids and vpool.guid in vpools_guids:
            raise RuntimeError('There are still vMachines served from the given VSR')
        if any(vdisk for vdisk in vpool.vdisks if vdisk.vsrid == vsr.vsrid):
            raise RuntimeError('There are still vDisks served from the given VSR')

        services = ['volumedriver_{0}'.format(vpool.name),
                    'failovercache_{0}'.format(vpool.name)]
        vsrs_left = False

        # Stop services
        for current_vsr in vpool.vsrs:
            if current_vsr.guid != vsr_guid:
                vsrs_left = True
            client = Client.load(current_vsr.serving_vmachine.ip)
            for service in services:
                Manager._exec_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.disable_service('{0}')
""".format(service))
                Manager._exec_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.stop_service('{0}')
""".format(service))

        # KVM pool
        client = Client.load(ip)
        if pmachine.hvtype == 'KVM':
            if vpool.name in client.run('virsh pool-list'):
                client.run('virsh pool-destroy {0}'.format(vpool.name))
            try:
                client.run('virsh pool-undefine {0}'.format(vpool.name))
            except:
                pass  # Ignore undefine errors, since that can happen on re-entrance

        # Remove services
        client = Client.load(ip)
        for service in services:
            Manager._exec_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.remove_service(domain='openvstorage', name='{0}')
""".format(service))
        configuration_dir = Manager._read_remote_config(client, 'ovs.core.cfgdir')

        voldrv_arakoon_cluster_id = str(Manager._read_remote_config(client, 'volumedriver.arakoon.clusterid'))
        voldrv_arakoon_cluster = ArakoonManagement().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        arakoon_node_configs = []
        for arakoon_node in voldrv_arakoon_client_config.keys():
            arakoon_node_configs.append(ArakoonNodeConfig(arakoon_node,
                                                          voldrv_arakoon_client_config[arakoon_node][0][0],
                                                          voldrv_arakoon_client_config[arakoon_node][1]))
        vrouter_clusterregistry = ClusterRegistry(str(vpool.name), voldrv_arakoon_cluster_id, arakoon_node_configs)
        # Reconfigure volumedriver
        if vsrs_left:
            node_configs = []
            for current_vsr in vpool.vsrs:
                if current_vsr.guid != vsr_guid:
                    node_configs.append(ClusterNodeConfig(str(current_vsr.vsrid), str(current_vsr.cluster_ip),
                                                          current_vsr.port - 1, current_vsr.port, current_vsr.port + 1))
            vrouter_clusterregistry.set_node_configs(node_configs)
        else:
            storagedriver_client = LocalStorageRouterClient('{0}/voldrv_vpools/{1}.json'.format(configuration_dir, vpool.name))
            storagedriver_client.destroy_filesystem()
            vrouter_clusterregistry.erase_node_configs()

        # Remove directories
        client = Client.load(ip)
        client.run('rm -rf {}/sco_{}'.format(vsr.mountpoint_cache, vpool.name))
        client.run('rm -rf {}/foc_{}'.format(vsr.mountpoint_cache, vpool.name))
        client.run('rm -rf {}/metadata_{}'.format(vsr.mountpoint_md, vpool.name))
        client.run('rm -rf {}/tlogs_{}'.format(vsr.mountpoint_md, vpool.name))
        client.run('rmdir {}'.format(vsr.mountpoint))

        # Remove files
        client.run('rm -f {}/read_{}'.format(vsr.mountpoint_cache, vpool.name))
        client.run('rm -f {0}/voldrv_vpools/{1}.json'.format(configuration_dir, vpool.name))

        # First model cleanup
        vsr.delete()

        if vsrs_left:
            # Restart leftover services
            for current_vsr in vpool.vsrs:
                if current_vsr.guid != vsr_guid:
                    client = Client.load(current_vsr.serving_vmachine.ip)
                    for service in services:
                        Manager._exec_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.enable_service('{0}')
""".format(service))
                        Manager._exec_python(client, """
from ovs.plugin.provider.service import Service
if Service.has_service('{0}'):
    Service.start_service('{0}')
""".format(service))
        else:
            # Final model cleanup
            vpool.delete()

    @staticmethod
    def _configure_amqp_to_volumedriver(client, vpname=None):
        """
        Reads out the RabbitMQ client config, using that to (re)configure the volumedriver configuration(s)
        """
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
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration
queue_config = {{'events_amqp_routing_key': Configuration.get('ovs.core.broker.volumerouter.queue'),
                 'events_amqp_uris': uris}}
for config_file in os.listdir('/opt/OpenvStorage/config/voldrv_vpools'):
    this_vpool_name = config_file.replace('.json', '')
    if config_file.endswith('.json') and (vpool_name is None or vpool_name == this_vpool_name):
        vsr_configuration = VolumeStorageRouterConfiguration(this_vpool_name)
        vsr_configuration.configure_event_publisher(queue_config)"""
        Manager._exec_python(client, remote_script.format(vpname if vpname is None else "'{0}'".format(vpname)))

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
        if client.file_exists('/opt/OpenvStorage/bin/activate'):
            return client.run('source /opt/OpenvStorage/bin/activate; python -c """{0}"""'.format(script))
        else:
            return client.run('python -c """{0}"""'.format(script))

    @staticmethod
    def _get_cluster_nodes():
        """
        Get nodes from Osis
        """
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
            if local_ovs_grid_ip in node.ipaddr:
                # For the local node, the local grid ip is saved as node ip
                grid_nodes.append(local_ovs_grid_ip)
            else:
                for ip in node.ipaddr:
                    if Manager._get_local_endpoint_to(ip, 22) == local_ovs_grid_ip:
                        grid_nodes.append(ip)
                        ip_found = True
                        break
                if not ip_found:
                    raise RuntimeError('No suitable ip address found for node {0}'.format(node.machineguid))
        return grid_nodes

    @staticmethod
    def _get_local_endpoint_to(ip, port, timeout=2):
        """
        Checks from which local ip this machine can connect to a given ip and port. Returns None if it can't connect
        """
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(timeout)
            sock.connect((ip, port))
        except:
            return None
        return sock.getsockname()[0]

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

        def try_run(command):
            """
            Tries executing a command, ignoring any error
            """
            try:
                client.run(command)
            except:
                pass

        try_run('service nfs-kernel-server stop')
        try_run('pkill arakoon')
        try_run('rm -rf /usr/local/lib/python2.7/*-packages/JumpScale*')
        try_run('rm -rf /usr/local/lib/python2.7/dist-packages/jumpscale.pth')
        try_run('rm -rf /opt/jumpscale')
        try_run('rm -rf /opt/OpenvStorage')
        try_run('rm -rf /mnt/db/arakoon /mnt/db/tlogs /mnt/cache/foc /mnt/cache/sco /mnt/cache/read')

    @staticmethod
    def _create_filesystems(client, create_extra):
        """
        Creates filesystems on the first two additional disks
        """
        # Scan block devices
        drive_lines = client.run("ls -l /dev/* | grep -E '/dev/(sd..?|fio..?)' | sed 's/\s\s*/ /g' | cut -d ' ' -f 10").split('\n')
        drives = {}
        for drive in drive_lines:
            partition = drive.strip()
            if partition == '':
                continue
            drive = partition.translate(None, digits)
            if '/dev/sd' in drive:
                if drive not in drives:
                    identifier = drive.replace('/dev/', '')
                    if client.run('cat /sys/block/{0}/device/type'.format(identifier)).strip() == '0' \
                            and client.run('cat /sys/block/{0}/removable'.format(identifier)).strip() == '0':
                        ssd_output = ''
                        try:
                            ssd_output = client.run("/usr/bin/lsscsi | grep 'FUSIONIO' | grep {0}".format(drive)).strip()
                        except:
                            pass
                        try:
                            ssd_output += str(client.run("hdparm -I {0} 2> /dev/null | grep 'Solid State'".format(drive)).strip())
                        except:
                            pass
                        drives[drive] = {'ssd': ('Solid State' in ssd_output or 'FUSIONIO' in ssd_output),
                                         'partitions': []}
                if drive in drives:
                    drives[drive]['partitions'].append(partition)
            else:
                if drive not in drives:
                    drives[drive] = {'ssd': True, 'partitions': []}
                drives[drive]['partitions'].append(partition)
        mounted = [device.strip() for device in client.run("mount | cut -d ' ' -f 1").strip().split('\n')]
        root_partition = client.run("mount | grep 'on / ' | cut -d ' ' -f 1").strip()
        # Start preparing partitions
        extra_mountpoints = ''
        hdds = [drive for drive, info in drives.iteritems() if info['ssd'] is False and root_partition not in info['partitions']]
        if create_extra:
            # Create partitions on HDD
            if len(hdds) == 0:
                print 'No HDD was found. At least one HDD is required when creating extra filesystems'
                sys.exit(1)
            if len(hdds) > 1:
                hdd = Helper.ask_choice(hdds, question='Choose the HDD to use for Open vStorage')
            else:
                hdd = hdds[0]
            print 'Using {0} as extra HDD'.format(hdd)
            hdds.remove(hdd)
            for partition in drives[hdd]['partitions']:
                if partition in mounted:
                    client.run('umount {0}'.format(partition))
            client.run('parted {0} -s mklabel gpt'.format(hdd))
            client.run('parted {0} -s mkpart backendfs 2MB 80%'.format(hdd))
            client.run('parted {0} -s mkpart tempfs 80% 100%'.format(hdd))
            client.run('mkfs.ext4 -q {0}1 -L backendfs'.format(hdd))
            client.run('mkfs.ext4 -q {0}2 -L tempfs'.format(hdd))

            extra_mountpoints = """
LABEL=backendfs /mnt/bfs         ext4    defaults,nobootwait,noatime,discard    0    2
LABEL=tempfs    /var/tmp         ext4    defaults,nobootwait,noatime,discard    0    2
"""

        # Create partitions on SSD
        ssds = [drive for drive, info in drives.iteritems() if info['ssd'] is True and root_partition not in info['partitions']]
        if len(ssds) == 0:
            if len(hdds) > 0:
                print 'No SSD found, but one or more HDDs are found that can be used instead.'
                print 'However, using a HDD instead of an SSD will cause severe performance loss.'
                continue_install = Helper.ask_yesno('Are you sure you want to continue?', default_value=False)
                if continue_install:
                    ssd = Helper.ask_choice(hdds, question='Choose the HDD to use as SSD replacement')
                else:
                    sys.exit(1)
            else:
                print 'No SSD found. At least one SSD (or replacing HDD) is required.'
                sys.exit(1)
        elif len(ssds) > 1:
            ssd = Helper.ask_choice(ssds, question='Choose the SSD to use for Open vStorage')
        else:
            ssd = ssds[0]
        print 'Using {0} as SSD'.format(ssd)
        for partition in drives[ssd]['partitions']:
            if partition in mounted:
                client.run('umount {0}'.format(partition))
        client.run('parted {0} -s mklabel gpt'.format(ssd))
        client.run('parted {0} -s mkpart cache 2MB 50%'.format(ssd))
        client.run('parted {0} -s mkpart db 50% 75%'.format(ssd))
        client.run('parted {0} -s mkpart mdpath 75% 100%'.format(ssd))
        client.run('mkfs.ext4 -q {0}1 -L cache'.format(ssd))
        client.run('mkfs.ext4 -q {0}2 -L db'.format(ssd))
        client.run('mkfs.ext4 -q {0}3 -L mdpath'.format(ssd))

        client.run('mkdir -p /mnt/db')
        client.run('mkdir -p /mnt/cache')
        client.run('mkdir -p /mnt/md')

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
        quality_mapping = {'unstable': ['stable', 'stable', '1.2.0'],
                           'test': ['stable', 'stable', '1.1.0'],
                           'stable': ['stable', 'stable', '1.0.2']}

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

        return quality_mapping[quality_level][1], quality_mapping[quality_level][2]

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

    @staticmethod
    def _validate_ip(ip):
        regex = '^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$'
        match = re.search(regex, ip)
        return match is not None


class Client(object):
    """
    Remote/local client
    """

    @staticmethod
    def load(ip, password=None, bypass_local=False):
        """
        Opens a client connection to a remote or local system
        """
        class LocalClient(object):
            """
            Provides local client functionality, having the same interface as the "Remote" client
            """

            @staticmethod
            def run(command, pty=None):
                """
                Executes a command
                """
                _ = pty  # Compatibility with Cuisine
                return check_output(command, shell=True)

            @staticmethod
            def file_read(filename):
                """
                Reads a file
                """
                with open(filename, 'r') as the_file:
                    return the_file.read()

            @staticmethod
            def file_exists(filename):
                """
                Checks whether a filename exists
                """
                return os.path.isfile(filename)

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
            Remote.cuisine.fabric.output['stdout'] = True
            Remote.cuisine.fabric.output['running'] = True
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
            ynstring = ' (y/n): '
            failuremsg = "Illegal value. Press 'y' or 'n'."
        elif default_value is True:
            ynstring = ' ([y]/n): '
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        elif default_value is False:
            ynstring = ' (y/[n]): '
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
    parser.add_option('-v', '--version', dest='version',
                      help='Specify a version to install.')
    (options, args) = parser.parse_args()

    try:
        Manager.install_node('127.0.0.1',
                             create_extra_filesystems=options.filesystems,
                             clean=options.clean,
                             version=options.version)
    except KeyboardInterrupt:
        print '\nAborting'

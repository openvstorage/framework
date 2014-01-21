# license see http://www.openvstorage.com/licenses/opensource/
"""
OVS management module
"""

import subprocess
import uuid
import os
import re
import platform
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.console import Console
from ovs.plugin.provider.service import Service
from ovs.plugin.provider.package import Package
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration


class Configure():
    """
    Configuration class
    """

    def __init__(self):
        """
        Class constructor
        """
        pass

    @staticmethod
    def init_exportfs(vpool_name):
        """
        Configure nfs
        """
        from ovs.extensions.fs.exportfs import Nfsexports
        vpool_mountpoint = os.path.join(os.sep, 'mnt', vpool_name)
        if not os.path.exists(vpool_mountpoint):
            os.makedirs(vpool_mountpoint)
        Nfsexports().add(vpool_mountpoint, '*', 'rw,fsid={0},sync,no_root_squash,no_subtree_check'.format(uuid.uuid4()))
        subprocess.call(['service', 'nfs-kernel-server', 'start'])

    @staticmethod
    def load_data():
        """
        Load default data set
        """
        # Select/Create system vmachine
        hostname = platform.node()
        vmachine_list = VMachineList.get_vmachine_by_name(hostname)
        if vmachine_list and len(vmachine_list) == 1:
            print 'System vMachine already created, updating ...'
            vmachine = vmachine_list[0]
        elif not vmachine_list or len(vmachine_list) == 0:
            print 'Creating System vMachine'
            vmachine = VMachine()
        else:
            raise ValueError('Multiple System vMachines with name {} found, check your model'.format(hostname))

        # Select/Create host hypervisor node
        pmachine = PMachineList.get_by_ip(Configuration.get('ovs.host.ip'))
        if pmachine is None:
            pmachine = PMachine()

        # Model system VMachine and Hypervisor node
        pmachine.ip = Configuration.get('ovs.host.Liip')
        pmachine.username = Configuration.get('ovs.host.login')
        pmachine.password = Configuration.get('ovs.host.password')
        pmachine.hvtype = Configuration.get('ovs.host.hypervisor')
        pmachine.name = Configuration.get('ovs.host.name')
        pmachine.save()
        vmachine.name = hostname
        vmachine.machineid = Configuration.get('ovs.core.uniqueid')
        vmachine.hvtype = Configuration.get('ovs.host.hypervisor')
        vmachine.is_vtemplate = False
        vmachine.is_internal = True
        vmachine.ip = Configuration.get('ovs.grid.ip')
        vmachine.pmachine = pmachine
        vmachine.save()

        from ovs.extensions.migration.migration import Migration
        Migration.migrate()

        return vmachine.guid

    @staticmethod
    def init_rabbitmq():
        """
        Reconfigure rabbitmq to work with ovs user.
        """
        os.system('rabbitmq-server -detached; rabbitmqctl stop_app; rabbitmqctl reset; rabbitmqctl stop;')

    @staticmethod
    def init_nginx():
        """
        Init nginx
        """
        import re
        # Update nginx configuration to not run in daemon mode
        nginx_file_handle = open('/etc/nginx/nginx.conf', 'r+a')
        nginx_content = nginx_file_handle.readlines()
        daemon_off = False
        for line in nginx_content:
            if re.match('^daemon off.*', line):
                daemon_off = True
                break
        if not daemon_off:
            nginx_file_handle.write('daemon off;')
        nginx_file_handle.close()
        # Remove nginx default config
        if os.path.exists('/etc/nginx/sites-enabled/default'):
            os.remove('/etc/nginx/sites-enabled/default')

    @staticmethod
    def init_storagerouter(vmachineguid, vpool_name):
        """
        Initializes the volume storage router.
        This requires a the OVS model to be configured and reachable
        @param vmachineguid: guid of the internal VSA machine hosting this volume storage router
        """
        mountpoints = [Configuration.get('volumedriver.metadata'), ]
        for path in mountpoints:
            if not os.path.exists(path) or not os.path.ismount(path):
                raise ValueError('Path to {} does not exist or is not a mountpoint'.format(path))
        try:
            output = subprocess.check_output(['mount', '-v']).splitlines()
        except subprocess.CalledProcessError:
            output = []
        all_mounts = map(lambda m: m.split()[2], output)
        mount_regex = re.compile('^/$|/dev|/sys|/run|/proc|{}|{}|{}'.format(Configuration.get('ovs.core.db.mountpoint'),
                                                                            Configuration.get('volumedriver.filesystem.distributed'),
                                                                            Configuration.get('volumedriver.metadata')))
        filesystems = filter(lambda d: not mount_regex.match(d), all_mounts)
        volumedriver_cache_mountpoint = Console.askChoice(filesystems, 'Select cache mountpoint')
        filesystems.remove(volumedriver_cache_mountpoint)
        cache_fs = os.statvfs(volumedriver_cache_mountpoint)
        scocache = "{}/sco".format(volumedriver_cache_mountpoint)
        readcache = "{}/read".format(volumedriver_cache_mountpoint)
        failovercache = "{}/foc".format(volumedriver_cache_mountpoint)
        metadatapath = "{}/metadata".format(Configuration.get('volumedriver.metadata'))
        tlogpath = "{}/tlogs".format(Configuration.get('volumedriver.metadata'))
        dirs2create = [scocache,
                       failovercache,
                       Configuration.get('volumedriver.readcache.serialization.path'),
                       Configuration.get('volumedriver.filesystem.cache'),
                       metadatapath,
                       tlogpath]
        files2create = [readcache]
        # Cache sizes
        # 20% = scocache
        # 20% = failovercache (@todo: check if this can possibly consume more then 20%)
        # 60% = readcache
        scocache_size = "{0}KiB".format((int(cache_fs.f_bavail * 0.2 / 4096) * 4096) * 4)
        readcache_size = "{0}KiB".format((int(cache_fs.f_bavail * 0.6 / 4096) * 4096) * 4)
        supported_backends = Configuration.get('volumedriver.supported.backends').split(',')
        if 'REST' in supported_backends:
            supported_backends.remove('REST')  # REST is not supported for now
        volumedriver_backend_type = Console.askChoice(supported_backends, 'Select type of storage backend')
        vrouter_id = '{}{}'.format(vpool_name, '%x' % uuid.getnode())
        connection_host, connection_port, connection_username, connection_password = None, None, None, None
        backend_config = {}
        if volumedriver_backend_type == 'LOCAL':
            volumedriver_local_filesystem = Console.askChoice(filesystems, 'Select mountpoint for local backend')
            backend_config = {'local_connection_path': volumedriver_local_filesystem}
        elif volumedriver_backend_type == 'REST':
            connection_host = Console.askString('Provide REST ip address')
            connection_port = Console.askInteger('Provide REST connection port')
            rest_connection_timeout_secs = Console.askInteger('Provide desired REST connection timeout(secs)')
            backend_config = {'rest_connection_host': connection_host,
                              'rest_connection_port': connection_port,
                              'buchla_connection_log_level': "0",
                              'rest_connection_verbose_logging': rest_connection_timeout_secs,
                              'rest_connection_metadata_format': "JSON"}
        elif volumedriver_backend_type == 'S3':
            connection_host = Console.askString('Specify fqdn or ip of your s3 host')
            connection_username = Console.askString('Specify S3 access key')
            connection_password = Console.askString('Specify S3 secret key')
            backend_config = {'s3_connection_host': connection_host,
                              's3_connection_username': connection_username,
                              's3_connection_password': connection_password,
                              's3_connection_verbose_logging': 1}
        backend_config.update({'backend_type': volumedriver_backend_type})
        vsr_configuration = VolumeStorageRouterConfiguration(vpool_name)
        vsr_configuration.configure_backend(backend_config)

        readcaches = [{'path': readcache, 'size': readcache_size}, ]
        vsr_configuration.configure_readcache(readcaches, Configuration.get('volumedriver.readcache.serialization.path'))

        scocaches = [{'path': scocache, 'size': scocache_size}, ]
        vsr_configuration.configure_scocache(scocaches, "1GB", "2GB")

        vsr_configuration.configure_failovercache(failovercache)

        filesystem_config = {'fs_backend_path': Configuration.get('volumedriver.filesystem.distributed')}
        vsr_configuration.configure_filesystem(filesystem_config)

        volumemanager_config = {'metadata_path': metadatapath, 'tlog_path': tlogpath}
        vsr_configuration.configure_volumemanager(volumemanager_config)

        vpools = VPoolList.get_vpool_by_name(vpool_name)
        this_vpool = VPool()
        if vpools and len(vpools) == 1:
            this_vpool = vpools[0]
        this_vpool.name = vpool_name
        this_vpool.description = "{} {}".format(volumedriver_backend_type, vpool_name)
        this_vpool.backend_type = volumedriver_backend_type
        this_vpool.backend_connection = '{}:{}'.format(connection_host, connection_port) if connection_port else connection_host
        this_vpool.backend_login = connection_username
        this_vpool.backend_password = connection_password
        this_vpool.save()
        vrouters = filter(lambda v: v.vsrid == vrouter_id, this_vpool.vsrs)

        if vrouters:
            vrouter = vrouters[0]
        else:
            vrouter = VolumeStorageRouter()
        # Make sure port is not already used
        from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
        ports_used_in_model = [vsr.port for vsr in VolumeStorageRouterList.get_volumestoragerouters()]
        vrouter_port_in_hrd = int(Configuration.get('volumedriver.filesystem.xmlrpc.port'))
        if vrouter_port_in_hrd in ports_used_in_model:
            vrouter_port = Console.askInteger('Provide Volumedriver connection port (make sure port is not in use)', max(ports_used_in_model) + 3)
        else:
            vrouter_port = vrouter_port_in_hrd  # Default
        this_vmachine = VMachine(vmachineguid)
        vrouter.name = vrouter_id.replace('_', ' ')
        vrouter.description = vrouter.name
        vrouter.vsrid = vrouter_id
        vrouter.ip = Configuration.get('ovs.grid.ip')
        vrouter.port = vrouter_port
        vrouter.mountpoint = os.path.join(os.sep, 'mnt', vpool_name)
        vrouter.serving_vmachine = this_vmachine
        vrouter.vpool = this_vpool
        vrouter.save()
        dirs2create.append(vrouter.mountpoint)

        vrouter_config = {"vrouter_id": vrouter_id,
                          "vrouter_redirect_timeout_ms": "5000",
                          "vrouter_migrate_timeout_ms" : "5000",
                          "vrouter_write_threshold" : 1024,
                          "host": vrouter.ip,
                          "xmlrpc_port": vrouter.port}
        vsr_configuration.configure_volumerouter(vpool_name, vrouter_config)

        voldrv_arakoon_cluster_id = Configuration.get('volumedriver.arakoon.clusterid')
        voldrv_arakoon_cluster = ArakoonManagement().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        vsr_configuration.configure_arakoon_cluster(voldrv_arakoon_cluster_id, voldrv_arakoon_client_config)

        queue_config = {"events_amqp_routing_key": Configuration.get('ovs.core.broker.volumerouter.queue'),
                        "events_amqp_uri": "{}://{}:{}@{}:{}".format(Configuration.get('ovs.core.broker.protocol'),
                                                                     Configuration.get('ovs.core.broker.login'),
                                                                     Configuration.get('ovs.core.broker.password'),
                                                                     Configuration.get('ovs.grid.ip'),
                                                                     Configuration.get('ovs.core.broker.port'))}
        vsr_configuration.configure_event_publisher(queue_config)

        for directory in dirs2create:
            if not os.path.exists(directory):
                os.makedirs(directory)
        for filename in files2create:
            if not os.path.exists(filename):
                open(filename, 'a').close()

        config_file = os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json'.format(vpool_name))
        log_file = os.path.join(os.sep, 'var', 'log', '{}.log'.format(vpool_name))
        cmd = '/usr/bin/volumedriver_fs -f --config-file={} --mountpoint {} --logfile {} -o big_writes -o uid=0 -o gid=0 -o sync_read'.format(config_file, vrouter.mountpoint, log_file)
        stopcmd = 'exportfs -u *:{0}; umount {0}'.format(vrouter.mountpoint)
        name = 'volumedriver_{}'.format(vpool_name)
        Service.add_service(package=('openvstorage', 'volumedriver'), name=name, command=cmd, stop_command=stopcmd)


class Control():
    """
    OVS Control class enabling you to
    * init
    * start
    * stop
    all components at once
    """

    def __init__(self):
        """
        Init class
        """
        pass

    def init(self, vpool_name):
        """
        Configure & Start the OVS components in the correct order to get your environment initialized after install
        * Reset rabbitmq
        * Remove nginx file /etc/nginx/sites-enabled/default configuration
        * Load default data into model
        * Configure volume storage router
        """
        while not re.match('^[0-9a-zA-Z]+([\-_]+[0-9a-zA-Z]+)*$', vpool_name):
            print 'Invalid vPool name given. Only 0-9, a-z, A-Z, _ and - are allowed.'
            suggestion = re.sub(
                '^([\-_]*)(?P<correct>[0-9a-zA-Z]+([\-_]+[0-9a-zA-Z]+)*)([\-_]*)$',
                '\g<correct>',
                re.sub('[^0-9a-zA-Z\-_]', '_', vpool_name)
            )
            vpool_name = Console.askString('Provide new vPool name', defaultparam=suggestion)

        if not self._package_is_running('openvstorage-core'):
            arakoon_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'arakoon')
            arakoon_clusters = map(lambda d: os.path.basename(d), os.walk(arakoon_dir).next()[1])
            for cluster in arakoon_clusters:
                cluster_instance = ArakoonManagement().getCluster(cluster)
                cluster_instance.createDirs(cluster_instance.listLocalNodes()[0])
            Configure.init_rabbitmq()
            self._start_package('openvstorage-core')
        if not self._package_is_running('openvstorage-webapps'):
            Configure.init_nginx()
            self._start_package('openvstorage-webapps')
        vmachineguid = Configure.load_data()
        Configure.init_storagerouter(vmachineguid, vpool_name)
        if not self._package_is_running('volumedriver'):
            self._start_package('volumedriver')
        vfs_info = os.statvfs('/mnt/{}'.format(vpool_name))
        vpool_size_bytes = vfs_info.f_blocks * vfs_info.f_bsize
        vpools = VPoolList.get_vpool_by_name(vpool_name)
        if len(vpools) != 1:
            raise ValueError('No or multiple vpools found with name {}, should not happen at this stage, please check your configuration'.format(vpool_name))
        this_vpool = vpools[0]
        this_vpool.size = vpool_size_bytes
        this_vpool.save()
        Configure.init_exportfs(vpool_name)
        install = Console.askYesNo('Do you want to mount the vPool?')
        if install is True:
            print '  Please wait while the vPool is mounted...'
            vmachine = VMachine(vmachineguid)
            vrouter = [vsr for vsr in this_vpool.vsrs if vsr.serving_vmachine_guid == vmachineguid][0]
            hypervisor = Factory.get(vmachine.pmachine)
            try:
                hypervisor.mount_nfs_datastore(vpool_name, vrouter.ip, vrouter.mountpoint)
                print '    Success'
            except Exception as ex:
                print '    Error, please mount the vPool manually. {0}'.format(str(ex))
        subprocess.call(['service', 'processmanager', 'start'])

    def _package_is_running(self, package):
        """
        Checks whether a package is running
        """
        _ = self
        return Package.is_running(namespace='openvstorage', name=package)

    def _start_package(self, package):
        """
        Starts a package
        """
        _ = self
        return Package.start(namespace='openvstorage', name=package)

    def _stop_package(self, package):
        """
        Stops a package
        """
        _ = self
        return Package.stop(namespace='openvstorage', name=package)

    def start(self):
        """
        Starts all packages
        """
        self._start_package('volumedriver')
        self._start_package('openvstorage-core')
        self._start_package('openvstorage-webapps')
        subprocess.call(['service', 'nfs-kernel-server', 'start'])

    def stop(self):
        """
        Stops all packages
        """
        subprocess.call(['service', 'nfs-kernel-server', 'stop'])
        self._stop_package('openvstorage-webapps')
        self._stop_package('openvstorage-core')
        self._stop_package('volumedriver')

    def status(self):
        """
        Gets the status from all packages
        """
        _ = self
        subprocess.call(['service', 'nfs-kernel-server', 'status'])
        Package.get_status(namespace='openvstorage', name='openvstorage-core')
        Package.get_status(namespace='openvstorage', name='openvstorage-webapps')

# license see http://www.openvstorage.com/licenses/opensource/
import subprocess
import uuid
import os
import re
from JumpScale import j
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vmachine import PMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterConfiguration


class Configure():
    def init_exportfs(self, vpool_name):
        
        # Configure nfs
        from ovs.extensions.fs.exportfs import Nfsexports
        vpool_mountpoint = j.system.fs.joinPaths(os.sep, 'mnt', vpool_name)
        if not j.system.fs.exists(vpool_mountpoint):
            j.system.fs.createDir(vpool_mountpoint)
        Nfsexports().add(vpool_mountpoint, '*', 'rw,fsid={0},sync,no_root_squash,no_subtree_check'.format(uuid.uuid4()))
        subprocess.call(['service', 'nfs-kernel-server', 'start'])

    def loadData(self):
        """
        Load default data set
        """
        # Select/Create system vmachine
        hostname = j.system.net.getHostname()
        vmachine_selector = VMachineList()
        vmachine_list = vmachine_selector.get_vmachine_by_name(hostname)
        if vmachine_list and len(vmachine_list) == 1:
            print "System vMachine already created, updating ..."
            vmachine = vmachine_list[0]
        elif not vmachine_list or len(vmachine_list) == 0:
            print "Creating System vMachine"
            vmachine = VMachine()
        else:
            raise ValueError('Multiple System vMachines with name %s found, check your model'%hostname)
    
        # Select/Create host hypervisor node
        pmachine_selector = PMachineList()
        #@todo implement more accurate search on PMachinelist to find pmachine
        pmachine_list = pmachine_selector.get_pmachines()
        found_pmachine = False
        if pmachine_list:
            for pmachine in pmachine_list:
                if pmachine.ip == j.application.config.get('ovs.host.ip'):
                    found_pmachine = True
                    break
        if not found_pmachine:
            pmachine = PMachine()
    
        # Model system VMachine and Hypervisor node
        pmachine.ip = j.application.config.get('ovs.host.ip')
        pmachine.username = j.application.config.get('ovs.host.login')
        pmachine.password = j.application.config.get('ovs.host.password')
        pmachine.hvtype = j.application.config.get('ovs.host.hypervisor')
        vmachine.name = hostname
        vmachine.hvtype = j.application.config.get('ovs.host.hypervisor')
        vmachine.is_vtemplate = False
        vmachine.is_internal = True
        vmachine.ip = j.application.config.get('ovs.grid.ip')
        vmachine.pmachine = pmachine
        pmachine.save()
        vmachine.save()
        
        from ovs.extensions.migration.migration import Migration
        Migration.migrate()
        
        return vmachine.guid

    def init_rabbitmq(self):
        """
        Reconfigure rabbitmq to work with ovs user.
        """
        os.system('rabbitmq-server -detached; rabbitmqctl stop_app; rabbitmqctl reset; rabbitmqctl stop;')

    def init_nginx(self):
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
        if j.system.fs.exists('/etc/nginx/sites-enabled/default'):
            j.system.fs.remove('/etc/nginx/sites-enabled/default')

    def init_storagerouter(self, vmachineguid, vpool_name):
        """
        Initializes the volume storage router.
        This requires a the OVS model to be configured and reachable
        @param vmachineguid: guid of the internal VSA machine hosting this volume storage router
        """
        hrd = j.application.config
        mountpoints = [hrd.get('volumedriver.metadata'),]
        for path in mountpoints:
            if not j.system.fs.exists(path) or not j.system.fs.isMount(path):
                raise ValueError('Path to %s does not exist or is not a mountpoint'%path)
        all_mounts = map(lambda m: m.split()[2], j.system.process.execute('mount -v', dieOnNonZeroExitCode=False)[1].splitlines())
        mount_regex = re.compile('^/$|/dev|/sys|/run|/proc|{}|{}|{}'.format(hrd.get('ovs.core.db.mountpoint'),
                                                                            hrd.get('volumedriver.filesystem.distributed'),
                                                                            hrd.get('volumedriver.metadata')))
        filesystems = filter(lambda d: not mount_regex.match(d), all_mounts)
        volumedriver_cache_mountpoint = j.console.askChoice(filesystems, 'Select cache mountpoint')
        filesystems.remove(volumedriver_cache_mountpoint)
        cache_fs = os.statvfs(volumedriver_cache_mountpoint)
        scocache = "{}/sco".format(volumedriver_cache_mountpoint)
        readcache = "{}/read".format(volumedriver_cache_mountpoint)
        failovercache = "{}/foc".format(volumedriver_cache_mountpoint)
        metadatapath = "{}/metadata".format(hrd.get('volumedriver.metadata'))
        tlogpath = "{}/tlogs".format(hrd.get('volumedriver.metadata'))
        dirs2create = [scocache,
                       failovercache,
                       hrd.get('volumedriver.readcache.serialization.path'),
                       hrd.get('volumedriver.filesystem.cache'),
                       metadatapath,
                       tlogpath]
        files2create = [readcache]
        # Cache sizes
        # 20% = scocache
        # 20% = failovercache (@todo: check if this can possibly consume more then 20%)
        # 60% = readcache
        scocache_size = "{0}KiB".format((int(cache_fs.f_bavail * 0.2 / 4096 )* 4096 ) * 4)
        readcache_size = "{0}KiB".format((int(cache_fs.f_bavail * 0.6 / 4096 )* 4096) * 4)
        volumedriver_backend_type = j.console.askChoice(hrd.get('volumedriver.supported.backends').split(','), 'Select type of storage backend')
        vrouter_id = '{}{}'.format(vpool_name, j.application.getUniqueMachineId())
        connection_host, connection_port, connection_username, connection_password = None, None, None, None
        if volumedriver_backend_type == 'LOCAL':
            volumedriver_local_filesystem = j.console.askChoice(filesystems, 'Select mountpoint for local backend')
            backend_config = {'local_connection_path': volumedriver_local_filesystem}
        elif volumedriver_backend_type == 'REST':
            connection_host = j.console.askString('Provide REST ip address')
            connection_port = j.console.askInteger('Provide REST connection port')
            rest_connection_timeout_secs = j.console.askInteger('Provide desired REST connection timeout(secs)')
            backend_config = {'rest_connection_host': connection_host,
                              'rest_connection_port': connection_port,
                              'buchla_connection_log_level': "0",
                              'rest_connection_verbose_logging': rest_connection_timeout_secs,
                              'rest_connection_metadata_format': "JSON"}
        elif volumedriver_backend_type == 'S3':
            connection_host = j.console.askString('Specify fqdn or ip of your s3 host')
            connection_username = j.console.askString('Specify S3 username')
            connection_password = j.console.askString('Specify S3 password')
            backend_config = {'s3_connection_host': connection_host,
                              's3_connection_username': connection_username,
                              's3_connection_password': connection_password,
                              's3_connection_verbose_logging': 1}
        backend_config.update({'backend_type': volumedriver_backend_type})
        vsr_configuration = VolumeStorageRouterConfiguration(vpool_name)
        vsr_configuration.configure_backend(backend_config)

        readcaches = [{'path': readcache, 'size': readcache_size},]
        vsr_configuration.configure_readcache(readcaches, hrd.get('volumedriver.readcache.serialization.path'))
        
        scocaches = [{'path': scocache, 'size': scocache_size},]
        vsr_configuration.configure_scocache(scocaches, "1GB", "2GB")
        
        vsr_configuration.configure_failovercache(failovercache)
        
        filesystem_config = {'fs_cache_path': hrd.get('volumedriver.filesystem.cache'),
                             'fs_backend_path': hrd.get('volumedriver.filesystem.distributed'),
                             'fs_volume_regex': hrd.get('volumedriver.filesystem.regex'),
                             'fs_xmlrpc_port': hrd.get('volumedriver.filesystem.xmlrpc.port')}
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
        this_vpool.backend_connection = '{}:{}'.format(connection_host,connection_port) if connection_port else connection_host
        this_vpool.backend_login = connection_username
        this_vpool.backend_password = connection_password
        this_vpool.save()
        vrouters = filter(lambda v: v.vsrid == vrouter_id, this_vpool.vsrs)
        
        if vrouters:
            vrouter = vrouters[0]
        else:
            vrouter = VolumeStorageRouter()
        this_vmachine = VMachine(vmachineguid)
        vrouter.name = vrouter_id.replace('_', ' ')
        vrouter.description = vrouter.name
        vrouter.vsrid = vrouter_id
        vrouter.ip = hrd.get('ovs.grid.ip')
        vrouter.port = int(hrd.get('volumedriver.filesystem.xmlrpc.port'))
        vrouter.mountpoint = j.system.fs.joinPaths(os.sep, 'mnt', vpool_name)
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

        voldrv_arakoon_cluster_id = hrd.get('volumedriver.arakoon.clusterid')
        voldrv_arakoon_cluster = ArakoonManagement().getCluster(voldrv_arakoon_cluster_id)
        voldrv_arakoon_client_config = voldrv_arakoon_cluster.getClientConfig()
        vsr_configuration.configure_arakoon_cluster(voldrv_arakoon_cluster_id, voldrv_arakoon_client_config)

        queue_config = {"events_amqp_routing_key": hrd.get('ovs.core.broker.volumerouter.queue'),
                        "events_amqp_uri": "{}://{}:{}@{}:{}".format(hrd.get('ovs.core.broker.protocol'),
                                                                     hrd.get('ovs.core.broker.login'),
                                                                     hrd.get('ovs.core.broker.password'),
                                                                     hrd.get('ovs.grid.ip'),
                                                                     hrd.get('ovs.core.broker.port'))}
        vsr_configuration.configure_event_publisher(queue_config)

        for dir in dirs2create:
            if not j.system.fs.exists(dir):
                j.system.fs.createDir(dir)
        for file in files2create:
            if not j.system.fs.exists(file):
                j.system.fs.createEmptyFile(file)

        #start volumedriver for vpool
        voldrv_package = j.packages.find(domain='openvstorage', name='volumedriver')[0]
        config_file = j.system.fs.joinPaths(hrd.get('ovs.core.cfgdir'), '{}.json'.format(vpool_name))
        cmd = '/usr/bin/volumedriver_fs -f --config-file={} --mountpoint {} -o big_writes -o uid=0 -o gid=0 -o sync_read'.format(config_file, vrouter.mountpoint)
        stopcmd = 'exportfs -u *:{0}; umount {0}'.format(vrouter.mountpoint)
        args = ''
        workingdir = ""
        name = 'volumedriver_{}'.format(vpool_name)
        domain = voldrv_package.domain
        ports = []
        j.tools.startupmanager.addProcess(name=name, cmd=cmd, args=args, env={}, numprocesses=1, priority=21, \
           shell=False, workingdir=workingdir,jpackage=voldrv_package,domain=domain,ports=ports,stopcmd=stopcmd)

class Control():
    """
    OVS Control class enabling you to
    * init
    * start
    * stop
    all components at once
    """
    def init(self, vpool_name):
        """
        Configure & Start the OVS components in the correct order to get your environment initialized after install
        * Reset rabbitmq
        * Remove nginx file /etc/nginx/sites-enabled/default configuration
        * Load default data into model
        * Configure volume storage router
        """
        ovsConfigure = Configure()
        if not self._packageIsRunning('openvstorage-core'):
            arakoon_dir = j.system.fs.joinPaths(j.application.config.get('ovs.core.cfgdir'), 'arakoon')
            arakoon_clusters = map(lambda d: j.system.fs.getBaseName(d), j.system.fs.listDirsInDir(arakoon_dir))
            for cluster in arakoon_clusters:
                cluster_instance = ArakoonManagement().getCluster(cluster)
                cluster_instance.createDirs(cluster_instance.listLocalNodes()[0])
            ovsConfigure.init_rabbitmq()
            self._startPackage('openvstorage-core')
        if not self._packageIsRunning('openvstorage-webapps'):
            ovsConfigure.init_nginx()
            self._startPackage('openvstorage-webapps')
        vmachineguid = ovsConfigure.loadData()
        if not self._packageIsRunning('volumedriver'):
            ovsConfigure.init_storagerouter(vmachineguid, vpool_name)
            self._startPackage('volumedriver')
        vfs_info = os.statvfs('/mnt/{}'.format(vpool_name))
        vpool_size_bytes = vfs_info.f_blocks * vfs_info.f_bsize
        vpools = VPoolList.get_vpool_by_name(vpool_name)
        if len(vpools) != 1:
            raise ValueError('No or multiple vpools found with name {}, should not happen at this stage, please check your configuration'.format(vpool_name))
        this_vpool = vpools[0]
        this_vpool.size = vpool_size_bytes
        this_vpool.save()
        ovsConfigure.init_exportfs(vpool_name)

    def _packageIsRunning(self, package):
        package = j.packages.find(domain='openvstorage', name=package)[0]
        return j.tools.startupmanager.getStatus4JPackage(package)

    def _startPackage(self, package):
        package = j.packages.find(domain='openvstorage', name=package)[0]
        j.tools.startupmanager.startJPackage(package)

    def _stopPackage(self, package):
        package = j.packages.find(domain='openvstorage', name=package)[0]
        j.tools.startupmanager.stopJPackage(package)

    def start(self):
        """
        Start following services
        * rabbitmq-server
        * memcached
        * ovscore:ovsdb
        * ovscore:ovsworkers
        * ovscore:ovsflower
        * ovscore:ovsvolmgr
        * ovswebapps:ovsapi
        * nginx
        * nfs-kernel-server
        """
        self._startPackage('volumedriver')
        self._startPackage('openvstorage-core')
        self._startPackage('openvstorage-webapps')
        subprocess.call(['service', 'nfs-kernel-server', 'start'])
    
    def stop(self):
        """
        Start following services
        * nfs-kernel-server
        * nginx
        * ovswebapps:ovsapi
        * ovscore:ovsvolmgr
        * ovscore:ovsflower
        * ovscore:ovsworkers
        * ovscore:ovsdb
        * memcached
        * rabbitmq-server
        """
        subprocess.call(['service', 'nfs-kernel-server', 'stop'])
        self._stopPackage('openvstorage-webapps')
        self._stopPackage('openvstorage-core')
        self._stopPackage('volumedriver')

    def status(self):
        """
        Get status for following services
        * nfs-kernel-server
        * nginx
        * ovswebapps:ovsapi
        * ovscore:ovsvolmgr
        * ovscore:ovsflower
        * ovscore:ovsworkers
        * ovscore:ovsdb
        * memcached
        * rabbitmq-server
        """
        subprocess.call(['service', 'nfs-kernel-server', 'status'])
        core_package = j.packages.find(domain='openvstorage', name='openvstorage-core')[0]
        j.tools.startupmanager.getStatus4JPackage(core_package)
        webapps_package = j.packages.find(domain='openvstorage', name='openvstorage-webapps')[0]
        j.tools.startupmanager.getStatus4JPackage(webapps_package)


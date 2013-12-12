# license see http://www.openvstorage.com/licenses/opensource/
from configobj import ConfigObj
from validate import Validator
import json
import subprocess
import os, uuid

configDir = '/opt/OpenvStorage/config'
configSpecDir = '{0}/specs'.format(configDir)

class Configure():
    """
    OVS configure class enabling devs/engs to get easily bootstrapped with a single node setup
    It allows you to configure the required components with default settings based on config-spec files
    """
    def _getConfig(self, file):
        config = ConfigObj(infile = '{0}/{1}'.format(configDir,file),
                           configspec = '{0}/{1}'.format(configSpecDir,file))
        return config

    def _validate(self, config, copy, write):
        validator = Validator()
        valid = config.validate(validator, copy=copy)
        if valid == True and write:
            config.write()
        return valid, config
    
    def arakoon(self):
        """
        Configure Arakoon
        """
        ovsCoreSupervisorConfig = ConfigObj('/etc/supervisor/conf.d/ovscore.conf')
        for cluster in os.listdir('{0}/arakoon'.format(configSpecDir)):
            for file in os.listdir('{0}/arakoon/{1}'.format(configSpecDir, cluster)):
                print file
                conffile = 'arakoon/{0}/{1}'.format(cluster,file)
                config = self._getConfig(conffile)
                absConfigDir = '{0}/{1}'.format(configDir, os.path.dirname(conffile))
                if not os.path.exists(absConfigDir):
                    subprocess.call(['mkdir', '-p', absConfigDir])
                valid, config = self._validate(config, copy=True, write=True)
            from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
            cl = ArakoonManagement().getCluster(cluster)
            for localnode in cl.listLocalNodes():
                cl.createDirs(localnode)
                programName = '{0}_{1}'.format(cluster,localnode)
                if not ovsCoreSupervisorConfig.has_key('program:{0}'.format(programName)):
                   cmd = 'arakoon --node {0} -config /opt/OpenvStorage/config/arakoon/{1}/{1}.cfg -start'.format(localnode, cluster)
                   ovsCoreSupervisorConfig.update({'program:{0}'.format(programName): {'command': cmd, 'autostart': 'true', 'autorestart': 'true'}})
            if not ovsCoreSupervisorConfig.has_key('group:ovscore'):
               ovsCoreSupervisorConfig.update({'group:ovscore': {'programs': [programName,]}})
            elif not programName in ovsCoreSupervisorConfig['group:ovscore']['programs']:
               ovsCoreSupervisorConfig['group:ovscore']['programs'].insert(0, programName)
            ovsCoreSupervisorConfig.write()

    def volumedriver(self, cache='/mnt/cache', dfspath='/mnt/dfs', backendfs='/mnt/bfs', rspath='/var/rsp', fscpath='/var/fsc', mdpath='/mnt/md', ):
        """
        Configure NFS Volumedriver
        1. Create JSON file for volumedriver
        2. Create volumeStorageRouterClient config file
        """
        """
        @param cache: path to the cache mountpoint to host "read" & "write" cache
        @param backendfs: path to volumedriver backend filesystem
        @param rspath: read cache serialization path
        @param fscpath: path to filesystem cache
        @param mdpath: path to the metadata directory to store "metadata" & "tlogs"
        """
        for path in [cache, backendfs, mdpath]:
            if not os.path.exists(path) or not os.path.ismount(path):
                raise ValueError('Path to %s does not exist or is not a mountpoint'%path)
        scocache = "{0}/sco".format(cache)
        readcache = "{0}/read".format(cache)
        metadatapath = "{0}/metadata".format(mdpath)
        tlogpath = "{0}/tlogs".format(mdpath)
        dirs2create = [scocache, backendfs, rspath, fscpath, metadatapath, tlogpath ]
        files2create = [readcache]
        cache_fs = os.statvfs(cache)
        scocache_size = "{0}KiB".format((int(cache_fs.f_bavail * 0.4 / 4096 )* 4096 ) * 4)
        readcache_size = "{0}KiB".format((int(cache_fs.f_bavail * 0.6 / 4096 )* 4096) * 4)
    
        vsrConfigFile = 'volumestoragerouterclient.cfg'
        config = self._getConfig(vsrConfigFile)
        valid, config = self._validate(config, copy=True, write=True)
        print "Config %s validation %s"%(config,valid)
    
        voldrvSpecFile = open('{0}/volumefs.json'.format(configSpecDir), 'r')
        voldrvConfigFile = open('/opt/OpenvStorage/config/volumefs.json', 'w')
        j = json.load(voldrvSpecFile)
        j['backend_connection_manager']['local_connection_path'] = backendfs
        j['content_addressed_cache']['read_cache_serialization_path'] = rspath
        j['filesystem']['fs_cache_path'] = fscpath
        j['filesystem']['fs_backend_path'] = dfspath
        j['volume_manager']['metadata_path'] = metadatapath
        j['volume_manager']['tlog_path'] = tlogpath
        j['content_addressed_cache']['clustercache_mount_points'][0]['path'] = readcache
        j['content_addressed_cache']['clustercache_mount_points'][0]['size'] = readcache_size
        j['scocache']['scocache_mount_points'][0]['path'] = scocache
        j['scocache']['scocache_mount_points'][0]['size'] = scocache_size
        json.dump(j, voldrvConfigFile, indent=2)
        voldrvSpecFile.close()
        voldrvConfigFile.close()
        for dir in dirs2create:
            if not os.path.exists(dir):
                subprocess.call(['mkdir', '-p', dir])
        for file in files2create:
            if not os.path.exists(file):
                subprocess.call(['touch', file])

        # Add the exposed filesystem to exports
        from ovs.extensions.fs.exportfs import Nfsexports
        volfsdir = '/srv/volumefs'
        subprocess.call(['mkdir', '-p', volfsdir])
        Nfsexports().add(volfsdir, '*', 'rw,fsid={0},sync,no_root_squash,no_subtree_check'.format(uuid.uuid4()))
    
    """
    Configure RabbitMQ
    """
    
    """
    Configure Celery
    """
    

    def django(self):
        """
        Configure Django
        """
        subprocess.call(['mkdir', '-p', '/var/log/OpenvStorage/api'])
        cwd = os.path.abspath(os.path.curdir)
        subprocess.call(['mkdir', '-p', '/var/log/api'])
        os.chdir('/opt/OpenvStorage/webapps/api')
        subprocess.call(['python', 'manage.py', 'syncdb', '--noinput'])
        os.chdir(cwd)
        ## Create SSL certificates
        valid_nr_days = 365 * 5
        baseName = '/opt/OpenvStorage/config/ssl/server'
        key = '{0}.key'.format(baseName)
        csr = '{0}.csr'.format(baseName)
        crt = '{0}.crt'.format(baseName)
        passphrase = '/opt/OpenvStorage/config/ssl/passphrase'
        # Generate system unique passphrase
        ssldir = os.path.dirname(passphrase)
        if not os.path.exists(ssldir):
            subprocess.call(['mkdir', '-p', ssldir])
        import uuid
        fh = open(passphrase, 'w')
        fh.write(str(uuid.getnode()))
        fh.close()
        # Create server key
        subprocess.call(['openssl', 'genrsa', '-des3', '-out', key, '-passout', 'file:{0}'.format(passphrase)])
        # Create signing request
        subprocess.call(['openssl', 'req', '-new', '-key', key, '-out', csr, '-passin', 'file:{0}'.format(passphrase), '-batch'])
        # Remove passphrase from key
        subprocess.call(['cp', key, '{0}.org'.format(key)])
        subprocess.call(['openssl', 'rsa', '-passin', 'file:{0}'.format(passphrase), '-in', '{0}.org'.format(key), '-out', key])
        # Sign certificate
        subprocess.call(['openssl', 'x509', '-req', '-days', '365', '-in', csr, '-signkey', key, '-out', crt])
        os.unlink('{0}.org'.format(key))

    def memcache(self):
        """
        Configure Memcache
        * Single node bootstrap installs memcache and runs it with default settings
        * Configure OpenvStorage memcache client to connect to default memcache settings
        """
        memcacheConfigFile = 'memcacheclient.cfg'
        config = self._getConfig(memcacheConfigFile)
        valid, config = self._validate(config, copy=True, write=True)
        print "Config %s validation %s"%(config,valid)

    def dbStorage(self):
        """
        Config OpenvStorage volatile and persistent databases
        """
        dbstorageConfigFile = 'storage.cfg'
        config = self._getConfig(dbstorageConfigFile)
        valid, config = self._validate(config, copy=True, write=True)
        print "Config %s validation %s"%(config,valid)

    def loadData(self):
        """
        Load default data set
        """
        from ovs.extensions.migration.migration import Migration
        Migration.migrate()

class Control():
    """
    OVS Control class enabling you to start/stop all components at once
    Single service restart needs to be done using the linux service or supervisor tool set
    """
    def init(self):
        """
        Configure & Start the OVS components in the correct order to get your environment initialized after install
        """
        ovsConfigure = Configure()
        ovsConfigure.arakoon()
        ovsConfigure.volumedriver()
        ovsConfigure.memcache()
        ovsConfigure.dbStorage()
        ovsConfigure.django()
        self.start()
        ovsConfigure.loadData()

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
        subprocess.call(['service', 'rabbitmq-server', 'start'])
        subprocess.call(['service', 'memcached', 'start'])
        exitCode = subprocess.call(['supervisorctl', 'pid'])
        if exitCode == 0:
            subprocess.call(['supervisorctl', 'update'])
            subprocess.call(['supervisorctl', 'start', 'all'])
        else:
            subprocess.call(['service', 'supervisord', 'start'])
        subprocess.call(['service', 'nginx', 'start'])
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
        subprocess.call(['service', 'nginx', 'stop'])
        subprocess.call(['supervisorctl', 'stop', 'all'])
        subprocess.call(['service', 'memcached', 'stop'])
        subprocess.call(['service', 'rabbitmq-server', 'stop'])

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
        subprocess.call(['service', 'nginx', 'status'])
        subprocess.call(['supervisorctl', 'status'])
        subprocess.call(['service', 'memcached', 'status'])
        subprocess.call(['service', 'rabbitmq-server', 'status'])

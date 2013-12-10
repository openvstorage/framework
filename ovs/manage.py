# license see http://www.openvstorage.com/licenses/opensource/
import subprocess
import uuid
from JumpScale import j
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vmachine import PMachine 
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement


class Configure():
    def init(self):
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
        vmachine.pmachine = pmachine
        pmachine.save()
        vmachine.save()
    
        # Configure nfs
        from ovs.extensions.fs.exportfs import Nfsexports
        if not j.system.fs.exists(j.application.config.get('volumedriver.filesystem.mountpoint')):
            j.system.fs.createDir(j.application.config.get('volumedriver.filesystem.mountpoint'))
        Nfsexports().add(j.application.config.get('volumedriver.filesystem.mountpoint'), '*', 'rw,fsid={0},sync,no_root_squash,no_subtree_check'.format(uuid.uuid4()))

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
        arakoon_dir = j.system.fs.joinPaths(j.application.config.get('ovs.core.cfgdir'), 'arakoon')
        arakoon_clusters = map(lambda d: j.system.fs.getBaseName(d), j.system.fs.listDirsInDir(arakoon_dir))
        for cluster in arakoon_clusters:
            cluster_instance = ArakoonManagement().getCluster(cluster)
            cluster_instance.createDirs(cluster_instance.listLocalNodes()[0])
        self.start()
        ovsConfigure.loadData()
        ovsConfigure.init()


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
        core_package = j.packages.find(domain='openvstorage', name='openvstorage-core')[0]
        j.tools.startupmanager.startJPackage(core_package)
        webapps_package = j.packages.find(domain='openvstorage', name='openvstorage-webapps')[0]
        j.tools.startupmanager.startJPackage(webapps_package)
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
        core_package = j.packages.find(domain='openvstorage', name='openvstorage-core')[0]
        j.tools.startupmanager.startJPackage(core_package)
        webapps_package = j.packages.find(domain='openvstorage', name='openvstorage-webapps')[0]
        j.tools.startupmanager.startJPackage(webapps_package)

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


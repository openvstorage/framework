# license see http://www.openvstorage.com/licenses/opensource/
import subprocess
from JumpScale import j
from ovs.dal.hybrids.vmachine import vMachine
from ovs.dal.hybrids.vmachine import pMachine 
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.pmachinelist import PMachineList


class Configure():
    def init(self):
        # Select/Create system vmachine
        hostname = j.system.net.getHostname()
        vmachine_selector = VMachineList()
        vmachine_list = vmachine_selector.get_vmachine_by_name(hostname)
        if len(vmachine_list) == 1:
            print "System vMachine already created, updating ..."
            vmachine = vmachine_list[0]
        elif len(vmachine_list) == 0:
            print "Creating System vMachine"
            vmachine = vMachine()
        else:
            raise ValueError('Multiple System vMachines with name %s found, check your model'%hostname)
    
        # Select/Create host hypervisor node
        pmachine_selector = PMachineList()
        #@todo implement more accurate search on PMachinelist to find pmachine
        pmachine_list = pmachine_selector.get_pmachines()
        found_pmachine = False
        for pmachine in pmachine_list:
            if pmachine.ip == j.application.config.get('ovs.host.ip'):
                found_pmachine = True
                break
        if not found_pmachine:
            pmachine = pMachine()
    
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
    
        # Connect and retrieve info from hypervisornode

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
        ovsConfigure.init()
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
        core_package = j.packages.find(domain='openvstorage', name='openvstorage-core')[0]
        j.tools.startupmanager.startJPackage(core_package)
        webapps_package = j.packages.find(domain='openvstorage', name='openvstorage-webapps')[0]
        j.tools.startupmanager.startJPackage(webapps_package)
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
        core_package = j.packages.find(domain='openvstorage', name='openvstorage-core')[0]
        j.tools.startupmanager.startJPackage(core_package)
        webapps_package = j.packages.find(domain='openvstorage', name='openvstorage-webapps')[0]
        j.tools.startupmanager.startJPackage(webapps_package)
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
        core_package = j.packages.find(domain='openvstorage', name='openvstorage-core')[0]
        j.tools.startupmanager.getStatus4JPackage(core_package)
        webapps_package = j.packages.find(domain='openvstorage', name='openvstorage-webapps')[0]
        j.tools.startupmanager.getStatus4JPackage(webapps_package)
        subprocess.call(['service', 'memcached', 'status'])
        subprocess.call(['service', 'rabbitmq-server', 'status'])

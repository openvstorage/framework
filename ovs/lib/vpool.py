# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
VPool module
"""

from ovs.celery_run import celery
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.fs.exportfs import Nfsexports
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.hypervisor.factory import Factory
from ovs.lib.vmachine import VMachineController
from ovs.lib.helpers.decorators import log


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """

    @staticmethod
    @celery.task(name='ovs.vpool.up_and_running')
    @log('VOLUMEDRIVER_TASK')
    def up_and_running(mountpoint, storagedriver_id):
        """
        Volumedriver informs us that the service is completely started. Post-start events can be executed
        :param mountpoint: Mountpoint to check
        :param storagedriver_id: ID of the storagedriver
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()
        if storagedriver.storagerouter.pmachine.hvtype == 'VMWARE':
            client = SSHClient(storagedriver.storagerouter)
            machine_id = System.get_my_machine_id(client)
            if EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|vmware_mode'.format(machine_id)) == 'classic':
                nfs = Nfsexports()
                nfs.unexport(mountpoint)
                nfs.export(mountpoint)
                nfs.trigger_rpc_mountd()

    @staticmethod
    @celery.task(name='ovs.vpool.sync_with_hypervisor')
    def sync_with_hypervisor(vpool_guid):
        """
        Syncs all vMachines of a given vPool with the hypervisor
        :param vpool_guid: Guid of the vPool to synchronize
        """
        vpool = VPool(vpool_guid)
        if vpool.status != VPool.STATUSES.RUNNING:
            raise ValueError('Synchronizing with hypervisor is only allowed if your vPool is in {0} status'.format(VPool.STATUSES.RUNNING))
        for storagedriver in vpool.storagedrivers:
            pmachine = storagedriver.storagerouter.pmachine
            hypervisor = Factory.get(pmachine)
            for vm_object in hypervisor.get_vms_by_nfs_mountinfo(storagedriver.storage_ip, storagedriver.mountpoint):
                search_vpool = None if pmachine.hvtype == 'KVM' else vpool
                vmachine = VMachineList.get_by_devicename_and_vpool(devicename=vm_object['backing']['filename'],
                                                                    vpool=search_vpool)
                VMachineController.update_vmachine_config(vmachine, vm_object, pmachine)

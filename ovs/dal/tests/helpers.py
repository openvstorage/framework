# Copyright (C) 2017 iNuron NV
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
DalHelper module
"""
import os
import copy
import glob
import json
import shutil
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs_extensions.generic import fakesleep
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs_extensions.generic.tests.sshclient_mock import MockedSSHClient
from ovs_extensions.packages.packagefactory import PackageFactory
from ovs_extensions.services.mockups.systemd import SystemdMock
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.tests.mockups import LocalStorageRouterClient, MDSClient, StorageRouterClient
from ovs.lib.helpers.decorators import Decorators
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.storagedriver import StorageDriverController


class DalHelper(object):
    """
    This class contains functionality used by all UnitTests related to the DAL
    """
    CLUSTER_DIR = '/tmp/unittest-{0}'
    UNITTEST_DIR = '/tmp/unittest'

    @staticmethod
    def setup(**kwargs):
        """
        Execute several actions before starting a new UnitTest
        :param kwargs: Additional key word arguments
        """
        if kwargs.get('fake_sleep', False) is True:
            fakesleep.monkey_patch()
        volatile, persistent = DalHelper._clean()
        Configuration.initialize()
        return volatile, persistent

    @staticmethod
    def teardown(**kwargs):
        """
        Execute several actions when ending a UnitTest
        :param kwargs: Additional key word arguments
        """
        if kwargs.get('fake_sleep', False) is True:
            fakesleep.monkey_restore()
        DalHelper._clean()

    @staticmethod
    def _clean():
        volatile = VolatileFactory.get_client()
        persistent = PersistentFactory.get_client()

        # noinspection PyProtectedMember
        volatile._clean()
        # noinspection PyProtectedMember
        persistent._clean()
        # noinspection PyProtectedMember
        SSHClient._clean()
        # noinspection PyProtectedMember
        SystemdMock._clean()
        # noinspection PyProtectedMember
        MDSClient._clean()
        # noinspection PyProtectedMember
        Decorators._clean()
        # noinspection PyProtectedMember
        MockedSSHClient._clean()
        # noinspection PyProtectedMember
        StorageRouterClient._clean()

        Logger._logs = {}
        DataList._test_hooks = {}
        Toolbox._function_pointers = {}
        Configuration._unittest_data = {}

        for file_name in glob.glob(ArakoonClusterConfig.CONFIG_FILE.format('unittest*')):
            os.remove(file_name)

        for full_path in glob.glob(DalHelper.UNITTEST_DIR.format('*')):
            shutil.rmtree(full_path)

        return volatile, persistent

    @staticmethod
    def build_dal_structure(structure, previous_structure=None):
        """
        Builds a model structure
        Example:
            structure = DalHelper.build_service_structure(
                {'vpools': [1],
                 'domains': [],
                 'storagerouters': [1],
                 'storagedrivers': [(1, 1, 1)],  # (<id>, <vpool_id>, <storagerouter_id>)
                 'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
                 'storagerouter_domains': []}  # (<id>, <storagerouter_id>, <domain_id>)
            )
        """
        Configuration.set(key=Configuration.EDITION_KEY, value=PackageFactory.EDITION_ENTERPRISE)

        if previous_structure is None:
            previous_structure = {}
        vdisks = previous_structure.get('vdisks', {})
        vpools = previous_structure.get('vpools', {})
        domains = previous_structure.get('domains', {})
        services = previous_structure.get('services', {})
        mds_services = previous_structure.get('mds_services', {})
        storagerouters = previous_structure.get('storagerouters', {})
        storagedrivers = previous_structure.get('storagedrivers', {})
        storagerouter_domains = previous_structure.get('storagerouter_domains', {})

        service_types = {}
        for service_type_name in ServiceType.SERVICE_TYPES.values():
            service_type = ServiceTypeList.get_by_name(service_type_name)
            if service_type is None:
                service_type = ServiceType()
                service_type.name = service_type_name
                service_type.save()
            service_types[service_type_name] = service_type
        srclients = {}
        for domain_id in structure.get('domains', []):
            if domain_id not in domains:
                domain = Domain()
                domain.name = 'domain_{0}'.format(domain_id)
                domain.save()
                domains[domain_id] = domain
        for vpool_id in structure.get('vpools', []):
            if vpool_id not in vpools:
                vpool = VPool()
                vpool.name = str(vpool_id)
                vpool.status = 'RUNNING'
                vpool.metadata = {'backend': {'caching_info': {}}}
                vpool.metadata_store_bits = 5
                vpool.save()
                vpools[vpool_id] = vpool
            else:
                vpool = vpools[vpool_id]
            srclients[vpool_id] = StorageRouterClient(vpool.guid, None)
            Configuration.set('/ovs/vpools/{0}/mds_config|mds_tlogs'.format(vpool.guid), 100)
            Configuration.set('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), 2)
            Configuration.set('/ovs/vpools/{0}/mds_config|mds_maxload'.format(vpool.guid), 75)
            Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps({}, indent=4), raw=True)
        for sr_id in structure.get('storagerouters', []):
            if sr_id not in storagerouters:
                storagerouter = StorageRouter()
                storagerouter.name = str(sr_id)
                storagerouter.ip = '10.0.0.{0}'.format(sr_id)
                storagerouter.rdma_capable = False
                storagerouter.node_type = 'MASTER'
                storagerouter.machine_id = str(sr_id)
                storagerouter.save()
                storagerouters[sr_id] = storagerouter
                disk = Disk()
                disk.storagerouter = storagerouter
                disk.state = 'OK'
                disk.name = '/dev/uda'
                disk.size = 1 * 1024 ** 4
                disk.is_ssd = True
                disk.aliases = ['/dev/uda']
                disk.save()
                partition = DiskPartition()
                partition.offset = 0
                partition.size = disk.size
                partition.aliases = ['/dev/uda-1']
                partition.state = 'OK'
                partition.mountpoint = '/tmp/unittest/sr_{0}/disk_1/partition_1'.format(sr_id)
                partition.disk = disk
                partition.roles = [DiskPartition.ROLES.DB, DiskPartition.ROLES.SCRUB]
                partition.save()
            else:
                storagerouter = storagerouters[sr_id]

            # noinspection PyProtectedMember
            System._machine_id[storagerouter.ip] = str(sr_id)
            mds_start = 10000 + 100 * (sr_id - 1)
            mds_end = 10000 + 100 * sr_id - 1
            arakoon_start = 20000 + 100 * (sr_id - 1)
            storagedriver_start = 30000 + 100 * (sr_id - 1)
            storagedriver_end = 30000 + 100 * sr_id - 1
            Configuration.initialize_host(host_id=sr_id,
                                          port_info={'mds': [mds_start, mds_end],
                                                     'arakoon': arakoon_start,
                                                     'storagedriver': [storagedriver_start, storagedriver_end]})

        for sd_id, vpool_id, sr_id in structure.get('storagedrivers', ()):
            if sd_id not in storagedrivers:
                storagedriver = StorageDriver()
                storagedriver.vpool = vpools[vpool_id]
                storagedriver.storagerouter = storagerouters[sr_id]
                storagedriver.name = str(sd_id)
                storagedriver.mountpoint = '/'
                storagedriver.cluster_ip = storagerouters[sr_id].ip
                storagedriver.storage_ip = '10.0.1.{0}'.format(sr_id)
                storagedriver.storagedriver_id = str(sd_id)
                storagedriver.ports = {'management': 1,
                                       'xmlrpc': 2,
                                       'dtl': 3,
                                       'edge': 4}
                storagedriver.save()
                storagedrivers[sd_id] = storagedriver
                DalHelper.set_vpool_storage_driver_configuration(vpool=vpools[vpool_id], storagedriver=storagedriver)
        for mds_id, sd_id in structure.get('mds_services', ()):
            if mds_id not in mds_services:
                sd = storagedrivers[sd_id]
                s_id = '{0}-{1}'.format(sd.storagerouter.name, mds_id)
                service = Service()
                service.name = s_id
                service.storagerouter = sd.storagerouter
                service.ports = [mds_id]
                service.type = service_types['MetadataServer']
                service.save()
                services[s_id] = service
                mds_service = MDSService()
                mds_service.service = service
                mds_service.number = 0
                mds_service.capacity = 10
                mds_service.vpool = sd.vpool
                mds_service.save()
                mds_services[mds_id] = mds_service
                StorageDriverController.add_storagedriverpartition(sd, {'size': None,
                                                                        'role': DiskPartition.ROLES.DB,
                                                                        'sub_role': StorageDriverPartition.SUBROLE.MDS,
                                                                        'partition': sd.storagerouter.disks[0].partitions[0],
                                                                        'mds_service': mds_service})
        for vdisk_id, storage_driver_id, vpool_id, mds_id in structure.get('vdisks', ()):
            if vdisk_id not in vdisks:
                vpool = vpools[vpool_id]
                devicename = 'vdisk_{0}'.format(vdisk_id)
                mds_backend_config = DalHelper.generate_mds_metadata_backend_config([] if mds_id is None else [mds_services[mds_id]])
                volume_id = srclients[vpool_id].create_volume(devicename, mds_backend_config, 0, str(storage_driver_id))
                vdisk = VDisk()
                vdisk.name = str(vdisk_id)
                vdisk.devicename = devicename
                vdisk.volume_id = volume_id
                vdisk.vpool = vpool
                vdisk.size = 0
                vdisk.save()
                vdisk.reload_client('storagedriver')
                vdisks[vdisk_id] = vdisk
        for srd_id, sr_id, domain_id, backup in structure.get('storagerouter_domains', ()):
            if srd_id not in storagerouter_domains:
                sr_domain = StorageRouterDomain()
                sr_domain.backup = backup
                sr_domain.domain = domains[domain_id]
                sr_domain.storagerouter = storagerouters[sr_id]
                sr_domain.save()
                storagerouter_domains[srd_id] = sr_domain
        return {'vdisks': vdisks,
                'vpools': vpools,
                'domains': domains,
                'services': services,
                'mds_services': mds_services,
                'service_types': service_types,
                'storagerouters': storagerouters,
                'storagedrivers': storagedrivers,
                'storagerouter_domains': storagerouter_domains}

    @staticmethod
    def create_vdisks_for_mds_service(amount, start_id, mds_service=None, storagedriver=None):
        """
        Generates vdisks and appends them to a given mds_service
        """
        if (mds_service is None and storagedriver is None) or (mds_service is not None and storagedriver is not None):
            raise RuntimeError('Either `mds_service` or `storagedriver` should be passed')
        vdisks = {}
        storagedriver_id = None
        vpool = None
        mds_services = []
        if mds_service is not None:
            mds_services.append(mds_service)
            for sd in mds_service.vpool.storagedrivers:
                if sd.storagerouter_guid == mds_service.service.storagerouter_guid:
                    storagedriver_id = sd.storagedriver_id
                    vpool = sd.vpool
            if storagedriver_id is None:
                raise RuntimeError('The given MDSService is located on a node without StorageDriver')
        else:
            storagedriver_id = storagedriver.storagedriver_id
            vpool = storagedriver.vpool
        srclient = StorageRouterClient(vpool.guid, None)
        for i in xrange(start_id, start_id + amount):
            devicename = 'vdisk_{0}'.format(i)
            mds_backend_config = DalHelper.generate_mds_metadata_backend_config(mds_services)
            volume_id = srclient.create_volume(devicename, mds_backend_config, 0, str(storagedriver_id))
            if len(mds_services) == 1:
                MDSClient.set_catchup(mds_services[0], volume_id, 50)
            vdisk = VDisk()
            vdisk.name = str(i)
            vdisk.devicename = devicename
            vdisk.volume_id = volume_id
            vdisk.vpool = vpool
            vdisk.size = 0
            vdisk.save()
            vdisk.reload_client('storagedriver')
            if mds_service is not None:
                junction = MDSServiceVDisk()
                junction.vdisk = vdisk
                junction.mds_service = mds_service
                junction.is_master = True
                junction.save()
            vdisks[i] = vdisk
        return vdisks

    @staticmethod
    def create_service(service_name, service_type, storagerouter=None, ports=None):
        """
        Create a Service in the model
        :param service_name: Name to give to the Service
        :type service_name: str
        :param service_type: Type of the Service
        :type service_type: ovs.dal.hybrids.servicetype.ServiceType
        :param storagerouter: StorageRouter hosting the Service, None for externally managed Services
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param ports: Ports on which the Service is running
        :type ports: list
        :return: The newly created Service
        :rtype: ovs.dal.hybrids.service.Service
        """
        if ports is None:
            ports = []
        service = Service()
        service.name = service_name
        service.storagerouter = storagerouter
        service.ports = ports
        service.type = service_type
        service.save()
        return service

    @staticmethod
    def generate_mds_metadata_backend_config(mds_services):
        """
        Generates a fake MDSMetaDataBackendConfig
        """
        configs = []
        for mds_service in mds_services:
            config = type('MDSNodeConfig', (),
                          {'address': DalHelper._generate_nc_function(True, mds_service),
                           'port': DalHelper._generate_nc_function(False, mds_service)})()
            configs.append(config)
        return type('MDSMetaDataBackendConfig', (),
                    {'node_configs': DalHelper._generate_bc_function(configs)})()

    @staticmethod
    def _generate_nc_function(address, mds_service):
        """
        Generates the lambda that will return the address or ip
        """
        if address is True:
            return lambda s: mds_service.service.storagerouter.ip
        return lambda s: int(mds_service.service.ports[0])

    @staticmethod
    def _generate_bc_function(_configs):
        """
        Generates the lambda that will return the config list
        """
        return lambda s: _configs

    @staticmethod
    def set_vpool_storage_driver_configuration(vpool, storagedriver=None, config=None):
        """
        Mock the vpool configuration
        :param vpool: vPool to mock the configuration for
        :type vpool: vPool
        :param storagedriver: StorageDriver on which the vPool is running or None for all StorageDrivers
        :type storagedriver: ovs.dal.hybrids.storagedriver.StorageDriver|None
        :param config: Configuration settings to adjust
        :type config: dict
        :return: None
        """
        default_config = {'backend_connection_manager': {'local_connection_path': ''},
                          'content_addressed_cache': {'read_cache_serialization_path': '/var/rsp/{0}'.format(vpool.name)},
                          'distributed_lock_store': {},
                          'distributed_transaction_log': {'dtl_path': '',
                                                          'dtl_transport': 'TCP'},
                          'event_publisher': {},
                          'file_driver': {'fd_cache_path': '',
                                          'fd_namespace': 'fd-{0}-{1}'.format(vpool.name, vpool.guid)},
                          'filesystem': {'fs_dtl_host': '',
                                         'fs_dtl_mode': StorageDriverClient.VOLDRV_DTL_ASYNC,
                                         'fs_dtl_config_mode': StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE,
                                         'fs_virtual_disk_format': 'raw'},
                          'metadata_server': {},
                          'network_interface': {},
                          'scocache': {'trigger_gap': '1GiB',
                                       'backoff_gap': '2GiB',
                                       'scocache_mount_points': [{'path': '',
                                                                  'size': '{0}KiB'.format(20 * 1024 * 1024)}]},
                          'threadpool_component': {},
                          'volume_manager': {'metadata_path': '',
                                             'tlog_path': '',
                                             'clean_interval': 1,
                                             'default_cluster_size': 4096,
                                             'number_of_scos_in_tlog': 16,
                                             'non_disposable_scos_factor': 2.0},
                          'volume_registry': {'vregistry_arakoon_cluster_id': 'voldrv',
                                              'vregistry_arakoon_cluster_nodes': []},
                          'volume_router': {'vrouter_sco_multiplier': 1024},
                          'volume_router_cluster': {'vrouter_cluster_id': vpool.guid}}
        if config is not None:
            default_config.update(config)

        sds = vpool.storagedrivers
        if storagedriver is not None:
            sds = [storagedriver]
        for sd in sds:
            new_config = copy.deepcopy(default_config)
            key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, sd.storagedriver_id)
            new_config['volume_router']['vrouter_id'] = sd.storagedriver_id
            LocalStorageRouterClient.configurations[key] = new_config
            Configuration.set(key, json.dumps(new_config), raw=True)

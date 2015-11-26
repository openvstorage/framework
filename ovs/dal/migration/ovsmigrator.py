# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OVS migration module
"""

import hashlib
import random
import string


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version):
        """
        Migrates from any version to any version, running all migrations required
        If previous_version is for example 0 and this script is at
        version 3 it will execute two steps:
          - 1 > 2
          - 2 > 3
        @param previous_version: The previous version from which to start the migration.
        """

        working_version = previous_version

        # Version 1 introduced:
        # - The datastore is still empty, add defaults
        if working_version < 1:
            from ovs.dal.hybrids.user import User
            from ovs.dal.hybrids.group import Group
            from ovs.dal.hybrids.role import Role
            from ovs.dal.hybrids.client import Client
            from ovs.dal.hybrids.failuredomain import FailureDomain
            from ovs.dal.hybrids.j_rolegroup import RoleGroup
            from ovs.dal.hybrids.j_roleclient import RoleClient
            from ovs.dal.hybrids.backendtype import BackendType
            from ovs.dal.hybrids.servicetype import ServiceType
            from ovs.dal.hybrids.branding import Branding
            from ovs.dal.lists.backendtypelist import BackendTypeList

            # Create groups
            admin_group = Group()
            admin_group.name = 'administrators'
            admin_group.description = 'Administrators'
            admin_group.save()
            viewers_group = Group()
            viewers_group.name = 'viewers'
            viewers_group.description = 'Viewers'
            viewers_group.save()

            # Create users
            admin = User()
            admin.username = 'admin'
            admin.password = hashlib.sha256('admin').hexdigest()
            admin.is_active = True
            admin.group = admin_group
            admin.save()

            # Create internal OAuth 2 clients
            admin_pw_client = Client()
            admin_pw_client.ovs_type = 'INTERNAL'
            admin_pw_client.grant_type = 'PASSWORD'
            admin_pw_client.user = admin
            admin_pw_client.save()
            admin_cc_client = Client()
            admin_cc_client.ovs_type = 'INTERNAL'
            admin_cc_client.grant_type = 'CLIENT_CREDENTIALS'
            admin_cc_client.client_secret = ''.join(random.choice(string.ascii_letters +
                                                                  string.digits +
                                                                  '|_=+*#@!/-[]{}<>.?,\'";:~')
                                                    for _ in range(128))
            admin_cc_client.user = admin
            admin_cc_client.save()

            # Create roles
            read_role = Role()
            read_role.code = 'read'
            read_role.name = 'Read'
            read_role.description = 'Can read objects'
            read_role.save()
            write_role = Role()
            write_role.code = 'write'
            write_role.name = 'Write'
            write_role.description = 'Can write objects'
            write_role.save()
            manage_role = Role()
            manage_role.code = 'manage'
            manage_role.name = 'Manage'
            manage_role.description = 'Can manage the system'
            manage_role.save()

            # Attach groups to roles
            mapping = [
                (admin_group, [read_role, write_role, manage_role]),
                (viewers_group, [read_role])
            ]
            for setting in mapping:
                for role in setting[1]:
                    rolegroup = RoleGroup()
                    rolegroup.group = setting[0]
                    rolegroup.role = role
                    rolegroup.save()
                for user in setting[0].users:
                    for role in setting[1]:
                        for client in user.clients:
                            roleclient = RoleClient()
                            roleclient.client = client
                            roleclient.role = role
                            roleclient.save()

            # Add backends
            for backend_type_info in [('Ceph', 'ceph_s3'), ('Amazon', 'amazon_s3'), ('Swift', 'swift_s3'),
                                      ('Local', 'local'), ('Distributed', 'distributed'), ('ALBA', 'alba')]:
                code = backend_type_info[1]
                backend_type = BackendTypeList.get_backend_type_by_code(code)
                if backend_type is None:
                    backend_type = BackendType()
                backend_type.name = backend_type_info[0]
                backend_type.code = code
                backend_type.save()

            # Add service types
            for service_type_info in ['MetadataServer', 'AlbaProxy', 'Arakoon']:
                service_type = ServiceType()
                service_type.name = service_type_info
                service_type.save()

            # Branding
            branding = Branding()
            branding.name = 'Default'
            branding.description = 'Default bootstrap theme'
            branding.css = 'bootstrap-default.min.css'
            branding.productname = 'Open vStorage'
            branding.is_default = True
            branding.save()
            slate = Branding()
            slate.name = 'Slate'
            slate.description = 'Dark bootstrap theme'
            slate.css = 'bootstrap-slate.min.css'
            slate.productname = 'Open vStorage'
            slate.is_default = False
            slate.save()

            # Failure Domain
            failure_domain = FailureDomain()
            failure_domain.name = 'Default'
            failure_domain.save()

            # We're now at version 1
            working_version = 1

        # Version 2 introduced:
        # - new Descriptor format
        if working_version < 2:
            import imp
            from ovs.dal.helpers import Descriptor
            from ovs.extensions.storage.persistentfactory import PersistentFactory

            client = PersistentFactory.get_client()
            keys = client.prefix('ovs_data')
            for key in keys:
                data = client.get(key)
                modified = False
                for entry in data.keys():
                    if isinstance(data[entry], dict) and 'source' in data[entry] and 'hybrids' in data[entry]['source']:
                        filename = data[entry]['source']
                        if not filename.startswith('/'):
                            filename = '/opt/OpenvStorage/ovs/dal/{0}'.format(filename)
                        module = imp.load_source(data[entry]['name'], filename)
                        cls = getattr(module, data[entry]['type'])
                        new_data = Descriptor(cls, cached=False).descriptor
                        if 'guid' in data[entry]:
                            new_data['guid'] = data[entry]['guid']
                        data[entry] = new_data
                        modified = True
                if modified is True:
                    data['_version'] += 1
                    client.set(key, data)

            # We're now at version 2
            working_version = 2

        # Version 3 introduced:
        # - new Descriptor format
        if working_version < 3:
            import imp
            from ovs.dal.helpers import Descriptor
            from ovs.extensions.storage.persistentfactory import PersistentFactory

            client = PersistentFactory.get_client()
            keys = client.prefix('ovs_data')
            for key in keys:
                data = client.get(key)
                modified = False
                for entry in data.keys():
                    if isinstance(data[entry], dict) and 'source' in data[entry]:
                        module = imp.load_source(data[entry]['name'], data[entry]['source'])
                        cls = getattr(module, data[entry]['type'])
                        new_data = Descriptor(cls, cached=False).descriptor
                        if 'guid' in data[entry]:
                            new_data['guid'] = data[entry]['guid']
                        data[entry] = new_data
                        modified = True
                if modified is True:
                    data['_version'] += 1
                    client.set(key, data)

            working_version = 3

        # Version 4 introduced:
        # - Flexible SSD layout
        if working_version < 4:
            import os
            from ovs.dal.lists.storagedriverlist import StorageDriverList
            from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
            from ovs.dal.hybrids.diskpartition import DiskPartition
            from ovs.dal.lists.servicetypelist import ServiceTypeList
            from ovs.extensions.generic.remote import Remote
            from ovs.extensions.generic.sshclient import SSHClient
            from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
            for service in ServiceTypeList.get_by_name('MetadataServer').services:
                mds_service = service.mds_service
                storagedriver = None
                for current_storagedriver in service.storagerouter.storagedrivers:
                    if current_storagedriver.vpool_guid == mds_service.vpool_guid:
                        storagedriver = current_storagedriver
                        break
                tasks = {}
                if storagedriver._data.get('mountpoint_md'):
                    tasks['{0}/mds_{1}_{2}'.format(storagedriver._data.get('mountpoint_md'),
                                                   storagedriver.vpool.name,
                                                   mds_service.number)] = (DiskPartition.ROLES.DB, StorageDriverPartition.SUBROLE.MDS)
                if storagedriver._data.get('mountpoint_temp'):
                    tasks['{0}/mds_{1}_{2}'.format(storagedriver._data.get('mountpoint_temp'),
                                                   storagedriver.vpool.name,
                                                   mds_service.number)] = (DiskPartition.ROLES.SCRUB, StorageDriverPartition.SUBROLE.MDS)
                for disk in service.storagerouter.disks:
                    for partition in disk.partitions:
                        for directory, (role, subrole) in tasks.iteritems():
                            with Remote(storagedriver.storagerouter.ip, [os], username='root') as remote:
                                stat_dir = directory
                                while not remote.os.path.exists(stat_dir) and stat_dir != '/':
                                    stat_dir = stat_dir.rsplit('/', 1)[0]
                                    if not stat_dir:
                                        stat_dir = '/'
                                inode = remote.os.stat(stat_dir).st_dev
                            if partition.inode == inode:
                                if role not in partition.roles:
                                    partition.roles.append(role)
                                    partition.save()
                                number = 0
                                migrated = False
                                for sd_partition in storagedriver.partitions:
                                    if sd_partition.role == role and sd_partition.sub_role == subrole:
                                        if sd_partition.mds_service == mds_service:
                                            migrated = True
                                            break
                                        if sd_partition.partition_guid == partition.guid:
                                            number = max(sd_partition.number, number)
                                if migrated is False:
                                    sd_partition = StorageDriverPartition()
                                    sd_partition.role = role
                                    sd_partition.sub_role = subrole
                                    sd_partition.partition = partition
                                    sd_partition.storagedriver = storagedriver
                                    sd_partition.mds_service = mds_service
                                    sd_partition.size = None
                                    sd_partition.number = number + 1
                                    sd_partition.save()
                                    client = SSHClient(storagedriver.storagerouter, username='root')
                                    path = sd_partition.path.rsplit('/', 1)[0]
                                    if path:
                                        client.dir_create(path)
                                        client.dir_chown(path, 'ovs', 'ovs')
                                    client.dir_create(directory)
                                    client.dir_chown(directory, 'ovs', 'ovs')
                                    client.symlink({sd_partition.path: directory})
            for storagedriver in StorageDriverList.get_storagedrivers():
                migrated_objects = {}
                for disk in storagedriver.storagerouter.disks:
                    for partition in disk.partitions:
                        # Process all mountpoints that are unique and don't have a specified size
                        for key, (role, sr_info) in {'mountpoint_md': (DiskPartition.ROLES.DB, {'metadata_{0}': StorageDriverPartition.SUBROLE.MD,
                                                                                                'tlogs_{0}': StorageDriverPartition.SUBROLE.TLOG}),
                                                     'mountpoint_fragmentcache': (DiskPartition.ROLES.WRITE, {'fcache_{0}': StorageDriverPartition.SUBROLE.FCACHE}),
                                                     'mountpoint_foc': (DiskPartition.ROLES.WRITE, {'fd_{0}': StorageDriverPartition.SUBROLE.FD,
                                                                                                    'dtl_{0}': StorageDriverPartition.SUBROLE.DTL}),
                                                     'mountpoint_dtl': (DiskPartition.ROLES.WRITE, {'fd_{0}': StorageDriverPartition.SUBROLE.FD,
                                                                                                    'dtl_{0}': StorageDriverPartition.SUBROLE.DTL}),
                                                     'mountpoint_readcaches': (DiskPartition.ROLES.READ, {'': None}),
                                                     'mountpoint_writecaches': (DiskPartition.ROLES.WRITE, {'sco_{0}': StorageDriverPartition.SUBROLE.SCO})}.iteritems():
                            if key in storagedriver._data:
                                is_list = isinstance(storagedriver._data[key], list)
                                entries = storagedriver._data[key][:] if is_list is True else [storagedriver._data[key]]
                                for entry in entries:
                                    if not entry:
                                        if is_list:
                                            storagedriver._data[key].remove(entry)
                                            if len(storagedriver._data[key]) == 0:
                                                del storagedriver._data[key]
                                        else:
                                            del storagedriver._data[key]
                                    else:
                                        with Remote(storagedriver.storagerouter.ip, [os], username='root') as remote:
                                            inode = remote.os.stat(entry).st_dev
                                        if partition.inode == inode:
                                            if role not in partition.roles:
                                                partition.roles.append(role)
                                                partition.save()
                                            for folder, subrole in sr_info.iteritems():
                                                number = 0
                                                migrated = False
                                                for sd_partition in storagedriver.partitions:
                                                    if sd_partition.role == role and sd_partition.sub_role == subrole:
                                                        if sd_partition.partition_guid == partition.guid:
                                                            number = max(sd_partition.number, number)
                                                if migrated is False:
                                                    sd_partition = StorageDriverPartition()
                                                    sd_partition.role = role
                                                    sd_partition.sub_role = subrole
                                                    sd_partition.partition = partition
                                                    sd_partition.storagedriver = storagedriver
                                                    sd_partition.size = None
                                                    sd_partition.number = number + 1
                                                    sd_partition.save()
                                                    if folder:
                                                        source = '{0}/{1}'.format(entry, folder.format(storagedriver.vpool.name))
                                                    else:
                                                        source = entry
                                                    client = SSHClient(storagedriver.storagerouter, username='root')
                                                    path = sd_partition.path.rsplit('/', 1)[0]
                                                    if path:
                                                        client.dir_create(path)
                                                        client.dir_chown(path, 'ovs', 'ovs')
                                                    client.symlink({sd_partition.path: source})
                                                    migrated_objects[source] = sd_partition
                                            if is_list:
                                                storagedriver._data[key].remove(entry)
                                                if len(storagedriver._data[key]) == 0:
                                                    del storagedriver._data[key]
                                            else:
                                                del storagedriver._data[key]
                                            storagedriver.save()
                if 'mountpoint_bfs' in storagedriver._data:
                    storagedriver.mountpoint_dfs = storagedriver._data['mountpoint_bfs']
                    if not storagedriver.mountpoint_dfs:
                        storagedriver.mountpoint_dfs = None
                    del storagedriver._data['mountpoint_bfs']
                    storagedriver.save()
                if 'mountpoint_temp' in storagedriver._data:
                    del storagedriver._data['mountpoint_temp']
                    storagedriver.save()
                if migrated_objects:
                    print 'Loading sizes'
                    config = StorageDriverConfiguration('storagedriver', storagedriver.vpool.name)
                    config.load(SSHClient(storagedriver.storagerouter, username='ovs'))
                    for readcache in config.configuration.get('content_addressed_cache', {}).get('clustercache_mount_points', []):
                        path = readcache.get('path', '').rsplit('/', 1)[0]
                        size = int(readcache['size'].strip('KiB')) * 1024 if 'size' in readcache else None
                        if path in migrated_objects:
                            migrated_objects[path].size = long(size)
                            migrated_objects[path].save()
                    for writecache in config.configuration.get('scocache', {}).get('scocache_mount_points', []):
                        path = writecache.get('path', '')
                        size = int(writecache['size'].strip('KiB')) * 1024 if 'size' in writecache else None
                        if path in migrated_objects:
                            migrated_objects[path].size = long(size)
                            migrated_objects[path].save()

            working_version = 4

        # Version 5 introduced:
        # - Failure Domains
        if working_version < 5:
            import os
            from ovs.dal.hybrids.failuredomain import FailureDomain
            from ovs.dal.lists.failuredomainlist import FailureDomainList
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            from ovs.extensions.generic.remote import Remote
            from ovs.extensions.generic.sshclient import SSHClient
            failure_domains = FailureDomainList.get_failure_domains()
            if len(failure_domains) > 0:
                failure_domain = failure_domains[0]
            else:
                failure_domain = FailureDomain()
                failure_domain.name = 'Default'
                failure_domain.save()
            for storagerouter in StorageRouterList.get_storagerouters():
                change = False
                if storagerouter.primary_failure_domain is None:
                    storagerouter.primary_failure_domain = failure_domain
                    change = True
                if storagerouter.rdma_capable is None:
                    client = SSHClient(storagerouter, username='root')
                    rdma_capable = False
                    with Remote(client.ip, [os], username='root') as remote:
                        for root, dirs, files in remote.os.walk('/sys/class/infiniband'):
                            for directory in dirs:
                                ports_dir = remote.os.path.join(root, directory, 'ports')
                                if not remote.os.path.exists(ports_dir):
                                    continue
                                for sub_root, sub_dirs, _ in remote.os.walk(ports_dir):
                                    if sub_root != ports_dir:
                                        continue
                                    for sub_directory in sub_dirs:
                                        state_file = remote.os.path.join(sub_root, sub_directory, 'state')
                                        if remote.os.path.exists(state_file):
                                            if 'ACTIVE' in client.run('cat {0}'.format(state_file)):
                                                rdma_capable = True
                    storagerouter.rdma_capable = rdma_capable
                    change = True
                if change is True:
                    storagerouter.save()

            working_version = 5

        # Version 6
        # Distributed scrubbing
        if working_version < 6:
            from ovs.dal.hybrids.diskpartition import DiskPartition
            from ovs.dal.lists.servicetypelist import ServiceTypeList
            from ovs.dal.lists.storagedriverlist import StorageDriverList
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            from ovs.extensions.generic.sshclient import SSHClient
            from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
            for storage_driver in StorageDriverList.get_storagedrivers():
                root_client = SSHClient(storage_driver.storagerouter, username='root')
                for partition in storage_driver.partitions:
                    if partition.role == DiskPartition.ROLES.SCRUB:
                        old_path = partition.path
                        partition.sub_role = None
                        partition.save()
                        partition.invalidate_dynamics(['folder', 'path'])
                        if root_client.dir_exists(partition.path):
                            continue  # New directory already exists
                        if '_mds_' in old_path:
                            if root_client.dir_exists(old_path):
                                root_client.symlink({partition.path: old_path})
                        if not root_client.dir_exists(partition.path):
                            root_client.dir_create(partition.path)
                        root_client.dir_chmod(partition.path, 0777)

            working_version = 6

        return working_version

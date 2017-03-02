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
MigrationController module
"""
from ovs.celery_run import celery
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.services.systemd import Systemd
from ovs.lib.helpers.decorators import ensure_single
from ovs.lib.helpers.toolbox import Schedule
from ovs.log.log_handler import LogHandler


class MigrationController(object):
    """
    This controller contains (part of the) migration code. It runs out-of-band with the updater so we reduce the risk of
    failures during the update
    """
    _logger = LogHandler.get('lib', name='migrations')

    @staticmethod
    @celery.task(name='ovs.migration.migrate', schedule=Schedule(minute='0', hour='6'))
    @ensure_single(task_name='ovs.migration.migrate')
    def migrate():
        """
        Executes async migrations. It doesn't matter too much when they are executed, as long as they get eventually
        executed. This code will typically contain:
        * "dangerous" migration code (it needs certain running services)
        * Migration code depending on a cluster-wide state
        * ...
        """
        sr_client_map = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            sr_client_map[storagerouter.guid] = SSHClient(endpoint=storagerouter,
                                                          username='root')

        # Addition of 'ExecReload' for AlbaProxy SystemD services
        getattr(ServiceManager, 'has_service')  # Invoke ServiceManager to fill out the ImplementationClass (default None)
        if ServiceManager.ImplementationClass == Systemd:  # Upstart does not have functionality to reload a process' configuration
            changed_clients = set()
            for storagedriver in StorageDriverList.get_storagedrivers():
                root_client = sr_client_map[storagedriver.storagerouter_guid]
                for alba_proxy in storagedriver.alba_proxies:
                    service = alba_proxy.service
                    service_name = 'ovs-{0}'.format(service.name)
                    if not ServiceManager.has_service(name=service_name, client=root_client):
                        continue
                    if 'ExecReload=' in root_client.file_read(filename='/lib/systemd/system/{0}.service'.format(service_name)):
                        continue

                    try:
                        ServiceManager.regenerate_service(name='ovs-albaproxy', client=root_client, target_name=service_name)
                        changed_clients.add(root_client)
                    except:
                        MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
            for root_client in changed_clients:
                root_client.run(['systemctl', 'daemon-reload'])

        # Adjustment of open file descriptors for Arakoon services to 8192
        changed_clients = set()
        for storagerouter in StorageRouterList.get_storagerouters():
            root_client = sr_client_map[storagerouter.guid]
            for service_name in ServiceManager.list_services(client=root_client):
                if not service_name.startswith('ovs-arakoon-'):
                    continue
                if '8192' in root_client.file_read(filename='/lib/systemd/system/{0}.service'.format(service_name)):
                    continue
                try:
                    ServiceManager.regenerate_service(name='ovs-arakoon', client=root_client, target_name=service_name)
                    changed_clients.add(root_client)
                    ExtensionsToolbox.edit_version_file(client=root_client, package_name='arakoon', old_service_name=service_name)
                except:
                    MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
        for root_client in changed_clients:
            root_client.run(['systemctl', 'daemon-reload'])

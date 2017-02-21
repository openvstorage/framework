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
OVS migration module
"""
import os


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'  # Used by migrator.py, so don't remove
    THIS_VERSION = 12

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version, master_ips=None, extra_ips=None):
        """
        Migrates from a given version to the current version. It uses 'previous_version' to be smart
        wherever possible, but the code should be able to migrate any version towards the expected version.
        When this is not possible, the code can set a minimum version and raise when it is not met.
        :param previous_version: The previous version from which to start the migration
        :type previous_version: float
        :param master_ips: IP addresses of the MASTER nodes
        :type master_ips: list or None
        :param extra_ips: IP addresses of the EXTRA nodes
        :type extra_ips: list or None
        """

        _ = master_ips, extra_ips
        working_version = previous_version

        # From here on, all actual migration should happen to get to the expected state for THIS RELEASE
        if working_version < OVSMigrator.THIS_VERSION:
            # Adjustment of open file descriptors for Arakoon services to 8192
            from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
            from ovs.extensions.generic.configuration import Configuration, NotFoundException
            from ovs.extensions.generic.sshclient import SSHClient
            from ovs.extensions.generic.system import System
            from ovs.extensions.generic.toolbox import ExtensionsToolbox
            from ovs.extensions.services.service import ServiceManager
            from ovs.extensions.services.systemd import Systemd

            local_machine_id = System.get_my_machine_id()
            local_ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(local_machine_id))
            local_client = SSHClient(endpoint=local_ip, username='root')
            service_manager = 'systemd' if ServiceManager.ImplementationClass == Systemd else 'upstart'
            for cluster_name in list(Configuration.list('/ovs/arakoon')) + ['cacc']:
                # Retrieve metadata
                try:
                    metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
                except NotFoundException:
                    metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, ip=local_ip)

                if metadata['internal'] is False:
                    continue

                cluster_name = metadata['cluster_name']
                if service_manager == 'systemd':
                    path = '/lib/systemd/system/ovs-arakoon-{0}.service'.format(cluster_name)
                    check = 'LimitNOFILE=8192'
                else:
                    path = '/etc/init/ovs-arakoon-{0}.conf'.format(cluster_name)
                    check = 'limit nofile 8192 8192'

                restart_required = False
                if os.path.exists(path):
                    with open(path, 'r') as system_file:
                        if check not in system_file.read():
                            restart_required = True

                if restart_required is False:
                    continue

                service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
                configuration_key = '/ovs/framework/hosts/{0}/services/{1}'.format(local_machine_id, service_name)
                if Configuration.exists(configuration_key) and ServiceManager.has_service(name=service_name, client=local_client):
                    # Rewrite the service file
                    service_params = Configuration.get(configuration_key)
                    startup_dependency = service_params['STARTUP_DEPENDENCY']
                    if startup_dependency == '':
                        startup_dependency = None
                    else:
                        startup_dependency = '.'.join(startup_dependency.split('.')[:-1])  # Remove .service from startup dependency
                    ServiceManager.add_service(name='ovs-arakoon',
                                               client=local_client,
                                               params=service_params,
                                               target_name='ovs-arakoon-{0}'.format(cluster_name),
                                               startup_dependency=startup_dependency,
                                               delay_registration=True)

                    # Let the update know that Arakoon needs to be restarted
                    # Inside `if Configuration.exists`, because useless to rapport restart if we haven't rewritten service file
                    ExtensionsToolbox.edit_version_file(client=local_client, package_name='arakoon', old_service_name=service_name)

            # Multiple Proxies
            if local_client.dir_exists(directory='/opt/OpenvStorage/config/storagedriver/storagedriver'):
                local_client.dir_delete(directories=['/opt/OpenvStorage/config/storagedriver/storagedriver'])

        return OVSMigrator.THIS_VERSION

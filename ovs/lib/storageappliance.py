# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
StorageAppliance module
"""
import copy
import os

from subprocess import check_output
from ovs.celery import celery
from ovs.extensions.generic.system import Ovs
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.package import Package
from ovs.log.logHandler import LogHandler

logger = LogHandler('lib', name='storageappliance')


class StorageApplianceController(object):
    """
    Contains all BLL related to StorageAppliance
    """

    @staticmethod
    @celery.task(name='ovs.storageappliance.get_physical_metadata')
    def get_physical_metadata(files, storageappliance_guid):
        """
        Gets physical information about the machine this task is running on
        """
        from ovs.lib.vpool import VPoolController

        mountpoints = check_output('mount -v', shell=True).strip().split('\n')
        mountpoints = [p.split(' ')[2] for p in mountpoints if len(p.split(' ')) > 2
                       and not p.split(' ')[2].startswith('/dev') and not p.split(' ')[2].startswith('/proc')
                       and not p.split(' ')[2].startswith('/sys') and not p.split(' ')[2].startswith('/run')
                       and p.split(' ')[2] != '/']
        arakoon_mountpoint = Configuration.get('ovs.core.db.arakoon.location')
        if arakoon_mountpoint in mountpoints:
            mountpoints.remove(arakoon_mountpoint)
        ipaddresses = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().split('\n')
        ipaddresses = [ip.strip() for ip in ipaddresses]
        ipaddresses.remove('127.0.0.1')
        xmlrpcport = Configuration.get('volumedriver.filesystem.xmlrpc.port')
        allow_vpool = VPoolController.can_be_served_on(storageappliance_guid)
        file_existence = {}
        for check_file in files:
            file_existence[check_file] = os.path.exists(check_file) and os.path.isfile(check_file)
        return {'mountpoints': mountpoints,
                'ipaddresses': ipaddresses,
                'xmlrpcport': xmlrpcport,
                'files': file_existence,
                'allow_vpool': allow_vpool}

    @staticmethod
    @celery.task(name='ovs.storageappliance.add_vpool')
    def add_vpool(parameters):
        """
        Add a vPool to the machine this task is running on
        """
        from ovs.extensions.grid.manager import Manager
        Manager.init_vpool(parameters['storageappliance_ip'], parameters['vpool_name'], parameters=parameters)

    @staticmethod
    @celery.task(name='ovs.storageappliance.remove_vsr')
    def remove_vsr(vsr_guid):
        """
        Removes a VSR (and, if it was the last VSR for a vPool, the vPool is removed as well)
        """
        from ovs.extensions.grid.manager import Manager

        Manager.remove_vpool(vsr_guid)

    @staticmethod
    @celery.task(name='ovs.storageappliance.update_vsrs')
    def update_vsrs(vsr_guids, storageappliances, parameters):
        """
        Add/remove multiple vPools
        @param vsr_guids: VSRs to be removed
        @param storageappliances: StorageAppliances on which to add a new link
        @param parameters: Settings for new links
        """
        success = True
        # Add VSRs
        for storageappliance_ip, storageapplaince_machineid in storageappliances:
            try:
                new_parameters = copy.copy(parameters)
                new_parameters['storageappliance_ip'] = storageappliance_ip
                local_machineid = Ovs.get_my_machine_id()
                if local_machineid == storageapplaince_machineid:
                    # Inline execution, since it's on the same node (preventing deadlocks)
                    StorageApplianceController.add_vpool(new_parameters)
                else:
                    # Async execution, since it has to be executed on another node
                    # @TODO: Will break in Celery 3.2, need to find another solution
                    # Requirements:
                    # - This code cannot continue until this new task is completed (as all these Storage Appliance
                    #   need to be handled sequentially
                    # - The wait() or get() method are not allowed anymore from within a task to prevent deadlocks
                    result = StorageApplianceController.add_vpool.s(new_parameters).apply_async(
                        routing_key='sr.{0}'.format(storageapplaince_machineid)
                    )
                    result.wait()
            except:
                success = False
        # Remove VSRs
        for vsr_guid in vsr_guids:
            try:
                StorageApplianceController.remove_vsr(vsr_guid)
            except:
                success = False
        return success

    @staticmethod
    @celery.task(name='ovs.storageappliance.get_version_info')
    def get_version_info(storageappliance_guid):
        """
        Returns version information regarding a given StorageAppliance
        """
        return {'storageappliance_guid': storageappliance_guid,
                'versions': Package.get_versions()}

    @staticmethod
    @celery.task(name='ovs.storageappliance.check_s3')
    def check_s3(host, port, accesskey, secretkey):
        """
        Validates whether connection to a given S3 backend can be made
        """
        try:
            import boto
            import boto.s3.connection
            backend = boto.connect_s3(aws_access_key_id=accesskey,
                                      aws_secret_access_key=secretkey,
                                      port=port,
                                      host=host,
                                      is_secure=(port == 443),
                                      calling_format=boto.s3.connection.OrdinaryCallingFormat())
            backend.get_all_buckets()
            return True
        except Exception as ex:
            logger.exception('Error during S3 check: {0}'.format(ex))
            return False

    @staticmethod
    @celery.task(name='ovs.storageappliance.check_mtpt')
    def check_mtpt(name):
        """
        Checks whether a given mountpoint for vPool is in use
        """
        mountpoint = '/mnt/{0}'.format(name)
        if not os.path.exists(mountpoint):
            return True
        return check_output('sudo -s ls -al {0} | wc -l'.format(mountpoint), shell=True).strip() == '3'

# Copyright 2015 iNuron NV
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
License module
"""
import re
import time
import zlib
import json
import base64
from ovs.celery_run import celery
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.api.client import OVSClient
from ovs.extensions.support.agent import SupportAgent
from ovs.extensions.generic.configuration import Configuration
from ovs.dal.hybrids.license import License
from ovs.dal.lists.licenselist import LicenseList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.helpers.decorators import add_hooks
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='license')


class LicenseController(object):
    """
    Validates licenses
    """

    @staticmethod
    @celery.task(name='ovs.license.validate')
    def validate(license_string):
        """
        Validates a license with the various components
        """
        try:
            result = {}
            data = LicenseController._decode(license_string)
            for component in data:
                cdata = data[component]
                name = cdata['name']
                data = cdata['data']
                _ = cdata['token']
                valid_until = float(cdata['valid_until']) if 'valid_until' in cdata else None
                if valid_until is not None and valid_until <= time.time():
                    result[component] = False
                    continue
                signature = cdata['signature'] if 'signature' in cdata else None
                validate_functions = Toolbox.fetch_hooks('license', '{0}.validate'.format(component))
                apply_functions = Toolbox.fetch_hooks('license', '{0}.apply'.format(component))
                if len(validate_functions) == 1 and len(apply_functions) == 1:
                    try:
                        valid, metadata = validate_functions[0](component=component, data=data, signature=signature)
                    except Exception, ex:
                        logger.debug('Error validating license for {0}: {1}'.format(component, ex))
                        valid = False
                        metadata = None
                    if valid is False:
                        logger.debug('Invalid license for {0}: {1}'.format(component, license_string))
                        result[component] = False
                    else:
                        result[component] = {'valid_until': valid_until,
                                             'metadata': metadata,
                                             'name': name}
                else:
                    logger.debug('No validate nor apply functions found for {0}'.format(component))
                    result[component] = False
            return result
        except Exception, ex:
            logger.exception('Error validating license: {0}'.format(ex))
            raise

    @staticmethod
    @celery.task(name='ovs.license.apply')
    def apply(license_string):
        """
        Applies a license. It will apply as much licenses as possible, however, it won't fail on invalid licenses as it
        will simply skip them.
        """
        try:
            clients = {}
            storagerouters = StorageRouterList.get_storagerouters()
            try:
                for storagerouter in storagerouters:
                    clients[storagerouter] = SSHClient(storagerouter.ip)
            except UnableToConnectException:
                raise RuntimeError('Not all StorageRouters are reachable')
            data = LicenseController._decode(license_string)
            for component in data:
                cdata = data[component]
                name = cdata['name']
                data = cdata['data']
                token = cdata['token']
                valid_until = float(cdata['valid_until']) if 'valid_until' in cdata else None
                if valid_until is not None and valid_until <= time.time():
                    continue
                signature = cdata['signature'] if 'signature' in cdata else None
                validate_functions = Toolbox.fetch_hooks('license', '{0}.validate'.format(component))
                apply_functions = Toolbox.fetch_hooks('license', '{0}.apply'.format(component))
                if len(validate_functions) == 1 and len(apply_functions) == 1:
                    valid, metadata = validate_functions[0](component=component, data=data, signature=signature)
                    if valid is True:
                        success = apply_functions[0](component=component, data=data, signature=signature)
                        if success is True:
                            license_object = LicenseList.get_by_component(component)
                            if license_object is None:
                                license_object = License()
                            license_object.component = component
                            license_object.name = name
                            license_object.token = token
                            license_object.data = data
                            license_object.valid_until = valid_until
                            license_object.signature = signature
                            license_object.save()
            license_contents = []
            for lic in LicenseList.get_licenses():
                license_contents.append(lic.hash)
            for storagerouter in storagerouters:
                client = clients[storagerouter]
                client.file_write('/opt/OpenvStorage/config/licenses', '{0}\n'.format('\n'.join(license_contents)))
        except Exception, ex:
            logger.exception('Error applying license: {0}'.format(ex))
            return None

    @staticmethod
    @celery.task(name='ovs.license.remove')
    def remove(license_guid):
        """
        Removes a license
        """
        clients = {}
        storagerouters = StorageRouterList.get_storagerouters()
        try:
            for storagerouter in storagerouters:
                clients[storagerouter] = SSHClient(storagerouter.ip)
        except UnableToConnectException:
            raise RuntimeError('Not all StorageRouters are reachable')
        lic = License(license_guid)
        if lic.can_remove is True:
            remove_functions = Toolbox.fetch_hooks('license', '{0}.remove'.format(lic.component))
            result = remove_functions[0](component=lic.component, data=lic.data, valid_until=lic.valid_until, signature=lic.signature)
            if result is True:
                lic.delete()
                license_contents = []
                for lic in LicenseList.get_licenses():
                    license_contents.append(lic.hash)
                for storagerouter in storagerouters:
                    client = clients[storagerouter]
                    client.file_write('/opt/OpenvStorage/config/licenses', '{0}\n'.format('\n'.join(license_contents)))
            return result
        return None

    @staticmethod
    @add_hooks('setup', 'extranode')
    def add_extra_node(**kwargs):
        ip = kwargs['cluster_ip']
        license_contents = []
        for lic in LicenseList.get_licenses():
            license_contents.append(lic.hash)
        client = SSHClient(ip)
        client.file_write('/opt/OpenvStorage/config/licenses', '{0}\n'.format('\n'.join(license_contents)))

    @staticmethod
    @celery.task(name='ovs.license.register')
    def register(name, email, company, phone, newsletter):
        """
        Registers the environment
        """
        SupportAgent().run()  # Execute a single heartbeat run
        client = OVSClient('monitoring.openvstorage.com', 443, credentials=None, verify=True, version=1)
        task_id = client.post('/support/register/',
                              data={'cluster_id': Configuration.get('ovs.support.cid'),
                                    'name': name,
                                    'email': email,
                                    'company': company,
                                    'phone': phone,
                                    'newsletter': newsletter,
                                    'register_only': True})
        if task_id:
            client.wait_for_task(task_id, timeout=120)
        for storagerouter in StorageRouterList.get_storagerouters():
            client = SSHClient(storagerouter)
            client.config_set('ovs.core.registered', True)

    @staticmethod
    def _encode(data):
        return base64.b64encode(zlib.compress(json.dumps(data)))

    @staticmethod
    def _decode(string):
        string = re.sub('\s', '', string)
        return json.loads(zlib.decompress(base64.b64decode(string)))

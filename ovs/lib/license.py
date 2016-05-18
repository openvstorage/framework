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
License module
"""
import re
import time
import zlib
import json
import base64
from ovs.celery_run import celery
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.dal.hybrids.license import License
from ovs.dal.lists.licenselist import LicenseList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.helpers.decorators import add_hooks
from ovs.log.logHandler import LogHandler


class LicenseController(object):
    """
    Validates licenses
    """
    _logger = LogHandler.get('lib', name='license')

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
                        LicenseController._logger.debug('Error validating license for {0}: {1}'.format(component, ex))
                        valid = False
                        metadata = None
                    if valid is False:
                        LicenseController._logger.debug('Invalid license for {0}: {1}'.format(component, license_string))
                        result[component] = False
                    else:
                        result[component] = {'valid_until': valid_until,
                                             'metadata': metadata,
                                             'name': name}
                else:
                    LicenseController._logger.debug('No validate nor apply functions found for {0}'.format(component))
                    result[component] = False
            return result
        except Exception, ex:
            LicenseController._logger.exception('Error validating license: {0}'.format(ex))
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
            LicenseController._logger.exception('Error applying license: {0}'.format(ex))
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
        """
        Add extra node hook
        :param kwargs: Extra parameters
        :return: None
        """
        ip = kwargs['cluster_ip']
        license_contents = []
        for lic in LicenseList.get_licenses():
            license_contents.append(lic.hash)
        client = SSHClient(ip)
        client.file_write('/opt/OpenvStorage/config/licenses', '{0}\n'.format('\n'.join(license_contents)))

    @staticmethod
    def _encode(data):
        return base64.b64encode(zlib.compress(json.dumps(data)))

    @staticmethod
    def _decode(string):
        string = re.sub('\s', '', string)
        return json.loads(zlib.decompress(base64.b64decode(string)))

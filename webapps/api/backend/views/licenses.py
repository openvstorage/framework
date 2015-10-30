# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for licenses
"""

import re
from backend.decorators import required_roles, load, return_object, return_list, return_task, log
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.lib.license import LicenseController
from ovs.dal.hybrids.license import License
from ovs.dal.lists.licenselist import LicenseList


class LicenseSet(viewsets.ViewSet):
    """
    Information about Licenses
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'licenses'
    base_name = 'licenses'

    @log()
    @required_roles(['read'])
    @return_list(License)
    @load()
    def list(self, component=None):
        """
        Lists all available Licenses
        """
        if component is None:
            return LicenseList.get_licenses()
        return LicenseList.get_by_component(component, return_as_list=True)

    @log()
    @required_roles(['read'])
    @return_object(License)
    @load(License)
    def retrieve(self, license):
        """
        Load information about a given License
        """
        return license

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load()
    def create(self, license_string, validate_only, registration_parameters=None):
        """
        Validates/Applies a license
        """
        if registration_parameters is None:
            if validate_only is True:
                return LicenseController.validate.delay(license_string)
            if validate_only is False:
                return LicenseController.apply.delay(license_string)
        else:
            invalid = []
            validation = {'name': '^[a-zA-Z0-9\-\' .]{3,}$',
                          'email': '^[a-zA-Z0-9\-\' .+]{3,}@[a-zA-Z0-9\-\' .+]{3,}$',  # Fairly simple regex, but the license is mailed anyway
                          'company': None,
                          'phone': None}
            for key in validation:
                if key not in registration_parameters or (validation[key] is not None and re.match(validation[key], registration_parameters[key]) is None):
                    invalid.append(key)
            if 'newsletter' not in registration_parameters or not isinstance(registration_parameters['newsletter'], bool):
                invalid.append('newsletter')
            if len(invalid) != 0:
                raise RuntimeError('Invalid parameters in registration_parameters: {0}'.format(', '.format(invalid)))
            return LicenseController.get_free_license.delay(registration_parameters)

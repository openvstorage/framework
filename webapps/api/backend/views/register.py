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
Module for licenses
"""

import re
from backend.decorators import required_roles, load, return_object, return_list, return_task, log
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.lib.license import LicenseController
from ovs.dal.hybrids.license import License
from ovs.dal.lists.licenselist import LicenseList


class RegisterSet(viewsets.ViewSet):
    """
    Information about Registrations - Used strictly as a stand-alone POST action entry
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'register'
    base_name = 'register'

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load()
    def create(self, name, email, company=None, phone=None, newsletter=False):
        """
        Validates/Applies a license
        """
        invalid = []
        validation = {'name': '^[a-zA-Z0-9\-\' .]{3,}$',
                      'email': '^[a-zA-Z0-9\-\' .+]{3,}@[a-zA-Z0-9\-\' .+]{3,}$',  # Fairly simple regex, but the license is mailed anyway
                      'company': None,
                      'phone': None}
        for key, variable in {'name': name, 'email': email, 'company': company, 'phone': phone}.iteritems():
            if validation[key] is not None and re.match(validation[key], variable) is None:
                invalid.append(key)
        if not isinstance(newsletter, bool):
            invalid.append('newsletter')
        if len(invalid) != 0:
            raise RuntimeError('Invalid parameters in registration_parameters: {0}'.format(', '.format(invalid)))
        return LicenseController.register.delay(name, email, company, phone, newsletter)

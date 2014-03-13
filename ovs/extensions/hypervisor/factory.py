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
Hypervisor factory module
"""
from hypervisors.vmware import VMware
from hypervisors.hyperv import HyperV
from hypervisors.xen import Xen
from hypervisors.kvm import KVM
from ovs.extensions.generic.filemutex import FileMutex


class Factory(object):
    """
    Factory class provides functionality to get abstracted hypervisor
    """

    hypervisors = {}

    @staticmethod
    def get(node):
        """
        Returns the appropriate hypervisor client class for a given VMachine
        """
        hvtype   = node.hvtype
        ip       = node.ip
        username = node.username
        password = node.password
        key = '{0}_{1}'.format(ip, username)
        if key not in Factory.hypervisors:
            mutex = FileMutex('hypervisor_{0}'.format(key))
            try:
                mutex.acquire(30)
                if key not in Factory.hypervisors:
                    if hvtype == 'VMWARE':
                        hypervisor = VMware(ip, username, password)
                    elif hvtype == 'KVM':
                        hypervisor = KVM(ip, username, password)
                    else:
                        raise NotImplementedError('Hypervisor {0} is not yet supported'.format(hvtype))
                    Factory.hypervisors[key] = hypervisor
            finally:
                mutex.release()
        return Factory.hypervisors[key]

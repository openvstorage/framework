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

import os
from subprocess import check_output


class SSHClient(object):
    """
    Remote/local client
    """

    @staticmethod
    def load(ip, password=None):
        """
        Opens a client connection to a remote or local system
        """

        from ovs.plugin.provider.remote import Remote
        client = Remote.cuisine.api
        Remote.cuisine.fabric.env['password'] = password
        Remote.cuisine.fabric.output['stdout'] = False
        Remote.cuisine.fabric.output['running'] = False
        client.connect(ip)
        return client

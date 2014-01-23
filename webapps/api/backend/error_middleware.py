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
Error middleware module
"""
import traceback
import os


class ExceptionMiddleware(object):
    """
    Error middleware object
    """
    def process_exception(self, request, exception):
        """
        Logs information about the given error to a plain logfile
        """
        _ = request, exception
        # @TODO: Use a real logger instead of raw dumping to a file
        os.system("echo '" + traceback.format_exc() + "' >> /var/log/ovs/django.log")


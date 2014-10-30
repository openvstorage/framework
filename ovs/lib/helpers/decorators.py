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
Contains various decorators
"""

import time
from ovs.dal.hybrids.log import Log
from ovs.dal.lists.storagedriverlist import StorageDriverList


def log(event_type):
    """
    Task logger
    """

    def wrap(f):
        """
        Wrapper function
        """

        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            # Log the call
            log_entry = Log()
            log_entry.source = event_type
            log_entry.module = f.__module__
            log_entry.method = f.__name__
            log_entry.method_args = list(args)
            log_entry.method_kwargs = kwargs
            log_entry.time = time.time()
            if event_type == 'VOLUMEDRIVER_TASK':
                log_entry.storagedriver = StorageDriverList.get_by_storagedriver_id(kwargs['storagedriver_id'])
            log_entry.save()

            # Call the function
            return f(*args, **kwargs)

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function

    return wrap

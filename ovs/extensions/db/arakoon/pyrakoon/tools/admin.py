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

import time
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon import errors, utils
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.client import admin
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonClientConfig, ArakoonNoMaster
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import _ArakoonClient, _convert_exceptions, _validate_signature
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', 'pyrakoon')


class ArakoonAdminClient(object):
    def __init__(self, node_id, config):
        self._client = _ArakoonAdminClient(node_id, config)

    @utils.update_argspec('self', 'n')
    @_convert_exceptions
    @_validate_signature('int')
    def collapse_tlogs(self, n):
        return self._client.collapse_tlogs(n)


class _ArakoonAdminClient(_ArakoonClient, admin.ClientMixin):
    def __init__(self, node_id, config):
        super(_ArakoonAdminClient, self).__init__(config)
        self._node_id = node_id

    def _process(self, message):
        bytes_ = ''.join(message.serialize())

        self._lock.acquire()

        try:
            start = time.time()
            try_count = 0.0
            backoff_period = 0.2
            call_succeeded = False
            retry_period = ArakoonClientConfig.getNoMasterRetryPeriod()
            deadline = start + retry_period

            while not call_succeeded and time.time() < deadline:
                try:
                    # Send on wire
                    connection = self._send_message(self._node_id, bytes_)
                    return utils.read_blocking(message.receive(), connection.read)
                except (errors.NotMaster, ArakoonNoMaster):
                    self.master_id = None
                    self.drop_connections()

                    sleep_period = backoff_period * try_count
                    if time.time() + sleep_period > deadline:
                        raise

                    try_count += 1.0
                    logger.warning('Master not found, retrying in {0:.2f} seconds'.format(sleep_period))

                    time.sleep(sleep_period)
        finally:
            self._lock.release()

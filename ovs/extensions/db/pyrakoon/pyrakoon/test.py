# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010 Incubaid BVBA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Testing utilities'''

import os.path
import time
import shutil
import struct
import logging
import tempfile
import subprocess

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from pyrakoon import client, compat, errors, protocol, utils


LOGGER = logging.getLogger(__name__)

#pylint: disable=R0904
class FakeClient(object, client.AbstractClient, client.ClientMixin):
    '''Fake, in-memory Arakoon client'''

    VERSION = 'FakeRakoon/0.1'
    '''Version of the server we fake''' #pylint: disable=W0105
    MASTER = 'arakoon0'
    '''Name of master node''' #pylint: disable=W0105

    connected = True

    def __init__(self):
        super(FakeClient, self).__init__()

        self._values = {}

    def _process(self, message): #pylint: disable=R0912
        bytes_ = StringIO.StringIO(''.join(message.serialize())).read

        # Helper
        recv = lambda type_: utils.read_blocking(type_.receive(), bytes_)

        command = recv(protocol.UINT32)

        def handle_hello():
            '''Handle a "hello" command'''

            _ = recv(protocol.STRING)
            _ = recv(protocol.STRING)

            for rbytes in protocol.UINT32.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes
            for rbytes in protocol.STRING.serialize(self.VERSION):
                yield rbytes

        def handle_exists():
            '''Handle an "exists" command'''

            _ = recv(protocol.BOOL)
            key = recv(protocol.STRING)

            for rbytes in protocol.UINT32.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes
            for rbytes in protocol.BOOL.serialize(key in self._values):
                yield rbytes

        def handle_who_master():
            '''Handle a "who_master" command'''

            for rbytes in protocol.UINT32.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes
            for rbytes in protocol.Option(protocol.STRING).serialize(
                self.MASTER):
                yield rbytes

        def handle_get():
            '''Handle a "get" command'''

            _ = recv(protocol.BOOL)
            key = recv(protocol.STRING)

            if key not in self._values:
                for rbytes in protocol.UINT32.serialize(
                    errors.NotFound.CODE):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(key):
                    yield rbytes
            else:
                for rbytes in protocol.UINT32.serialize(
                    protocol.RESULT_SUCCESS):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(self._values[key]):
                    yield rbytes

        def handle_set():
            '''Handle a "set" command'''

            key = recv(protocol.STRING)
            value = recv(protocol.STRING)

            self._values[key] = value

            for rbytes in protocol.UINT32.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes

        def handle_delete():
            '''Handle a "delete" command'''

            key = recv(protocol.STRING)

            if key not in self._values:
                for rbytes in protocol.UINT32.serialize(
                    errors.NotFound.CODE):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(key):
                    yield rbytes
            else:
                del self._values[key]
                for rbytes in protocol.UINT32.serialize(
                    protocol.RESULT_SUCCESS):
                    yield rbytes

        def handle_prefix_keys():
            '''Handle a "prefix_keys" command'''

            _ = recv(protocol.BOOL)
            prefix = recv(protocol.STRING)
            max_elements = recv(protocol.UINT32)

            matches = [key for key in self._values.iterkeys()
                if key.startswith(prefix)]

            matches = matches if max_elements < 0 else matches[:max_elements]

            for rbytes in protocol.UINT32.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes

            for rbytes in protocol.List(protocol.STRING).serialize(matches):
                yield rbytes

        def handle_test_and_set():
            '''Handle a "test_and_set" command'''

            key = recv(protocol.STRING)
            test_value = recv(protocol.Option(protocol.STRING))
            set_value = recv(protocol.Option(protocol.STRING))

            # Key doesn't exist and test_value is not None -> NotFound
            if key not in self._values and test_value is not None:
                for rbytes in protocol.UINT32.serialize(
                    errors.NotFound.CODE):
                    yield rbytes
                for rbytes in protocol.STRING.serialize(key):
                    yield rbytes

                return

            # Key doesn't exist and test_value is None -> create
            if key not in self._values and test_value is None:
                self._values[key] = set_value

                for rbytes in protocol.UINT32.serialize(
                    protocol.RESULT_SUCCESS):
                    yield rbytes
                for rbytes in protocol.Option(protocol.STRING).serialize(None):
                    yield rbytes

                return

            # Key exists
            orig_value = self._values[key]

            # Need to update?
            if test_value == orig_value:
                if set_value is not None:
                    self._values[key] = set_value
                else:
                    del self._values[key]

            # Return original value
            for rbytes in protocol.UINT32.serialize(
                protocol.RESULT_SUCCESS):
                yield rbytes

            for rbytes in protocol.Option(protocol.STRING).serialize(
                orig_value):
                yield rbytes


        handlers = {
            protocol.Hello.TAG: handle_hello,
            protocol.Exists.TAG: handle_exists,
            protocol.WhoMaster.TAG: handle_who_master,
            protocol.Get.TAG: handle_get,
            protocol.Set.TAG: handle_set,
            protocol.Delete.TAG: handle_delete,
            protocol.PrefixKeys.TAG: handle_prefix_keys,
            protocol.TestAndSet.TAG: handle_test_and_set,
        }

        if command in handlers:
            result = StringIO.StringIO(''.join(handlers[command]()))
        else:
            result = StringIO.StringIO()
            result.write(struct.pack('<I', errors.UnknownFailure.CODE))
            result.write(struct.pack('<I', 0))
            result.seek(0)

        return utils.read_blocking(message.receive(), result.read)


DEFAULT_CLIENT_PORT = 4932
DEFAULT_MESSAGING_PORT = 4933

class ArakoonEnvironmentMixin: #pylint: disable=C1001
    '''Test mixin to manage an Arakoon process'''

    #pylint: disable=C0103,W0232,W0201
    def setUpArakoon(self, name, config_template):
        '''Launch an Arakoon daemon process

        :param name: Cluster name
        :type name: `str`
        :param config_template: Configuration file template
        :type config_template: `str`

        :return: Client configuration tuple, config path and base path
        :rtype: `((str, dict<str, (str, int)>), str, str)`
        '''

        base = tempfile.mkdtemp(prefix=name)
        self._arakoon_environment_base = base
        LOGGER.info('Running in %s', base)

        home_dir = os.path.join(base, 'home')
        os.mkdir(home_dir)

        log_dir = os.path.join(base, 'log')
        os.mkdir(log_dir)

        config_path = os.path.join(base, 'config.ini')
        config = config_template % {
            'CLIENT_PORT': DEFAULT_CLIENT_PORT,
            'MESSAGING_PORT': DEFAULT_MESSAGING_PORT,
            'HOME': home_dir,
            'LOG_DIR': log_dir,
            'CLUSTER_ID': name,
        }

        fd = open(config_path, 'w')
        try:
            fd.write(config)
        finally:
            fd.close()

        # Start server
        command = ['arakoon', '-config', config_path, '--node', 'arakoon_0']
        self._arakoon_process = subprocess.Popen(
            command, close_fds=True, cwd=base)

        #pylint: disable=E1101
        LOGGER.info('Arakoon running, PID %d', self._arakoon_process.pid)
        #pylint: enable=E1101

        return (name, {
            'arakoon_0': (['127.0.0.1'], DEFAULT_CLIENT_PORT),
        }), config_path, base

    def tearDownArakoon(self):
        '''Teardown a managed Arakoon process'''

        try:
            if self._arakoon_process:

                #pylint: disable=E1101
                LOGGER.info(
                    'Killing Arakoon process %d', self._arakoon_process.pid)
                try:
                    self._arakoon_process.terminate()
                except OSError:
                    LOGGER.exception('Failure while killing Arakoon')
                #pylint: enable=E1101

        finally:
            base = self._arakoon_environment_base
            if os.path.isdir(base):
                LOGGER.info('Removing tree %s', base)
                shutil.rmtree(base)


#pylint: disable=W0232
class NurseryEnvironmentMixin(ArakoonEnvironmentMixin):
    '''Test mixin to manage an Arakoon nursery keeper'''

    #pylint: disable=C0103
    def setUpNursery(self, name, config_template):
        '''Launch an Arakoon nursery keeper daemon process

        :param name: Cluster name
        :type name: `str`
        :param config_template: Configuration file template
        :type config_template: `str`

        :return: Client configuration tuple, config path and base path
        :rtype: `((str, dict<str, (str, int)>), str, str)`
        '''

        client_config, config_path, base = self.setUpArakoon(
            name, config_template)

        #pylint: disable=W0142
        compat_client_config = compat.ArakoonClientConfig(*client_config)

        # Give server some time to get up
        ok = False
        for _ in xrange(5):
            LOGGER.info('Attempting hello call')
            try:
                client_ = compat.ArakoonClient(compat_client_config)
                client_.hello('testsuite', compat_client_config.getClusterId())
                client_.dropConnections() #pylint: disable=W0212
            except: #pylint: disable=W0702
                LOGGER.exception('Call failed, sleeping')
            else:
                LOGGER.debug('Call succeeded')
                ok = True
                break

        if not ok:
            raise RuntimeError('Unable to start Arakoon server')

        subprocess.check_call([
            'arakoon', '-config', config_path, '--nursery-init',
            client_config[0]
        ], close_fds=True, cwd=base)

        time.sleep(5)

        return client_config, config_path, base

    def tearDownNursery(self):
        '''Teardown a managed Arakoon nursery keeper process'''

        self.tearDownArakoon()

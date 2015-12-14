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

'''Arakoon client interface'''

from pyrakoon import errors, protocol
import pyrakoon.utils
from pyrakoon.client.utils import call

class ClientMixin: #pylint: disable=W0232,R0904,old-style-class
    '''Mixin providing client actions for standard cluster functionality

    This can be mixed into any class implementing :class:`AbstractClient`.
    '''

    #pylint: disable=C0111
    @call(protocol.Hello)
    def hello(self): #pylint: disable=R0201
        assert False

    @call(protocol.Exists)
    def exists(self): #pylint: disable=R0201
        assert False

    @call(protocol.WhoMaster)
    def who_master(self): #pylint: disable=R0201
        assert False

    @call(protocol.Get)
    def get(self): #pylint: disable=R0201
        assert False

    @call(protocol.Set)
    def set(self): #pylint: disable=R0201
        assert False

    @call(protocol.Delete)
    def delete(self): #pylint: disable=R0201
        assert False

    @call(protocol.PrefixKeys)
    def prefix(self): #pylint: disable=R0201
        assert False

    @call(protocol.TestAndSet)
    def test_and_set(self): #pylint: disable=R0201
        assert False

    @call(protocol.Sequence)
    def sequence(self): #pylint: disable=R0201
        assert False

    @call(protocol.Range)
    def range(self): #pylint: disable=R0201
        assert False

    @call(protocol.RangeEntries)
    def range_entries(self): #pylint: disable=R0201
        assert False

    @call(protocol.MultiGet)
    def multi_get(self): #pylint: disable=R0201
        assert False

    @call(protocol.MultiGetOption)
    def multi_get_option(self): #pylint: disable=R0201
        assert False

    @call(protocol.ExpectProgressPossible)
    def expect_progress_possible(self): #pylint: disable=R0201
        assert False

    @call(protocol.GetKeyCount)
    def get_key_count(self): #pylint: disable=R0201
        assert False

    @call(protocol.UserFunction)
    def user_function(self): #pylint: disable=R0201
        assert False

    @call(protocol.Confirm)
    def confirm(self): #pylint: disable=R0201
        assert False

    @call(protocol.Assert)
    def assert_(self): #pylint: disable=R0201
        assert False

    @call(protocol.RevRangeEntries)
    def rev_range_entries(self): #pylint: disable=R0201
        assert False

    @call(protocol.Statistics)
    def statistics(self): #pylint: disable=R0201
        assert False

    @call(protocol.Version)
    def version(self): #pylint: disable=R0201
        assert False

    @call(protocol.AssertExists)
    def assert_exists(self): #pylint: disable=R0201
        assert False

    @call(protocol.DeletePrefix)
    def delete_prefix(self): #pylint: disable=R0201
        assert False

    @call(protocol.Replace)
    def replace(self): #pylint: disable=R0201
        assert False

    @call(protocol.Nop)
    def nop(self): #pylint: disable=R0201
        assert False

    @call(protocol.GetCurrentState)
    def get_current_state(self): #pylint: disable=R0201
        assert False

    __getitem__ = get
    __setitem__ = set
    __delitem__ = delete
    __contains__ = exists


class NotConnectedError(RuntimeError):
    '''Error used when a call on a not-connected client is made'''


class AbstractClient: #pylint: disable=W0232,R0903,R0922,old-style-class
    '''Abstract base class for implementations of Arakoon clients'''

    #pylint: disable=pointless-string-statement
    connected = False
    '''Flag to denote whether the client is connected

    If this is :data:`False`, a :class:`NotConnectedError` will be raised when
    a call is issued.

    :type: :class:`bool`
    '''

    def _process(self, message):
        '''
        Submit a message to the server, parse the result and return it

        The given `message` should be serialized using its
        :meth:`~pyrakoon.protocol.Message.serialize` method and submitted to
        the server. Then the :meth:`~pyrakoon.protocol.Message.receive`
        coroutine of the `message` should be used to retrieve and parse a
        result from the server. The result value should be returned by this
        method, or any exceptions should be rethrown if caught.

        :param message: Message to handle
        :type message: :class:`pyrakoon.protocol.Message`

        :return: Server result value
        :rtype: :obj:`object`

        :see: :func:`pyrakoon.utils.process_blocking`
        '''

        raise NotImplementedError


#pylint: disable=R0904
class SocketClient(object, AbstractClient):
    '''Arakoon client using TCP to contact a cluster node

    :warning: Due to the lack of resource and exception management, this is
        not intended to be used in real-world code.
    '''

    def __init__(self, address, cluster_id):
        '''
        :param address: Node address (host & port)
        :type address: `(str, int)`
        :param cluster_id: Identifier of the cluster
        :type cluster_id: `str`
        '''

        import threading

        super(SocketClient, self).__init__()

        self._lock = threading.Lock()

        self._socket = None
        self._address = address
        self._cluster_id = cluster_id

    def connect(self):
        '''Create client socket and connect to server'''

        import socket

        self._socket = socket.create_connection(self._address)
        prologue = protocol.build_prologue(self._cluster_id)
        self._socket.sendall(prologue)

    @property
    def connected(self):
        '''Check whether a connection is available'''

        return self._socket is not None

    def _process(self, message):
        self._lock.acquire()

        try:
            for part in message.serialize():
                self._socket.sendall(part)

            return pyrakoon.utils.read_blocking(
                message.receive(), self._socket.recv)
        except Exception as exc:
            if not isinstance(exc, errors.ArakoonError):
                try:
                    if self._socket:
                        self._socket.close()
                finally:
                    self._socket = None

            raise
        finally:
            self._lock.release()

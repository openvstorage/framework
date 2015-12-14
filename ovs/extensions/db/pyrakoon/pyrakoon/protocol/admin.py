# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2013, 2014 Incubaid BVBA
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

'''Arakoon administrative call implementations'''

import operator

from pyrakoon import errors, protocol, utils

class OptimizeDB(protocol.Message):
    '''"optimize_db" message'''

    __slots__ = ()

    TAG = 0x0025 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "optimize_db" command to the server

        This method will trigger optimization of the store on the node this
        command is sent to.

        :note: This only works on slave nodes
    ''')


class DefragDB(protocol.Message):
    '''"defrag_db" message'''

    __slots__ = ()

    TAG = 0x0026 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "defrag_db" command to the server

        This method will trigger defragmentation of the store on the node this
        comamand is sent to.

        :note: This only works on slave nodes
    ''')


class DropMaster(protocol.Message):
    '''"drop_master" message'''

    __slots__ = ()

    TAG = 0x0030 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "drop_master" command to the server

        This method instructs a node to drop its master role, if possible.
        When the call returns successfully, the node was no longer master, but
        could have gained the master role already in-between.

        :note: This doesn't work in a single-node environment
    ''')


class CollapseTlogs(protocol.Message):
    '''"collapse_tlogs" message'''

    __slots__ = '_count',

    TAG = 0x0014 | protocol.Message.MASK
    ARGS = ('count', protocol.INT32),
    RETURN_TYPE = None # Hack to work around irregular return type

    DOC = utils.format_doc('''
        Send a "collapse_tlogs" command to the server

        This method instructs a node to collapse its *TLOG* collection by
        constructing a *head* database and removing superfluous *TLOG* files.

        The number of *TLOG* files to keep should be passed as a parameter.

        :param count: Number of *TLOG* files to keep
        :type count: :class:`int`
        :return: For every *TLOG*, the time it took to collapse it
        :rtype: `[int]`
    ''')

    def __init__(self, count):
        self._count = count

    count = property(operator.attrgetter('_count'))

    def receive(self): #pylint: disable=R0912
        self.RETURN_TYPE = protocol.INT32 #pylint: disable=C0103

        count_receiver = protocol.Message.receive(self)
        request = count_receiver.next()

        while isinstance(request, protocol.Request):
            value = yield request
            request = count_receiver.send(value)

        if not isinstance(request, protocol.Result):
            raise TypeError

        count = request.value

        result = [None] * count

        for idx in xrange(count):
            success_receiver = protocol.INT32.receive()
            request = success_receiver.next()

            while isinstance(request, protocol.Request):
                value = yield request
                request = success_receiver.send(value)

            if not isinstance(request, protocol.Result):
                raise TypeError

            success = request.value

            if success == 0:
                time_receiver = protocol.INT64.receive()
                request = time_receiver.next()

                while isinstance(request, protocol.Request):
                    value = yield request
                    request = time_receiver.send(value)

                if not isinstance(request, protocol.Result):
                    raise TypeError

                time = request.value
                result[idx] = time
            else:
                message_receiver = protocol.STRING.receive()
                request = message_receiver.next()

                while isinstance(request, protocol.Request):
                    value = yield request
                    request = message_receiver.send(value)

                if not isinstance(request, protocol.Result):
                    raise TypeError

                message = request.value

                if success in errors.ERROR_MAP:
                    raise errors.ERROR_MAP[success](message)
                else:
                    raise errors.ArakoonError(
                        'Unknown error code 0x%x, server said: %s' % \
                            (success, message))

        yield protocol.Result(result)


class FlushStore(protocol.Message):
    '''"flush_store" message'''

    __slots__ = ()

    TAG = 0x0042 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = protocol.UNIT

    DOC = utils.format_doc('''
        Send a "flush_store" command to the server

        This method instructs a node to flush its store to disk.
    ''')

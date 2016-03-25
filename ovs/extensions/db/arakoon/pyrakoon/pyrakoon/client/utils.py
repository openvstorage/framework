# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010, 2013, 2014 Incubaid BVBA
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

'''Utility functions for building client mixins'''

import functools

from ovs.extensions.db.arakoon.pyrakoon.pyrakoon import protocol, utils

def validate_types(specs, args):
    '''Validate method call argument types

    :param specs: Spec of expected types
    :type specs: iterable of `(str, pyrakoon.protocol.Type)`
    :param args: Argument values
    :type args: iterable of :obj:`object`

    :raise TypeError: Type of an argument is invalid
    :raise ValueError: Value of an argument is invalid
    '''

    for spec, arg in zip(specs, args):
        name, type_ = spec[:2]

        try:
            type_.check(arg)
        except TypeError:
            raise TypeError('Invalid type of argument "%s"' % name)
        except ValueError:
            raise ValueError('Invalid value of argument "%s"' % name)


def call(message_type):
    '''Expose a :class:`~pyrakoon.protocol.Message` as a method on a client

    :note: If the client method has a `consistency` option (i.e.
        :data:`pyrakoon.protocol.CONSISTENCY_ARG` is present in the :attr:`ARGS`
        field of `message_type`), an `allow_dirty`  argument is added 
        automatically, and both are moved to the back.

    :param message_type: Type of the message this method should call
    :type message_type: :class:`type`

    :return: Method which wraps a call to an Arakoon server using given message
        type
    :rtype: `callable`
    '''

    def wrapper(fun):
        '''Decorator helper'''

        has_consistency = False

        # Calculate argspec of final method
        argspec = ['self']
        for arg in message_type.ARGS:
            if arg is protocol.CONSISTENCY_ARG:
                has_consistency = True
                continue

            if len(arg) == 2:
                argspec.append(arg[0])
            elif len(arg) == 3:
                argspec.append((arg[0], arg[2]))
            else:
                raise ValueError

        if has_consistency:
            name, _, default = protocol.CONSISTENCY_ARG
            argspec.append((name, default))

        @utils.update_argspec(*argspec) #pylint: disable=W0142
        @functools.wraps(fun)
        def wrapped(**kwargs): #pylint: disable=C0111
            self = kwargs['self']

            if not self.connected:
                from ovs.extensions.db.arakoon.pyrakoon.pyrakoon import client
                raise client.NotConnectedError('Not connected')

            args = tuple(kwargs[arg[0]] for arg in message_type.ARGS)
            validate_types(message_type.ARGS, args)

            message = message_type(*args) #pylint: disable=W0142

            return self._process(message) #pylint: disable=W0212

        wrapped.__doc__ = message_type.DOC #pylint: disable=W0622

        return wrapped

    return wrapper

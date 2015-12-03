# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010, 2013, 2014 Incubaid BVBA
#
# Licensees holding a valid Incubaid license may use this file in
# accordance with Incubaid's Arakoon commercial license agreement. For
# more information on how to enter into this agreement, please contact
# Incubaid (contact details can be found on www.arakoon.org/licensing).
#
# Alternatively, this file may be redistributed and/or modified under
# the terms of the GNU Affero General Public License version 3, as
# published by the Free Software Foundation. Under this license, this
# file is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# See the GNU Affero General Public License for more details.
# You should have received a copy of the
# GNU Affero General Public License along with this program (file "COPYING").
# If not, see <http://www.gnu.org/licenses/>.

'''Utility functions for building client mixins'''

import functools

from ovs.extensions.db.pyrakoon.pyrakoon import protocol, utils

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

    :note: If the client method has an `allow_dirty` option (i.e.
        :data:`pyrakoon.protocol.ALLOW_DIRTY_ARG` is present in the :attr:`ARGS`
        field of `message_type`), this is automatically moved to the back.

    :param message_type: Type of the message this method should call
    :type message_type: :class:`type`

    :return: Method which wraps a call to an Arakoon server using given message
        type
    :rtype: `callable`
    '''

    def wrapper(fun):
        '''Decorator helper'''

        has_allow_dirty = False

        # Calculate argspec of final method
        argspec = ['self']
        for arg in message_type.ARGS:
            if arg is protocol.ALLOW_DIRTY_ARG:
                has_allow_dirty = True
                continue

            if len(arg) == 2:
                argspec.append(arg[0])
            elif len(arg) == 3:
                argspec.append((arg[0], arg[2]))
            else:
                raise ValueError

        if has_allow_dirty:
            name, _, default = protocol.ALLOW_DIRTY_ARG
            argspec.append((name, default))

        @utils.update_argspec(*argspec) #pylint: disable=W0142
        @functools.wraps(fun)
        def wrapped(**kwargs): #pylint: disable=C0111
            self = kwargs['self']

            if not self.connected:
                from ovs.extensions.db.pyrakoon.pyrakoon import client
                raise client.NotConnectedError('Not connected')

            args = tuple(kwargs[arg[0]] for arg in message_type.ARGS)
            validate_types(message_type.ARGS, args)

            message = message_type(*args) #pylint: disable=W0142

            return self._process(message) #pylint: disable=W0212

        wrapped.__doc__ = message_type.DOC #pylint: disable=W0622

        return wrapped

    return wrapper

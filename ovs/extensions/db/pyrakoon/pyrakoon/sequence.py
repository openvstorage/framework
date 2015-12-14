# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010, 2014 Incubaid BVBA
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

'''Sequence implementation'''

import operator

from pyrakoon import protocol, utils

#pylint: disable=R0903

class Step(object):
    '''A step in a sequence operation'''

    TAG = None
    '''Operation command tag''' #pylint: disable=W0105
    ARGS = None
    '''Argument definition''' #pylint: disable=W0105

    def __init__(self, *args):
        if len(args) != len(self.ARGS):
            raise TypeError('Invalid number of arguments')

        for (_, type_), arg in zip(self.ARGS, args):
            type_.check(arg)

    def serialize(self):
        '''Serialize the operation

        :return: Serialized operation
        :rtype: iterable of :class:`str`
        '''

        for bytes_ in protocol.UINT32.serialize(self.TAG):
            yield bytes_

        for name, type_ in self.ARGS:
            for bytes_ in type_.serialize(getattr(self, name)):
                yield bytes_


class Set(Step):
    '''"Set" operation'''

    TAG = 1
    ARGS = ('key', protocol.STRING), ('value', protocol.STRING),

    def __init__(self, key, value):
        super(Set, self).__init__(key, value)

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'),
        doc=utils.format_doc('''
            Key to set

            :type: :class:`str`
        '''))
    value = property(operator.attrgetter('_value'),
        doc=utils.format_doc('''
            Value to set

            :type: :class:`str`
        '''))

class Delete(Step):
    '''"Delete" operation'''

    TAG = 2
    ARGS = ('key', protocol.STRING),

    def __init__(self, key):
        super(Delete, self).__init__(key)

        self._key = key

    key = property(operator.attrgetter('_key'),
        doc=utils.format_doc('''
            Key to delete

            :type: :class:`str`
        '''))

class Assert(Step):
    '''"Assert" operation'''

    TAG = 8
    ARGS = ('key', protocol.STRING), \
        ('value', protocol.Option(protocol.STRING)),

    def __init__(self, key, value):
        super(Assert, self).__init__(key, value)

        self._key = key
        self._value = value

    key = property(operator.attrgetter('_key'),
        doc=utils.format_doc('''
            Key for which to assert the given value

            :type: :class:`str`
        '''))
    value = property(operator.attrgetter('_value'),
        doc=utils.format_doc('''
            Expected value

            :type: :class:`str` or :data:`None`
        '''))

class AssertExists(Step):
    '''"AssertExists" operation'''

    TAG = 15
    ARGS = ('key', protocol.STRING),

    def __init__(self, key):
        super(AssertExists, self).__init__(key)

        self._key = key

    key = property(operator.attrgetter('_key'),
        doc=utils.format_doc('''
            Key to check

            :type: :class:`str`
        '''))


class Sequence(Step):
    '''"Sequence" operation

    This is a container for a list of other operations.
    '''

    TAG = 5
    ARGS = ()

    def __init__(self, steps):
        super(Sequence, self).__init__()

        self._steps = steps

    steps = property(operator.attrgetter('_steps'),
        doc=utils.format_doc('''
            Sequence steps

            :type: iterable of :class:`Step`
        '''))

    def serialize(self):
        for bytes_ in protocol.UINT32.serialize(self.TAG):
            yield bytes_

        for bytes_ in protocol.UINT32.serialize(len(self.steps)):
            yield bytes_

        for step in self.steps:
            for bytes_ in step.serialize():
                yield bytes_

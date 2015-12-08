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

'''Exceptions raised by client operations, as returned by a node'''

import inspect

class ArakoonError(Exception):
    '''Base type for all Arakoon client errors'''

    CODE = None
    '''Error code sent by the Arakoon server''' #pylint: disable=W0105

class NoMagic(ArakoonError):
    '''Server received a command without the magic mask'''

    CODE = 0x0001

class TooManyDeadNodes(ArakoonError):
    '''Too many nodes in the cluster are unavailable to process the request'''

    CODE = 0x0002

class NoHello(ArakoonError):
    '''No *Hello* message was sent to the server after connecting'''

    CODE = 0x0003

class NotMaster(ArakoonError):
    '''This node is not a master node'''

    CODE = 0x0004

class NotFound(KeyError, ArakoonError): #pylint: disable=R0901
    '''Key not found'''

    CODE = 0x0005

class WrongCluster(ValueError, ArakoonError):
    '''Wrong cluster ID passed'''

    CODE = 0x0006

class AssertionFailed(ArakoonError):
    '''Assertion failed'''

    CODE = 0x0007

class ReadOnly(ArakoonError):
    '''Node is read-only'''

    CODE = 0x0008

class OutsideInterval(ValueError, ArakoonError):
    '''Request outside interval handled by node'''

    CODE = 0x0009

class GoingDown(ArakoonError):
    '''Node is going down'''

    CODE = 0x0010

class NotSupported(ArakoonError):
    '''Unsupported operation'''

    CODE = 0x0020

class NoLongerMaster(ArakoonError):
    '''No longer master'''

    CODE = 0x0021

class InconsistentRead(ArakoonError):
    '''Inconsistent read'''

    CODE = 0x0080

class MaxConnections(ArakoonError):
    '''Connection limit reached'''

    CODE = 0x00fe

class UnknownFailure(ArakoonError):
    '''Unknown failure'''

    CODE = 0x00ff


ERROR_MAP = dict((value.CODE, value) for value in globals().itervalues()
    if inspect.isclass(value)
        and issubclass(value, ArakoonError)
        and value.CODE is not None)
'''Map of Arakoon error codes to exception types''' #pylint: disable=W0105

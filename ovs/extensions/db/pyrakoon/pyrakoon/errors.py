# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2010 Incubaid BVBA
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

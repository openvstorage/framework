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

'''Compatibility layer for the original Arakoon Python client'''

import os
import time
import random
import select
import socket
import functools
import threading
import inspect
import operator
import ssl

from ovs.extensions.db.arakoon.pyrakoon.pyrakoon import client, consistency, errors, protocol, sequence, utils
from ovs.log.logHandler import LogHandler

__docformat__ = 'epytext'

#pylint: skip-file

#pylint: disable=C0111,W0142,R0912,C0103,W0212,R0913,W0201,W0231,R0903
#pylint: disable=W0223,R0201,W0703,E1121,R0904

# C0111: Missing docstring
# W0142: Used * or ** magic
# R0912: Too many branches
# C0103: Invalid name
# W0212: Access to a protected member
# R0913: Too many arguments
# W0201: Attributed defined outside __init__
# W0231: __init__ method from base class X is not called
# R0903: Too few public methods
# W0223: Method X is abstract in class Y but not overridden
# R0201: Method could be a function
# W0703: Catch "Exception"
# E1121: Too many positional arguments for function call
# R0904: Too many public methods

LOGGER = LogHandler.get('arakoon_client', 'pyrakoon', propagate=False)

class Consistency:
    pass

class Consistent(Consistency):
    def __str__(self):
        return 'Consistent'

class NoGuarantee(Consistency):
    def __str__(self):
        return 'NoGuarantee'

class AtLeast(Consistency):
    def __init__(self,i):
        self.i = i

    def __str__(self):
        return 'AtLeast(%i)' % self.i



def _validate_signature_helper(fun, *args):
    param_native_type_mapping = {
        'int': int,
        'string': str,
        'bool': bool,
    }

    def validate(arg, arg_type):
        r = False
        if arg_type in param_native_type_mapping:
            r =  isinstance(arg, param_native_type_mapping[arg_type])
        elif arg_type == 'string_option':
            r =  isinstance(arg, str) or arg is None
        elif arg_type == 'string_list':
            r =  all(isinstance(value, str) for value in arg)
        elif arg_type == 'sequence':
            r =  isinstance(arg, Sequence)
        elif arg_type == 'consistency_option':
            r = isinstance(arg, Consistency) or arg is None
        elif arg_type == 'consistency':
            r =  isinstance(arg, Consistency)
        else:
            raise RuntimeError('Invalid argument type supplied: %s' % arg_type)
        return r

    @functools.wraps(fun)
    def wrapped(**kwargs):
        new_args = [None] * (len(args) + 1)
        missing_args = inspect.getargs(fun.func_code).args

        for (idx, missing_arg) in enumerate(missing_args):
            if missing_arg in kwargs:
                new_args[idx] = kwargs[missing_arg]
                del kwargs[missing_arg]

        if kwargs:
            raise ArakoonInvalidArguments(fun.func_name,
                list(kwargs.iteritems()))

        i = 0
        error_key_values = []

        for arg, arg_type in zip(new_args[1:], args):
            if not validate(arg, arg_type):
                error_key_values.append(
                    (fun.func_code.co_varnames[i + 1], new_args[i]))
            i += 1

        if error_key_values:
            raise ArakoonInvalidArguments(fun.func_name, error_key_values)

        return fun(*new_args)

    return wrapped

_validate_signature = lambda *args: lambda fun: \
    _validate_signature_helper(fun, *args)


def _convert_exceptions(fun):
    '''
    Wrap a function to convert `pyrakoon` exceptions into suitable
    `ArakoonException` instances
    '''

    @functools.wraps(fun)
    def wrapped(*args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except Exception, exc:
            new_exception = _convert_exception(exc)

            if new_exception is exc:
                raise

            raise new_exception

    return wrapped



    

class ArakoonClient(object):
    def __init__(self, config):
        """
        Constructor of an Arakoon client object.

        It takes one optional paramater 'config'.
        This parameter contains info on the arakoon server nodes.
        See the constructor of L{ArakoonClientConfig} for more details.

        @type config: L{ArakoonClientConfig}
        @param config: The L{ArakoonClientConfig} object to be used by the client. Defaults to None in which
            case a default L{ArakoonClientConfig} object will be created.
        """

        self._client = _ArakoonClient(config)

        # Keep a reference, for compatibility reasons
        self._config = config
        self._consistency = Consistent()

    def _initialize(self, config):
        raise NotImplementedError

    def _determine_consistency(self, consistency_):
        c = self._consistency
        if consistency_:
            c = consistency_

        if isinstance(c, Consistent):
            c = consistency.CONSISTENT
        elif isinstance(c,NoGuarantee):
            c = consistency.INCONSISTENT
        elif isinstance(c,AtLeast):
            c = consistency.AtLeast(c.i)
        else:
            raise ValueError('consistency')
        return c

    @utils.update_argspec('self', 'clientId', ('clusterId', 'arakoon'))
    @_convert_exceptions
    @_validate_signature('string', 'string')
    def hello(self, clientId, clusterId='arakoon'):
        """
        Send a string of your choosing to the server.

        Will return the server node identifier and the version of arakoon it is running

        @type clientId  : string
        @type clusterId : string
        @param clusterId : must match the cluster_id of the node

        @rtype: string
        @return: The master identifier and its version in a single string
        """

        return self._client.hello(clientId, clusterId)

    @utils.update_argspec('self', 'key', ('consistency', None))
    @_convert_exceptions
    @_validate_signature('string', 'consistency_option')
    def exists(self, key, consistency = None):
        """
        @type key : string
        @param key : key
        @return : True if there is a value for that key, False otherwise
        """

        return self._client.exists(key, consistency = consistency)

    @utils.update_argspec('self', 'key', ('consistency', None))
    @_convert_exceptions
    @_validate_signature('string', 'consistency_option')
    def get(self, key, consistency = None):
        """
        Retrieve a single value from the store.

        Retrieve the value associated with the given key

        @type key: string
        @param key: The key whose value you are interested in

        @rtype: string
        @return: The value associated with the given key
        """

        consistency_ = self._determine_consistency(consistency)
        return self._client.get(key,consistency = consistency_)
    

    @utils.update_argspec('self', 'key', 'value')
    @_convert_exceptions
    @_validate_signature('string', 'string')
    def set(self, key, value):
        """
        Update the value associated with the given key.

        If the key does not yet have a value associated with it, a new key value pair will be created.
        If the key does have a value associated with it, it is overwritten.
        For conditional value updates see L{testAndSet}

        @type key: string
        @type value: string
        @param key: The key whose associated value you want to update
        @param value: The value you want to store with the associated key

        @rtype: void
        """

        return self._client.set(key, value)

    @utils.update_argspec('self', 'seq', ('sync', False))
    @_convert_exceptions
    @_validate_signature('sequence', 'bool')
    def sequence(self, seq, sync=False):
        """
        Try to execute a sequence of updates.

        It's all-or-nothing: either all updates succeed, or they all fail.
        @type seq: Sequence
        """

        def convert_set(step):
            return sequence.Set(step._key, step._value)

        def convert_delete(step):
            return sequence.Delete(step._key)

        def convert_assert(step):
            return sequence.Assert(step._key, step._value)

        def convert_assert_exists(step):
            return sequence.AssertExists(step._key)

        def convert_replace(step):
            return sequence.Replace(step._key, step._wanted)

        def convert_sequence(sequence_):
            steps = []

            for step in sequence_._updates:
                if isinstance(step, Set):
                    steps.append(convert_set(step))
                elif isinstance(step, Delete):
                    steps.append(convert_delete(step))
                elif isinstance(step, Assert):
                    steps.append(convert_assert(step))
                elif isinstance(step, AssertExists):
                    steps.append(convert_assert_exists(step))
                elif isinstance(step, Sequence):
                    steps.append(convert_sequence(step))
                elif isinstance(step, Replace):
                    steps.append(convert_replace(step))
                else:
                    raise TypeError

            return sequence.Sequence(steps)

        #pylint: disable=E1123
        return self._client.sequence((convert_sequence(seq), ), sync=sync)

    @utils.update_argspec('self', 'key')
    @_convert_exceptions
    @_validate_signature('string')
    def delete(self, key):
        """
        Remove a key-value pair from the store.

        @type key: string
        @param key: Remove this key and its associated value from the store

        @rtype: void
        """

        return self._client.delete(key)

    __setitem__ = set
    __getitem__ = get
    __delitem__ = delete
    __contains__ = exists

    @utils.update_argspec('self', 'beginKey', 'beginKeyIncluded', 'endKey',
        'endKeyIncluded', ('maxElements', -1))
    @_convert_exceptions
    @_validate_signature('string_option', 'bool', 'string_option', 'bool',
        'int')
    def range(self, beginKey, beginKeyIncluded, endKey, endKeyIncluded,
        maxElements=-1):
        """
        Perform a range query on the store, retrieving the set of matching keys

        Retrieve a set of keys that lexographically fall between the beginKey and the endKey
        You can specify whether the beginKey and endKey need to be included in the result set
        Additionaly you can limit the size of the result set to maxElements. Default is to return all matching keys.

        @type beginKey: string option
        @type beginKeyIncluded: boolean
        @type endKey :string option
        @type endKeyIncluded: boolean
        @type maxElements: integer
        @param beginKey: Lower boundary of the requested range
        @param beginKeyIncluded: Indicates if the lower boundary should be part of the result set
        @param endKey: Upper boundary of the requested range
        @param endKeyIncluded: Indicates if the upper boundary should be part of the result set
        @param maxElements: The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.

        @rtype: list of strings
        @return: Returns a list containing all matching keys
        """
        consistency_ = self._determine_consistency(self._consistency)
        result = self._client.range(beginKey, beginKeyIncluded,
                                    endKey, endKeyIncluded,
                                    maxElements,
                                    consistency = consistency_)
        

        return result

    @utils.update_argspec('self', 'beginKey', 'beginKeyIncluded', 'endKey',
        'endKeyIncluded', ('maxElements', -1))
    @_convert_exceptions
    @_validate_signature('string_option', 'bool', 'string_option', 'bool',
        'int')
    def range_entries(self, beginKey, beginKeyIncluded, endKey, endKeyIncluded,
        maxElements=-1):
        """
        Perform a range query on the store, retrieving the set of matching key-value pairs

        Retrieve a set of keys that lexographically fall between the beginKey and the endKey
        You can specify whether the beginKey and endKey need to be included in the result set
        Additionaly you can limit the size of the result set to maxElements. Default is to return all matching keys.

        @type beginKey: string option
        @type beginKeyIncluded: boolean
        @type endKey :string option
        @type endKeyIncluded: boolean
        @type maxElements: integer
        @param beginKey: Lower boundary of the requested range
        @param beginKeyIncluded: Indicates if the lower boundary should be part of the result set
        @param endKey: Upper boundary of the requested range
        @param endKeyIncluded: Indicates if the upper boundary should be part of the result set
        @param maxElements: The maximum number of key-value pairs to return. Negative means no maximum, all matches will be returned. Defaults to -1.

        @rtype: list of strings
        @return: Returns a list containing all matching key-value pairs
        """
        consistency_ = self._determine_consistency(self._consistency)
        result = self._client.range_entries(beginKey, beginKeyIncluded,
                                            endKey, endKeyIncluded,
                                            maxElements,
                                            consistency = consistency_)

        return result

    @utils.update_argspec('self', 'keyPrefix', ('maxElements', -1))
    @_convert_exceptions
    @_validate_signature('string', 'int')
    def prefix(self, keyPrefix, maxElements=-1):
        """
        Retrieve a set of keys that match with the provided prefix.

        You can indicate whether the prefix should be included in the result set if there is a key that matches exactly
        Additionaly you can limit the size of the result set to maxElements

        @type keyPrefix: string
        @type maxElements: integer
        @param keyPrefix: The prefix that will be used when pattern matching the keys in the store
        @param maxElements: The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.

        @rtype: list of strings
        @return: Returns a list of keys matching the provided prefix
        """
        consistency_ = self._determine_consistency(self._consistency)
        result = self._client.prefix(keyPrefix, maxElements, consistency = consistency_)

        return result

    @utils.update_argspec('self')
    @_convert_exceptions
    def whoMaster(self):
        self._client.determine_master()
        return self._client.master_id

    @utils.update_argspec('self', 'key', 'oldValue', 'newValue')
    @_convert_exceptions
    @_validate_signature('string', 'string_option', 'string_option')
    def testAndSet(self, key, oldValue, newValue):
        """
        Conditionaly update the value associcated with the provided key.

        The value associated with key will be updated to newValue if the current value in the store equals oldValue
        If the current value is different from oldValue, this is a no-op.
        Returns the value that was associated with key in the store prior to this operation. This way you can check if the update was executed or not.

        @type key: string
        @type oldValue: string option
        @type newValue: string
        @param key: The key whose value you want to updated
        @param oldValue: The expected current value associated with the key.
        @param newValue: The desired new value to be stored.

        @rtype: string
        @return: The value that was associated with the key prior to this operation
        """

        return self._client.test_and_set(key, oldValue, newValue)

    @utils.update_argspec('self', 'keys')
    @_convert_exceptions
    @_validate_signature('string_list')
    def multiGet(self, keys):
        """
        Retrieve the values for the keys in the given list.

        @type keys: string list
        @rtype: string list
        @return: the values associated with the respective keys
        """
        consistency_ = self._determine_consistency(self._consistency)
        return self._client.multi_get(keys, consistency = consistency_)

    @utils.update_argspec('self', 'keys')
    @_convert_exceptions
    @_validate_signature('string_list', )
    def multiGetOption(self, keys):
        """
        Retrieve the values for the keys in the given list.

        @type keys: string list
        @rtype: string option list
        @return: the values associated with the respective keys (None if no value corresponds)
        """
        consistency_ = self._determine_consistency(self._consistency)
        return self._client.multi_get_option(keys, consistency = consistency_)

    @utils.update_argspec('self')
    @_convert_exceptions
    def expectProgressPossible(self):
        """
        @return: true if the master thinks progress is possible, false otherwise
        """

        try:
            message = protocol.ExpectProgressPossible()
            return self._client._process(message, retry = False)
        except ArakoonException:
            return False

    @utils.update_argspec('self')
    @_convert_exceptions
    def getKeyCount(self):
        """
        Retrieve the number of keys in the database on the master

        @rtype: int
        """

        return self._client.get_key_count()

    @utils.update_argspec('self', 'name', 'argument')
    @_convert_exceptions
    def userFunction(self, name, argument):
        '''Call a user-defined function on the server
        @param name: Name of user function
        @type name: string
        @param argument: Optional function argument
        @type argument: string option

        @return: Function result
        @rtype: string option
        '''

        return self._client.user_function(name, argument)

    @utils.update_argspec('self', 'key', 'value')
    @_convert_exceptions
    def confirm(self, key, value):
        """
        Do nothing if the value associated with the given key is this value;
        otherwise, behave as set(key,value)
        @rtype: void
        """

        self._client.confirm(key, value)

    @utils.update_argspec('self', 'key', 'vo')
    @_convert_exceptions
    def aSSert(self, key, vo):
        """
        verifies the value for key to match vo
        @type key: string
        @type vo: string_option
        @param key: the key to be verified
        @param vo: what the value should be (can be None)
        @rtype: void
        """

        self._client.assert_(key, vo)

    @utils.update_argspec('self', 'key')
    @_convert_exceptions
    def aSSert_exists(self, key):
        return self._client.assert_exists(key)

    @utils.update_argspec('self', 'beginKey', 'beginKeyIncluded', 'endKey',
        'endKeyIncluded', ('maxElements', -1))
    @_convert_exceptions
    def rev_range_entries(self,
                          beginKey, beginKeyIncluded,
                          endKey,  endKeyIncluded,
                          maxElements= -1):
        """
        Performs a reverse range query on the store, returning a sorted (in reverse order) list of key value pairs.
        @type beginKey: string option
        @type endKey :string option
        @type beginKeyIncluded: boolean
        @type endKeyIncluded: boolean
        @type maxElements: integer
        @param beginKey: higher boundary of the requested range
        @param endKey: lower boundary of the requested range
        @param maxElements: maximum number of key-value pairs to return. Negative means 'all'. Defaults to -1
        @rtype : list of (string,string)
        """
        consistency_ = self._determine_consistency(self._consistency)
        result = self._client.rev_range_entries(beginKey, beginKeyIncluded,
                                                endKey, endKeyIncluded,
                                                maxElements, consistency = consistency_)

        return result

    @utils.update_argspec('self')
    @_convert_exceptions
    def statistics(self):
        """
        @return a dictionary with some statistics about the master
        """

        return self._client.statistics()


    @utils.update_argspec('self', ('nodeId', None))
    @_convert_exceptions
    def getVersion(self, nodeId = None):
        """
        will return a tuple containing major, minor and patch level versions of the server side

        Note: The nodeId argument is currently not supported

        @type nodeId : String
        @param nodeId : id of the node you want to query (None if you want to query the master)
        @rtype : (int,int,int,string)
        @return : (major, minor, patch, info)
        """
        message = protocol.Version()
        r = self._client._process(message, node_id = nodeId)
        return r
        

    @utils.update_argspec('self')
    @_convert_exceptions
    def nop(self):
        """
        a nop is a paxos update that changes nothing to the database
        """

        return self._client.nop()

    @utils.update_argspec('self', 'nodeId')
    @_convert_exceptions
    def getCurrentState(self, nodeId):
        message = protocol.GetCurrentState()
        r = self._client._process(message, node_id = nodeId)
        return r

    @utils.update_argspec('self', 'key', 'wanted')
    @_convert_exceptions
    def replace(self, key, wanted):
        """
        assigns the wanted value to the key, and returns the previous assignment (if any) for that key.
        If wanted is None, the binding is deleted.
        @type key:string
        @type wanted: string option
        @rtype: string option
        @return: the previous binding (if any)
        """

        return self._client.replace(key,wanted)

    @utils.update_argspec('self','prefix')
    @_convert_exceptions
    def deletePrefix(self, prefix):
        """
        type prefix: string
        """

        return self._client.delete_prefix(prefix)

    @utils.update_argspec('self')
    @_convert_exceptions
    def get_txid(self):
        _res = self._client.get_tx_id()
        res = None
        if _res is consistency.CONSISTENT:
            res = Consistent()
        elif _res is consistency.INCONSISTENT:
            res = NoGuarantee()
        elif isinstance(_res, consistency.AtLeast):
            res = AtLeast(_res.i)
        else:
            raise ValueError('Unknown result: %r' % res)
        return res

    @utils.update_argspec('self','c')
    @_convert_exceptions
    @_validate_signature('consistency')
    def setConsistency(self, c):
        """
        Allows fine grained consistency constraints on subsequent reads
        @type c: `Consistency`
        """
        self._consistency = c

    @utils.update_argspec('self')
    @_convert_exceptions
    def allowDirtyReads(self):
        """
        Allow the client to read values from a slave or a node in limbo
        """
        self._consistency = NoGuarantee()

    def disallowDirtyReads(self):
        """
        Force the client to read from the master
        """
        self._consistency = Consistent()
    
    def makeSequence(self):
        return Sequence()

    def dropConnections(self):
        return self._client.drop_connections()

    _masterId = property(
        lambda self:self._client.master_id,
        lambda self, v: setattr(self._client, 'master_id', v))

# Exception types
# This is mostly a copy from the ArakoonExceptions module, with some cosmetic
# changes and some code simplifications

class ArakoonException(Exception):
    _msg = None

    def __init__(self, msg=''):
        if self._msg is not None and msg == '':
            msg = self._msg

        Exception.__init__(self, msg)

class ArakoonNotFound(ArakoonException, KeyError):
    _msg = 'Key not found'

class ArakoonUnknownNode(ArakoonException):
    _msgF = 'Unknown node identifier: %s'

    def __init__(self, nodeId):
        self._msg = ArakoonUnknownNode._msgF % nodeId

        ArakoonException.__init__(self, self._msg)

class ArakoonNodeNotLocal(ArakoonException):
    _msgF = "Unknown node identifier: %s"

    def __init__(self, nodeId):
        self._msg = ArakoonNodeNotLocal._msgF % nodeId

        ArakoonException.__init__(self, self._msg)

class ArakoonNotConnected(ArakoonException):
    _msgF = 'No connection available to node at \'%s:%s\''

    def __init__(self, location):
        self._msg = ArakoonNotConnected._msgF % location

        ArakoonException.__init__(self, self._msg)

class ArakoonNoMaster(ArakoonException):
    _msg = 'Could not determine the Arakoon master node'

class ArakoonNoMasterResult(ArakoonException):
    _msg = 'Master could not be contacted.'

class ArakoonNodeNotMaster(ArakoonException):
    _msg = 'Cannot perform operation on non-master node'

class ArakoonNodeNoLongerMaster(ArakoonException):
    _msg = '''
    Operation might or might not have been performed on node which is no longer master
    '''.strip()

class ArakoonGoingDown(ArakoonException):
    _msg = 'Server is going down'

class ArakoonSocketException(ArakoonException):
    pass

class ArakoonSockReadNoBytes(ArakoonException):
    _msg = 'Could not read a single byte from the socket. Aborting.'

class ArakoonSockNotReadable(ArakoonSocketException):
    _msg = 'Socket is not readable. Aborting.'

class ArakoonSockRecvError(ArakoonSocketException):
    _msg = 'Error while receiving data from socket'

class ArakoonSockRecvClosed(ArakoonSocketException):
    _msg = 'Cannot receive on a not-connected socket'

class ArakoonSockSendError(ArakoonSocketException):
    _msg = 'Error while sending data on socket'

class ArakoonInvalidArguments(ArakoonException, TypeError):
    _msgF = 'Invalid argument(s) for %s: %s'

    def __init__ (self, fun_name, invalid_args):
        # Allow passing single argument, used by _convert_exception
        if not invalid_args:
            ArakoonException.__init__(self, fun_name)
            return

        error_string = ', '.join('%s=%s' % arg for arg in invalid_args)

        self._msg = ArakoonInvalidArguments._msgF % (fun_name, error_string)

        ArakoonException.__init__(self, self._msg)

class ArakoonAssertionFailed(ArakoonException):
    _msg = 'Assert did not yield expected result'


def _convert_exception(exc):
    '''Convert an exception to a suitable `ArakoonException`

    This function converts several types of `errors.ArakoonError` instances
    into `ArakoonException` instances, for compatibility reasons.

    If no suitable conversion can be performed, the original exception is
    returned.

    :param exc: Exception to convert
    :type exc: `object`

    :return: New exception
    :rtype: `object`
    '''

    if isinstance(exc, errors.NotFound):
        exc_ = ArakoonNotFound(exc.message)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, errors.NotMaster):
        exc_ = ArakoonNodeNotMaster(exc.message)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, (TypeError, ValueError)):
        exc_ = ArakoonInvalidArguments(exc.message, None)
        LOGGER.exception(exc)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, errors.AssertionFailed):
        exc_ = ArakoonAssertionFailed(exc.message)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, errors.ArakoonError):
        exc_ = ArakoonException(exc.message)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, errors.NoLongerMaster):
        exc_ = ArakoonNodeNoLongerMaster(exc.message)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, errors.GoingDown):
        exc_ = ArakoonGoingDown(exc.message)
        exc_.inner = exc
        return exc_
    elif isinstance(exc, errors.ReadOnly):
        exc_ = ArakoonException(exc.message)
        exc_.inner = exc
        return exc_
    else:
        return exc


# Sequence type definitions
class Update(object):
    def write(self, fob):
        raise NotImplementedError

class Set(Update):
    def __init__(self, key, value):
        self._key = key
        self._value = value

class Delete(Update):
    def __init__(self, key):
        self._key = key

class Assert(Update):
    def __init__(self, key, value):
        self._key = key
        self._value = value

class AssertExists(Update):
    def __init__(self, key):
        self._key = key

class Replace(Update):
    def __init__(self, key, wanted):
        self._key = key
        self._wanted = wanted

class Sequence(Update):
    def __init__(self):
        self._updates = []

    def addUpdate(self, u):
        self._updates.append(u)

    @utils.update_argspec('self', 'key', 'value')
    @_validate_signature('string', 'string')
    def addSet(self, key, value):
        self._updates.append(Set(key, value))

    @utils.update_argspec('self', 'key')
    @_validate_signature('string')
    def addDelete(self, key):
        self._updates.append(Delete(key))

    def addAssert(self, key, value):
        self._updates.append(Assert(key, value))

    @utils.update_argspec('self', 'key')
    @_validate_signature('string')
    def addAssertExists(self, key):
        self._updates.append(AssertExists(key))

    @utils.update_argspec('self', 'key', 'wanted')
    @_validate_signature('string', 'string_option')
    def addReplace(self, key, wanted):
        self._updates.append(Replace(key, wanted))

# ArakoonClientConfig
# This is copied from the ArakoonProtocol module
ARA_CFG_TRY_CNT = 1
ARA_CFG_CONN_TIMEOUT = 60
ARA_CFG_CONN_BACKOFF = 5
ARA_CFG_NO_MASTER_RETRY = 60

class ArakoonClientConfig :

    def __init__ (self, clusterId, nodes,
                  tls = False, tls_ca_cert = None, tls_cert = None):
        """
        Constructor of an ArakoonClientConfig object

        The constructor takes one optional parameter 'nodes'.
        This is a dictionary containing info on the arakoon server nodes. It contains:

          - nodeids as keys
          - ([ip], port) as values
        e.g. ::
            cfg = ArakoonClientConfig ( 'ricky', 
                { "myFirstNode" : ( ["127.0.0.1"], 4000 ),
                  "mySecondNode" : (["127.0.0.1", "192.168.0.1"], 5000 ) ,
                  "myThirdNode"  : (["127.0.0.1"], 6000 ) 
                })

        Note: This client only supports TLSv1 when connecting to nodes,
        due to Python 2.x. 

        @type clusterId: string
        @param clusterId: name of the cluster
        @type nodes: dict
        @param nodes: A dictionary containing the locations for the server nodes
        @param tls: Use a TLS connection
            If `tls_ca_cert` is given, this *must* be `True`, otherwise
            a `ValueError` will be raised
        @type tls: 'bool'
        @param tls_cert: Path of client certificate & key files
            These should be passed as a tuple. When provided, `tls_ca_cert` 
            *must* be provided as well, otherwise a `ValueError` will be raised.
        @type tls_cert: '(str, str)'
        """
        self._clusterId = clusterId

        sanitize = lambda s: \
                   s if not isinstance(s,str) \
                   else [a.strip() for a in s.split(',')]
        nodes = dict(
            (node_id, (sanitize(addr), port))
            for (node_id, (addr, port)) in nodes.iteritems())
        
        self._nodes = nodes

        if tls_ca_cert and not tls:
            raise ValueError('tls_ca_cert passed, but tls is False')
        if tls_cert and not tls_ca_cert:
            raise ValueError('tls_cert passed, but tls_ca_cert not given')

        if tls_ca_cert is not None and not os.path.isfile(tls_ca_cert):
            raise ValueError('Invalid TLS CA cert path: %s' % tls_ca_cert)

        if tls_cert:
            cert, key = tls_cert
            if not os.path.isfile(cert):
                raise ValueError('Invalid TLS cert path: %s' % cert)
            if not os.path.isfile(key):
                raise ValueError('Invalid TLS key path: %s' % key)

        self._tls = tls
        self._tls_ca_cert = tls_ca_cert
        self._tls_cert = tls_cert

    tls = property(operator.attrgetter('_tls'))
    tls_ca_cert = property(operator.attrgetter('_tls_ca_cert'))
    tls_cert = property(operator.attrgetter('_tls_cert'))
    
    @staticmethod
    def getNoMasterRetryPeriod() :
        """
        Retrieve the period messages to the master should be retried when a master re-election occurs

        This period is specified in seconds

        @rtype: integer
        @return: Returns the retry period in seconds
        """
        return ARA_CFG_NO_MASTER_RETRY

    def getNodeLocations(self, nodeId):
        return self._nodes[nodeId]
    
    def getNodeLocation(self, nodeId):
        """
        Retrieve location of the server node with give node identifier

        A location is a pair consisting of a hostname or ip address as first element.
        The second element of the pair is the tcp port

        @type nodeId: string
        @param nodeId: The node identifier whose location you are interested in

        @rtype: pair(string,int)
        @return: Returns a pair with the nodes hostname or ip and the tcp port, e.g. ("127.0.0.1", 4000)
        """
        ips, port = self.getNodeLocations(nodeId)
        return (ips[0], port)


    def getTryCount (self):
        """
        Retrieve the number of attempts a message should be tried before giving up

        Can be controlled by changing the global variable L{ARA_CFG_TRY_CNT}

        @rtype: integer
        @return: Returns the max retry count.
        """
        return ARA_CFG_TRY_CNT


    def getNodes(self):
        """
        Retrieve the dictionary with node locations
        @rtype: dict
        @return: Returns a dictionary mapping the node identifiers (string) to its location ( pair<string,integer> )
        """
        return self._nodes


    @staticmethod
    def getConnectionTimeout():
        """
        Retrieve the tcp connection timeout

        Can be controlled by changing the global variable L{ARA_CFG_CONN_TIMEOUT}

        @rtype: integer
        @return: Returns the tcp connection timeout
        """
        return ARA_CFG_CONN_TIMEOUT

    @staticmethod
    def getBackoffInterval():
        """
        Retrieves the backoff interval.

        If an attempt to send a message to the server fails,
        the client will wait a random number of seconds. The maximum wait time is n*getBackoffInterVal()
        with n being the attempt counter.
        Can be controlled by changing the global variable L{ARA_CFG_CONN_BACKOFF}

        @rtype: integer
        @return: The maximum backoff interval
        """
        return ARA_CFG_CONN_BACKOFF

    def getClusterId(self):
        return self._clusterId

# Actual client implementation
class _ArakoonClient(object, client.AbstractClient, client.ClientMixin):
    def __init__(self, config, timeout=0):
        self._config = config
        self.master_id = None

        self._lock = threading.RLock()
        self._connections = dict()
        self._timeout = timeout

    @property
    def connected(self):
        return True

    def _process(self, message, node_id = None, retry = True):
        
        bytes_ = ''.join(message.serialize())

        self._lock.acquire()

        try:
            start = time.time()
            tryCount = 0.0
            backoffPeriod = 0.2
            retryPeriod = ArakoonClientConfig.getNoMasterRetryPeriod()
            deadline = start + retryPeriod
            while True:
                try:
                    # Send on wire
                    if node_id is None:
                        connection = self._send_to_master(bytes_)
                    else:
                        connection = self._send_message(node_id, bytes_)
                    return utils.read_blocking(message.receive(),
                        connection.read)
                except (errors.NotMaster,
                        ArakoonNoMaster,
                        ArakoonNotConnected,
                        ArakoonSockReadNoBytes):
                    self.master_id = None
                    self.drop_connections()
                    
                    sleepPeriod = backoffPeriod * tryCount
                    if retry and time.time() + sleepPeriod <= deadline:
                        tryCount += 1.0
                        LOGGER.warning(
                            'Master not found, retrying in %0.2f seconds' % \
                            sleepPeriod)
                        time.sleep(sleepPeriod)
                    else:
                        raise

        finally:
            self._lock.release()

    def _send_message(self, node_id, data, count=-1):
        result = None

        if count < 0:
            count = self._config.getTryCount()

        for i in xrange(count):
            if i > 0:
                max_sleep = i * ArakoonClientConfig.getBackoffInterval()
                time.sleep(random.randint(0, max_sleep))

            self._lock.acquire()
            try:
                connection = self._get_connection(node_id)
                connection.send(data)

                result = connection
                break
            except Exception as e:
                LOGGER.exception('%s : Message exchange with node %s failed',
                                 e, node_id)
                try:
                    self._connections.pop(node_id).close()
                finally:
                    self.master_id = None
            finally:
                self._lock.release()

        if not result:
            raise

        return result

    def _send_to_master(self, data):
        self.determine_master()

        connection = self._send_message(self.master_id, data)

        return connection

    def drop_connections(self):
        for key in tuple(self._connections.iterkeys()):
            self._connections.pop(key).close()

    def determine_master(self):
        node_ids = []

        if self.master_id is None:
            node_ids = self._config.getNodes().keys()
            random.shuffle(node_ids)

            while self.master_id is None and node_ids:
                node = node_ids.pop()

                try:
                    self.master_id = self._get_master_id_from_node(node)
                    tmp_master = self.master_id

                    try:
                        if self.master_id is not None:
                            if self.master_id != node and not \
                                self._validate_master_id(self.master_id):
                                self.master_id = None
                                LOGGER.warning(
                                    'Node "%s" thinks the master is "%s", but actually it isn\'t',
                                    node, tmp_master)
                    except Exception as e:
                        LOGGER.exception(
                            '%s: Unable to validate master on node %s', e, tmp_master)
                        self.master_id = None

                except Exception as e:
                    LOGGER.exception(
                        '%s: Unable to query node "%s" to look up master',e, node)

        if not self.master_id:
            LOGGER.error('Unable to determine master node')
            raise ArakoonNoMaster

    def _get_master_id_from_node(self, node_id):
        command = protocol.WhoMaster()
        data = ''.join(command.serialize())

        connection = self._send_message(node_id, data)

        receiver = command.receive()
        return utils.read_blocking(receiver, connection.read)

    def _validate_master_id(self, master_id):
        if not master_id:
            return False

        other_master_id = self._get_master_id_from_node(master_id)

        return other_master_id == master_id

    def _get_connection(self, node_id):
        connection = None

        if node_id in self._connections:
            connection = self._connections[node_id]

        if not connection:
            node_location = self._config.getNodeLocation(node_id)
            connection = _ClientConnection(node_location,
                                           self._config.getClusterId(),
                                           self._config.tls, self._config.tls_ca_cert,
                                           self._config.tls_cert,
                                           self._timeout)
            connection.connect()

            self._connections[node_id] = connection

        return connection


class _ClientConnection(object):
    def __init__(self, address, cluster_id,
                 tls, tls_ca_cert, tls_cert,
                 timeout = 0):
        self._address = address
        self._connected = False
        self._socket = None
        self._cluster_id = cluster_id
        self._tls = tls
        self._tls_ca_cert = tls_ca_cert
        self._tls_cert = tls_cert
        if (isinstance(timeout, (int, float)) and timeout > 0) or timeout is None:
            self._timeout = timeout
        else:
            self._timeout = ArakoonClientConfig.getConnectionTimeout()

    def connect(self):
        if self._socket:
            self._socket.close()
            self._socket = None

        try:
            self._socket = socket.create_connection(self._address, self._timeout)

            after_idle_sec = 20
            interval_sec = 20
            max_fails = 3
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)
            
            if self._tls:
                kwargs = {
                    'ssl_version': ssl.PROTOCOL_TLSv1,
                    'cert_reqs': ssl.CERT_OPTIONAL,
                    'do_handshake_on_onnect' : True
                }
                
                if self._tls_ca_cert:
                    kwargs['cert_reqs'] = ssl.CERT_REQUIRED
                    kwargs['ca_certs'] = self._tls_ca_cert

                if self._tls_cert:
                    cert, key = self._tls_cert
                    kwargs['keyfile'] = key
                    kwargs['certfile'] = cert

                self._socket = ssl.wap_socket(self._socket, **kwargs)
            
            
            data = protocol.build_prologue(self._cluster_id)
            self._socket.sendall(data)

            self._connected = True
        except Exception as e:
            LOGGER.exception('%s: Unable to connect to %s', e, self._address)

    def send(self, data):
        if not self._connected:
            self.connect()

            if not self._connected:
                raise ArakoonNotConnected(self._address)

        try:
            self._socket.sendall(data)
        except Exception as e:
            LOGGER.exception('%s:Error while sending data to %s', e, self._address)
            self.close()
            raise ArakoonSockSendError

    def close(self):
        if self._connected and self._socket:
            try:
                self._socket.close()
            except Exception as e:
                LOGGER.exception('%s: Error while closing socket to %s',
                                 e,
                                 self._address)
            finally:
                self._connected = False

    def read(self, count):
        if not self._connected:
            raise ArakoonSockRecvClosed

        bytes_remaining = count
        result = []

        if isinstance(self._socket, ssl.SSLSocket):
            pending = self._socket.pending()
            if pending > 0:
                tmp = self._socket.recv(min(bytes_remaining, pending))
                result.append(tmp)
                bytes_remaining = bytes_remaining - len(tmp)
    
        while bytes_remaining > 0:
            reads, _, _ = select.select([self._socket], [], [], self._timeout)

            if self._socket in reads:
                try:
                    data = self._socket.recv(bytes_remaining)
                except Exception as e:
                    LOGGER.exception('%s: Error while reading socket', e)
                    self._connected = False

                    raise ArakoonSockRecvError

                if len(data) == 0:
                    try:
                        self.close()
                    except Exception as e:
                        LOGGER.exception('%s: Error while closing socket', e)

                    self._connected = False

                    raise ArakoonSockReadNoBytes

                result.append(data)
                bytes_remaining -= len(data)

            else:
                try:
                    self.close()
                except Exception as e:
                    LOGGER.exception('%s: Error while closing socket', e)
                finally:
                    self._connnected = False

                raise ArakoonSockNotReadable

        return ''.join(result)

from protocol import admin
class ArakoonAdmin(ArakoonClient):

    @utils.update_argspec('self', 'node_id', 'n')
    @_convert_exceptions
    @_validate_signature('string', 'int')
    def collapse(self, node_id, n):

        """
        Tell the targeted node to collapse tlogs into a head database
        Will return the server node identifier and the version of arakoon it is running

        @type node_id  : string
        @type n : int
        @param node_id : id of targeted node
        """
        message = admin.CollapseTlogs(n)
        x = self._client._timeout
        try:
            self._client._timeout = None # Don't timeout on this call
            self._client._process(message, node_id = node_id)
        finally:
            self._client._timeout = x


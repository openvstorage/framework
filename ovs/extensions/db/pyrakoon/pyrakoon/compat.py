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

import time
import random
import select
import socket
import logging
import functools
import threading

from pyrakoon import client, errors, protocol, sequence, utils

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

LOGGER = logging.getLogger(__name__)

def _add_handler():
    if hasattr(logging, 'NullHandler'):
        handler = logging.NullHandler() #pylint: disable=E1101
    else:
        class NullHandler(logging.Handler):
            def emit(self, record):
                pass

        handler = NullHandler()

    LOGGER.addHandler(handler)

_add_handler()
del _add_handler


def _validate_signature_helper(fun, *args):
    param_native_type_mapping = {
        'int': int,
        'string': str,
        'bool': bool,
    }

    def validate(arg, arg_type):
        if arg_type in param_native_type_mapping:
            return isinstance(arg, param_native_type_mapping[arg_type])
        elif arg_type == 'string_option':
            return isinstance(arg, str) or arg is None
        elif arg_type == 'string_list':
            return all(isinstance(value, str) for value in arg)
        elif arg_type == 'sequence':
            return isinstance(arg, Sequence)
        else:
            raise RuntimeError('Invalid argument type supplied: %s' % arg_type)

    @functools.wraps(fun)
    def wrapped(**kwargs):
        new_args = [None] * (len(args) + 1)
        missing_args = fun.func_code.co_varnames

        for missing_arg in missing_args:
            if missing_arg in kwargs:
                pos = fun.func_code.co_varnames.index(missing_arg)
                new_args[pos] = kwargs[missing_arg]
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

    def _initialize(self, config):
        raise NotImplementedError

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

    @utils.update_argspec('self', 'key')
    @_convert_exceptions
    @_validate_signature('string')
    def exists(self, key):
        """
        @type key : string
        @param key : key
        @return : True if there is a value for that key, False otherwise
        """

        return self._client.exists(key)

    @utils.update_argspec('self', 'key')
    @_convert_exceptions
    @_validate_signature('string')
    def get(self, key):
        """
        Retrieve a single value from the store.

        Retrieve the value associated with the given key

        @type key: string
        @param key: The key whose value you are interested in

        @rtype: string
        @return: The value associated with the given key
        """

        return self._client.get(key)

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

        result = self._client.range(beginKey, beginKeyIncluded, endKey,
            endKeyIncluded, maxElements)

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

        result = self._client.range_entries(beginKey, beginKeyIncluded, endKey,
            endKeyIncluded, maxElements)

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

        result = self._client.prefix(keyPrefix, maxElements)

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

        return self._client.multi_get(keys)

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

        return self._client.multi_get_option(keys)

    @utils.update_argspec('self')
    @_convert_exceptions
    def expectProgressPossible(self):
        """
        @return: true if the master thinks progress is possible, false otherwise
        """

        try:
            return self._client.expect_progress_possible()
        except ArakoonNoMaster:
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

        result = self._client.rev_range_entries(beginKey, beginKeyIncluded,
            endKey, endKeyIncluded, maxElements)

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

        if nodeId:
            raise ValueError('nodeId is not supported')

        return self._client.version()

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
        return self._client.get_current_state() # TODO: not to master, but to node

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


    def makeSequence(self):
        return Sequence()

    def dropConnections(self):
        return self._client.drop_connections()


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

class ArakoonSockReadNoBytes(ArakoonException):
    _msg = 'Could not read a single byte from the socket. Aborting.'

class ArakoonSockNotReadable(ArakoonException):
    _msg = 'Socket is not readable. Aborting.'

class ArakoonSockRecvError(ArakoonException):
    _msg = 'Error while receiving data from socket'

class ArakoonSockRecvClosed(ArakoonException):
    _msg = 'Cannot receive on a not-connected socket'

class ArakoonSockSendError(ArakoonException):
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

# ArakoonClientConfig
# This is copied from the ArakoonProtocol module
ARA_CFG_TRY_CNT = 1
ARA_CFG_CONN_TIMEOUT = 60
ARA_CFG_CONN_BACKOFF = 5
ARA_CFG_NO_MASTER_RETRY = 60

class ArakoonClientConfig :

    def __init__ (self, clusterId, nodes):
        """
        Constructor of an ArakoonClientConfig object

        The constructor takes one optional parameter 'nodes'.
        This is a dictionary containing info on the arakoon server nodes. It contains:

          - nodeids as keys
          - (ips, tcp port) tuples as value
        e.g. ::
            cfg = ArakoonClientConfig ( {
                    "myFirstNode" : ( ["127.0.0.1"], 4000 ),
                    "mySecondNode" : (["127.0.0.1", "192.168.0.1"], 5000 ) ,
                    "myThirdNode"  : (["127.0.0.1"], 6000 ) } )
        Defaults to a single node running on localhost:4000

        @type clusterId: string
        @param clusterId: name of the cluster
        @type nodes: dict
        @param nodes: A dictionary containing the locations for the server nodes

        """
        self._clusterId = clusterId
        self._nodes = nodes

    @staticmethod
    def getNoMasterRetryPeriod() :
        """
        Retrieve the period messages to the master should be retried when a master re-election occurs

        This period is specified in seconds

        @rtype: integer
        @return: Returns the retry period in seconds
        """
        return ARA_CFG_NO_MASTER_RETRY

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
        ips, port = self._nodes[nodeId]
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
    def __init__(self, config):
        self._config = config
        self.master_id = None

        self._lock = threading.RLock()
        self._connections = dict()

    @property
    def connected(self):
        return True

    def _process(self, message):
        bytes_ = ''.join(message.serialize())

        self._lock.acquire()

        try:
            start = time.time()
            tryCount = 0.0
            backoffPeriod = 0.2
            callSucceeded = False
            retryPeriod = ArakoonClientConfig.getNoMasterRetryPeriod()
            deadline = start + retryPeriod

            while not callSucceeded and time.time() < deadline:
                try:
                    # Send on wire
                    connection = self._send_to_master(bytes_)
                    return utils.read_blocking(message.receive(),
                        connection.read)
                except (errors.NotMaster, ArakoonNoMaster):
                    self.master_id = None
                    self.drop_connections()

                    sleepPeriod = backoffPeriod * tryCount
                    if time.time() + sleepPeriod > deadline:
                        raise

                    tryCount += 1.0
                    LOGGER.warning(
                        'Master not found, retrying in %0.2f seconds' % \
                            sleepPeriod)

                    time.sleep(sleepPeriod)

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
            except Exception:
                LOGGER.exception('Message exchange with node %s failed',
                    node_id)
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
                            else:
                                LOGGER.warning(
                                    'Node "%s" doesn\'t know the master',
                                    node)
                    except Exception:
                        LOGGER.exception(
                            'Unable to validate master on node %s', tmp_master)
                        self.master_id = None

                except Exception:
                    LOGGER.exception(
                        'Unable to query node "%s" to look up master', node)

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
                self._config.getClusterId())
            connection.connect()

            self._connections[node_id] = connection

        return connection


class _ClientConnection(object):
    def __init__(self, address, cluster_id):
        self._address = address
        self._connected = False
        self._socket = None
        self._cluster_id = cluster_id

    def connect(self):
        if self._socket:
            self._socket.close()
            self._socket = None

        try:
            self._socket = socket.create_connection(self._address,
                ArakoonClientConfig.getConnectionTimeout())
            self._socket.setblocking(False)

            data = protocol.build_prologue(self._cluster_id)
            self._socket.sendall(data)

            self._connected = True
        except Exception:
            LOGGER.exception('Unable to connect to %s', self._address)

    def send(self, data):
        if not self._connected:
            self.connect()

            if not self._connected:
                raise ArakoonNotConnected(self._address)

        try:
            self._socket.sendall(data)
        except Exception:
            LOGGER.exception('Error while sending data to %s', self._address)
            self.close()
            raise ArakoonSockSendError

    def close(self):
        if self._connected and self._socket:
            try:
                self._socket.close()
            except Exception:
                LOGGER.exception('Error while closing socket to %s',
                    self._address)
            finally:
                self._connected = False

    def read(self, count):
        if not self._connected:
            raise ArakoonSockRecvClosed

        bytes_remaining = count
        result = []
        timeout = ArakoonClientConfig.getConnectionTimeout()

        while bytes_remaining > 0:
            reads, _, _ = select.select([self._socket], [], [], timeout)

            if self._socket in reads:
                try:
                    data = self._socket.recv(bytes_remaining)
                except Exception:
                    LOGGER.exception('Error while reading socket')
                    self._connected = False

                    raise ArakoonSockRecvError

                if len(data) == 0:
                    try:
                        self.close()
                    except Exception:
                        LOGGER.exception('Error while closing socket')

                    self._connected = False

                    raise ArakoonSockReadNoBytes

                result.append(data)
                bytes_remaining -= len(data)

            else:
                try:
                    self.close()
                except Exception:
                    LOGGER.exception('Error while closing socket')
                finally:
                    self._connnected = False

                raise ArakoonSockNotReadable

        return ''.join(result)

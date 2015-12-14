# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2012 Incubaid BVBA
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

'''Arakoon Nursery support'''

#pylint: disable=R0903
# R0903: Too few public methods

import logging
import operator

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

from pyrakoon import protocol, utils


LOGGER = logging.getLogger(__name__)

class NurseryConfigType(protocol.Type):
    '''NurseryConfig type'''

    def check(self, value):
        raise NotImplementedError('NurseryConfig can\'t be checked')

    def serialize(self, value):
        raise NotImplementedError('NurseryConfig can\'t be serialized')

    def receive(self):
        buffer_receiver = protocol.STRING.receive()
        request = buffer_receiver.next() #pylint: disable=E1101

        while isinstance(request, protocol.Request):
            value = yield request
            request = buffer_receiver.send(value) #pylint: disable=E1101

        if not isinstance(request, protocol.Result):
            raise TypeError

        read = StringIO.StringIO(request.value).read

        routing = parse_nursery_routing(read)

        config_count = utils.read_blocking(protocol.UINT32.receive(), read)
        configs = {}

        for _ in xrange(config_count):
            cluster_id = utils.read_blocking(protocol.STRING.receive(), read)
            cluster_size = utils.read_blocking(protocol.UINT32.receive(), read)

            config = {}

            for _ in xrange(cluster_size):
                node_id = utils.read_blocking(protocol.STRING.receive(), read)
                ips = utils.read_blocking(
                    protocol.List(protocol.STRING).receive(), read)
                port = utils.read_blocking(protocol.UINT32.receive(), read)

                config[node_id] = (tuple(ips), port)

            configs[cluster_id] = config

        yield protocol.Result(NurseryConfig(routing, configs))

NURSERY_CONFIG = NurseryConfigType()


class NurseryConfig(object):
    '''Nursery configuration'''

    __slots__ = '_routing', '_clusters',

    def __init__(self, routing, clusters):
        '''Initialize a configuration object

        :param routing: Routing tree
        :type routing: `Node`
        :param clusters: Dictionary of cluster information
        :type clusters: `dict` of `str` to `([str], int)` tuples
        '''

        self._routing = routing
        self._clusters = clusters

    routing = property(operator.attrgetter('_routing'))
    clusters = property(operator.attrgetter('_clusters'))

    def __repr__(self):
        return 'NurseryConfig(%r, %r)' % (self.routing, self.clusters)


def parse_nursery_routing(read):
    '''Parse nursery routing information from a given stream

    :param read: Function returning the requested amount of data
    :type read: `callable` of `int -> str`
    '''

    is_leaf = utils.read_blocking(protocol.BOOL.receive(), read)

    if is_leaf:
        cluster = utils.read_blocking(protocol.STRING.receive(), read)

        return LeafNode(cluster)
    else:
        boundary = utils.read_blocking(protocol.STRING.receive(), read)

        left = parse_nursery_routing(read)
        right = parse_nursery_routing(read)

        return InternalNode(boundary, left, right)


class Node(object):
    '''Base type for nursery routing tree nodes'''

    pass

class LeafNode(Node):
    '''Nursery routing tree leaf node'''

    __slots__ = '_cluster',

    def __init__(self, cluster):
        super(LeafNode, self).__init__()

        self._cluster = cluster

    cluster = property(operator.attrgetter('_cluster'))

    def __repr__(self):
        return 'LeafNode(%r)' % self.cluster

    def __eq__(self, other):
        if not isinstance(other, LeafNode):
            return NotImplemented

        return self.cluster == other.cluster

class InternalNode(Node):
    '''Nursery routing tree internal node'''

    __slots__ = '_boundary', '_left', '_right',

    def __init__(self, boundary, left, right):
        super(InternalNode, self).__init__()

        self._boundary = boundary
        self._left = left
        self._right = right

    boundary = property(operator.attrgetter('_boundary'))
    left = property(operator.attrgetter('_left'))
    right = property(operator.attrgetter('_right'))

    def __repr__(self):
        return 'InternalNode(%r, %r, %r)' % \
            (self.boundary, self.left, self.right)

    def __eq__(self, other):
        if not isinstance(other, InternalNode):
            return NotImplemented

        return self.boundary == other.boundary \
                and self.left == other.left \
                and self.right == other.right


class GetNurseryConfig(protocol.Message):
    '''"get_nursery_config" message'''

    __slots__ = ()

    TAG = 0x0020 | protocol.Message.MASK
    ARGS = ()
    RETURN_TYPE = NURSERY_CONFIG

    DOC = utils.format_doc('''
        Send a "get_nursery_config" command to the server

        This method returns Arakoon Nursery configuration settings.

        :return: Nursery configuration
        :rtype: `NurseryConfig`
    ''')


class NurseryClient(object):
    '''Arakoon nursery client'''

    def __init__(self, process, client_factory):
        self._process = process
        self._client_factory = client_factory
        self._initialized = False
        self._routing = None
        self._clients = {}

    def initialize(self):
        '''Initialize the nursery client

        This method retrieves the nursery configuration from the keeper cluster,
        and creates connections to all nursery clusters.
        '''

        LOGGER.info('Initializing nursery client')

        LOGGER.debug('Requesting nursery configuration')
        config = self._process(GetNurseryConfig())
        LOGGER.debug('Received %r', config)

        LOGGER.debug('Disconnecting clients')
        for client in self._clients.itervalues():
            client.disconnect()

        self._clients = {}

        self._routing = config.routing
        for cluster_name, cluster_config in config.clusters.iteritems():
            LOGGER.debug('Creating client for cluster %r', cluster_name)
            self._clients[cluster_name] = self._client_factory(
                cluster_name, cluster_config)

        self._initialized = True

    def _find_client_for_key(self, key):
        '''Retrieve the client responsible for a given key

        :param key: Key to look up
        :type key: `str`
        '''

        def loop(top):
            '''Recursive function to find the cluster we're looking for

            :param top: Tree node to walk over
            :type top: `LeafNode` or `InternalNode`
            '''

            if isinstance(top, LeafNode):
                return top.cluster

            if isinstance(top, InternalNode):
                if key < top.boundary:
                    return loop(top.left)
                else:
                    return loop(top.right)

            raise TypeError

        cluster = loop(self._routing)

        return self._clients[cluster]

    def get(self, key):
        '''Retrieve a value from the nursery

        :param key: Key of the value to retrieve
        :type key: `str`

        :return: Value of the given key
        :rtype: `str`
        '''

        if not self._initialized:
            self.initialize()

        return self._find_client_for_key(key).get(key)

    def set(self, key, value):
        '''Set a value for the given key

        :param key: Key to set
        :type key: `str`
        :param value: Value to set
        :type value: `str`
        '''

        if not self._initialized:
            self.initialize()

        return self._find_client_for_key(key).set(key, value)

    def delete(self, key):
        '''Delete a key

        :param key: Key to delete
        :type key: `str`
        '''

        if not self._initialized:
            self.initialize()

        return self._find_client_for_key(key).delete(key)

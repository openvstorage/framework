// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.refreshHandle = undefined;
        self.refreshTimeout = undefined;

        // Obserables
        self.nodes = ko.observableArray([]);

        // Functions
        self.refresh = function() {
            if (generic.xhrCompleted(self.refreshHandle)) {
                self.refreshHandle = api.get('statistics/memcache')
                    .done(function(data) {
                        $.each(data.offline, function(index, node) {
                            var node_info, add = false;
                            $.each(self.nodes(), function(oindex, onode) {
                                if (onode.node() === node) {
                                    node_info = onode;
                                }
                            });
                            if (node_info === undefined) {
                                node_info = {
                                    node         : ko.observable(''),
                                    bytes        : ko.observable(''),
                                    currItems    : ko.observable(0),
                                    totalItems   : ko.observable(0),
                                    getHits      : ko.observable(0),
                                    cmdGet       : ko.observable(0),
                                    cmdGetps     : ko.deltaObservable(generic.formatShort),
                                    hitRate      : ko.observable(0),
                                    bytesRead    : ko.observable(''),
                                    bytesWritten : ko.observable(''),
                                    uptime       : ko.observable(0),
                                    raw          : ko.observable(''),
                                    online       : ko.observable(true)
                                };
                                node_info.cmdGetps(0);
                                add = true;
                            }
                            node_info.node(node);
                            node_info.online(false);
                            if (add) {
                                self.nodes.push(node_info);
                            }
                        });
                        $.each(data.nodes, function(index, node) {
                            var node_info, add = false, rawString = '', attribute;
                            $.each(self.nodes(), function(oindex, onode) {
                                if (onode.node() === node.node) {
                                    node_info = onode;
                                }
                            });
                            if (node_info === undefined) {
                                node_info = {
                                    node         : ko.observable(''),
                                    bytes        : ko.observable(''),
                                    currItems    : ko.observable(0),
                                    totalItems   : ko.observable(0),
                                    getHits      : ko.observable(0),
                                    cmdGet       : ko.observable(0),
                                    cmdGetps     : ko.deltaObservable(generic.formatShort),
                                    hitRate      : ko.observable(0),
                                    bytesRead    : ko.observable(''),
                                    bytesWritten : ko.observable(''),
                                    uptime       : ko.observable(0),
                                    raw          : ko.observable(''),
                                    online       : ko.observable(true)
                                };
                                node_info.cmdGetps(0);
                                add = true;
                            }
                            node_info.node(node.node);
                            node_info.online(true);
                            node_info.bytes(generic.formatBytes(node.bytes));
                            node_info.currItems(node.curr_items);
                            node_info.totalItems(node.total_items);
                            node_info.getHits(node.get_hits);
                            node_info.cmdGet(node.cmd_get);
                            node_info.cmdGetps(node.cmd_get);
                            node_info.hitRate(node.cmd_get === 0 ? 100 : generic.round(node.get_hits / node.cmd_get * 100, 2));
                            node_info.bytesRead(generic.formatBytes(node.bytes_read));
                            node_info.bytesWritten(generic.formatBytes(node.bytes_written));
                            node_info.uptime(node.uptime);

                            for (attribute in node) {
                                if (node.hasOwnProperty(attribute)) {
                                    rawString += generic.padRight(attribute, ' ', 25) + node[attribute].toString() + '\n';
                                }
                            }
                            node_info.raw(rawString);

                            if (add) {
                                self.nodes.push(node_info);
                            }
                        });
                    });
            }
        };
    };
});

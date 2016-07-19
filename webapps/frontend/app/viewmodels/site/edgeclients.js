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
/*global define, window */
define([
    'jquery', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/edgeclient'
], function($, dialog, ko, shared, generic, Refresher, api, VDisk, EdgeClient) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared            = shared;
        self.guard             = { authenticated: true };
        self.vDiskCache        = {};
        self.clientLookup      = {};
        self.widgets           = [];
        self.edgeClientHeaders = [
            { key: 'vdisk', value: $.t('ovs:generic.vdisk'), width: undefined },
            { key: 'ip',    value: $.t('ovs:generic.ip'),    width: 200       },
            { key: 'port',  value: $.t('ovs:generic.port'),  width: 100       }
        ];

        // Handles
        self.edgeClientHandle = {};

        // Observables
        self.edgeClients = ko.observableArray([]);

        // Functions
        self.loadEdgeClients = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.edgeClientHandle[options.page])) {
                    options.contents = 'object_id';
                    self.edgeClientHandle[options.page] = api.get('edgeclients', { queryparams: options })
                        .then(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new EdgeClient(guid);
                                },
                                dependencyLoader: function(item) {
                                    var id = item.objectId();
                                    if (!self.vDiskCache.hasOwnProperty(id)) {
                                        self.vDiskCache[id] = undefined;
                                    } else {
                                        item.vDisk(self.vDiskCache[id]);
                                    }
                                }
                            });
                        })
                        .done(function() {
                            var volumes = [], query;
                            $.each(self.vDiskCache, function(objectId, vdisk) {
                                if (vdisk === undefined) {
                                    volumes.push(objectId);
                                }
                            });
                            query = {
                                type: 'AND',
                                items: [['volume_id', 'IN', volumes]]
                            };
                            return api.get('vdisks', { queryparams: { query: JSON.stringify(query), contents: 'name' }})
                                .done(function(data) {
                                    $.each(data.data, function(index, entry) {
                                        if (self.vDiskCache[entry.volume_id] === undefined) {
                                            self.vDiskCache[entry.volume_id] = new VDisk(entry.guid);
                                        }
                                        self.vDiskCache[entry.volume_id].fillData(entry);
                                    });
                                    $.each(self.edgeClients(), function(index, client) {
                                        if (client.vDisk() === undefined) {
                                            client.vDisk(self.vDiskCache[client.objectId()]);
                                        }
                                    })
                                });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
        };
    };
});

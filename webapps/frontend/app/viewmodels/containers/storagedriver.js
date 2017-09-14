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
    'ovs/api', 'ovs/generic'
], function($, ko, api, generic) {
    "use strict";
    return function(guid) {
        var self = this;

        // Observables
        self.albaProxyGuids           = ko.observableArray([]);
        self.backendInfo              = ko.observable();
        self.blockCacheBackendInfo    = ko.observable();
        self.blockCacheConnectionInfo = ko.observable();
        self.connectionInfo           = ko.observable();
        self.guid                     = ko.observable(guid);
        self.loaded                   = ko.observable(false);
        self.loading                  = ko.observable(false);
        self.mountpoint               = ko.observable();
        self.name                     = ko.observable();
        self.ports                    = ko.observableArray([0, 0, 0, 0]);
        self.storageIP                = ko.observable();
        self.storageRouterGuid        = ko.observable();
        self.vdiskGuids               = ko.observableArray([]);
        self.vpoolBackendInfo         = ko.observable();
        self.localSummary             = ko.observable();

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.mountpoint, data, 'mountpoint');
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.ports, data, 'ports');
            generic.trySet(self.storageIP, data, 'storage_ip');
            generic.trySet(self.storageRouterGuid, data, 'storagerouter_guid');
            generic.trySet(self.vdiskGuids, data, 'vdisks_guids');
            generic.trySet(self.vpoolBackendInfo, data, 'vpool_backend_info');
            generic.trySet(self.albaProxyGuids, data, 'alba_proxies_guids');
            generic.trySet(self.localSummary, data, 'local_summary');
            if (data.hasOwnProperty('vpool_backend_info')) {
                generic.trySet(self.backendInfo, data.vpool_backend_info, 'backend_info');
                generic.trySet(self.connectionInfo, data.vpool_backend_info, 'connection_info');
                generic.trySet(self.blockCacheBackendInfo, data.vpool_backend_info, 'block_cache_backend_info');
                generic.trySet(self.blockCacheConnectionInfo, data.vpool_backend_info, 'block_cache_connection_info');
            }
            self.loaded(true);
            self.loading(false);
        };
        self.load = function(contents) {
            var options = {};
            if (contents !== undefined) {
                options.contents = contents;
            }
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.get('storagedrivers/' + self.guid(), {queryparams: options})
                    .done(function(data) {
                        self.fillData(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };

        // Computed
        self.caching = ko.computed(function() {
            if (self.vpoolBackendInfo() === undefined) {
                return 'none'
            }
            var read = self.vpoolBackendInfo().cache_read,
                write = self.vpoolBackendInfo().cache_write;

            if (read === true && write === true) {
                return 'read_and_write';
            }
            if (read === true && write === false) {
                return 'read';
            }
            if (read === false && write === true) {
                return 'write';
            }
            return 'none';
        });
        self.blockCaching = ko.computed(function() {
            if (self.vpoolBackendInfo() === undefined) {
                return 'none'
            }
            var read = self.vpoolBackendInfo().block_cache_read,
                write = self.vpoolBackendInfo().block_cache_write;

            if (read === true && write === true) {
                return 'read_and_write';
            }
            if (read === true && write === false) {
                return 'read';
            }
            if (read === false && write === true) {
                return 'write';
            }
            return 'none';
        });
        self.canBeDeleted = ko.computed(function() {
            return self.vdiskGuids().length === 0;
        });
    };
});

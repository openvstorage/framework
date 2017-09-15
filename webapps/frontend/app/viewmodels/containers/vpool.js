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
    'jquery', 'knockout', 'ovs/shared',
    'ovs/generic', 'ovs/api',
    'viewmodels/containers/backendtype', 'viewmodels/containers/vdisk'
], function($, ko, shared, generic, api, BackendType, VDisk) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared = shared;

        // Handles
        self.loadHandle          = undefined;
        self.diskHandle          = undefined;
        self.storageRouterHandle = undefined;

        // Observables
        self.backendConnection    = ko.observable();
        self.backendGuid          = ko.observable();
        self.backendHost          = ko.observable();
        self.backendLocal         = ko.observable(true);
        self.backendLogin         = ko.observable();
        self.backendName          = ko.observable();
        self.backendPort          = ko.observable();
        self.backendPreset        = ko.observable();
        self.backendPolicies      = ko.observableArray([]);
        self.backendRead          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendReadSpeed     = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWriteSpeed    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWritten       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cachingInfo          = ko.observable();
        self.cacheMisses          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.configuration        = ko.observable();
        self.extensible           = ko.observable(true);
        self.guid                 = ko.observable(guid);
        self.iops                 = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.loaded               = ko.observable(false);
        self.loading              = ko.observable(false);
        self.metadata             = ko.observable();
        self.name                 = ko.observable();
        self.notExtensibleReasons = ko.observableArray([]);
        self.rdmaEnabled          = ko.observable();
        self.readSpeed            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.status               = ko.observable();
        self.storageDriverGuids   = ko.observableArray([]);
        self.storageRouterGuids   = ko.observableArray([]);
        self.storedData           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.totalCacheHits       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.vDisks               = ko.observableArray([]);
        self.writeSpeed           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });

        // Computed
        self.bandwidth = ko.computed(function() {
            if (self.readSpeed() === undefined || self.writeSpeed() === undefined) {
                return undefined;
            }
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });

        // Functions
        self.fillData = function(data, options) {
            options = options || {};
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.status, data, 'status');
            generic.trySet(self.metadata, data, 'metadata');
            generic.trySet(self.backendConnection, data, 'connection');
            generic.trySet(self.backendLogin, data, 'login');
            generic.trySet(self.rdmaEnabled, data, 'rdma_enabled');

            if (data.hasOwnProperty('configuration')) {
                self.configuration(data.configuration);
            }
            if (data.metadata !== undefined && data.metadata !== null && data.metadata.hasOwnProperty('backend')) {
                if (data.metadata.backend.hasOwnProperty('backend_info')) {
                    generic.trySet(self.backendGuid, data.metadata.backend.backend_info, 'backend_guid');
                    generic.trySet(self.backendName, data.metadata.backend.backend_info, 'name');
                    generic.trySet(self.backendPreset, data.metadata.backend.backend_info, 'preset');
                    generic.trySet(self.backendPolicies, data.metadata.backend.backend_info, 'policies');
                }
                if (data.metadata.backend.hasOwnProperty('connection_info')) {
                    generic.trySet(self.backendHost, data.metadata.backend.connection_info, 'host');
                    generic.trySet(self.backendPort, data.metadata.backend.connection_info, 'port');
                    generic.trySet(self.backendLocal, data.metadata.backend.connection_info, 'local');
                }
            }
            if (data.hasOwnProperty('vdisks_guids') && !generic.tryGet(options, 'skipDisks', false)) {
                generic.crossFiller(
                    data.vdisks_guids, self.vDisks,
                    function(guid) {
                        return new VDisk(guid);
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('storagedrivers_guids')) {
                self.storageDriverGuids(data.storagedrivers_guids);
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.storedData(stats.stored);
                self.iops(stats['4k_operations_ps']);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.totalCacheHits(stats.cache_hits);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
                self.backendReadSpeed(stats.backend_data_read_ps);
                self.backendWriteSpeed(stats.backend_data_written_ps);
            }
            if (data.hasOwnProperty('extensible')) {
                self.extensible(data.extensible[0]);
                self.notExtensibleReasons(data.extensible[1]);
            }

            self.loaded(true);
            self.loading(false);
        };
        self.load = function(contents, options) {
            options = options || {};
            self.loading(true);
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadHandle)) {
                    var listOptions = {};
                    if (contents !== undefined) {
                        listOptions.contents = contents;
                    }
                    self.loadHandle = api.get('vpools/' + self.guid(), {queryparams: listOptions})
                        .done(function (data) {
                            self.fillData(data, options);
                            self.loaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function () {
                            self.loading(false);
                        });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.storageRouterHandle)) {
                    self.storageRouterHandle = api.get('vpools/' + self.guid() + '/storagerouters')
                        .done(function(data) {
                            self.storageRouterGuids(data.data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.getCachingData = function(sr_guid) {
            var cachingData = {};
            var cacheStructure = {
                'read': false,
                'write': false,
                'isBackend': false,
                'quota': undefined,
                'backendInfo': {  // Will be filled in when isBackend is true
                    // Structure is kept the same as the python api will return
                    'name': undefined,
                    'backend_guid': undefined,
                    'alba_backend_guid': undefined,
                    'policies': [],
                    'preset': undefined,
                    'connectionInfo': {
                        'client_id': undefined,
                        'client_secret': undefined,
                        'host': undefined,
                        'port': undefined,
                        'local': undefined
                    }
                }
            };
            var map = {
                'fragmentCache': {
                    'read': 'fragment_cache_on_read',
                    'write': 'fragment_cache_on_write',
                    'quota': 'quota_fc',
                    'backendPrefix': 'backend_aa_{0}'
                },
                'blockCache': {
                    'read': 'block_cache_on_read',
                    'write': 'block_cache_on_write',
                    'quota': 'quota_bc',
                    'backendPrefix': 'backend_bc_{0}'
                }
            };
            var backendMetadata = self.metadata().backend;
            // Extract the fragment cache and block cache read and write
            if (backendMetadata.hasOwnProperty('caching_info') && backendMetadata['caching_info'].hasOwnProperty(sr_guid)) {
                var cachingInfo = backendMetadata['caching_info'][sr_guid];
                $.each(map, function(cacheType, cacheTypeData) {
                    var metadataCacheData = $.extend(true, {}, cacheStructure);  // Deepcopy
                    cachingData[cacheType] = metadataCacheData;
                    $.each(cacheTypeData, function(structureKey, metadataKey){
                        if (structureKey === 'backendPrefix') {
                            // Get possible backend related info
                            metadataKey = metadataKey.format([sr_guid]);
                            if (self.metadata().hasOwnProperty(metadataKey)) {
                                var aaBackendData = self.metadata()[metadataKey];
                                metadataCacheData.isBackend = true;
                                $.extend(metadataCacheData.backendInfo, aaBackendData.backend_info);
                                $.extend(metadataCacheData.backendInfo.connectionInfo, aaBackendData.connection_info);
                            }
                        } else {
                            metadataCacheData[structureKey] = cachingInfo.hasOwnProperty(metadataKey) ? cachingInfo[metadataKey] : false;
                        }
                    });
                });
            }
            return cachingData

        }
    };
});

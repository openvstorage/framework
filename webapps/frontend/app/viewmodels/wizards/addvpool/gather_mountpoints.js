// Copyright 2014 Open vStorage NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout', 'ovs/generic', './data', 'ovs/api', '../../containers/storagerouter',
    '../../containers/storagedriver', 'ovs/shared'
], function($, ko, generic, data, api, StorageRouter, StorageDriver, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;
        self.shared = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [], storagedriver_mountpoints = [], bfs_mountpoints = [];

            // Collect previously configured storagedriver mountpoints
            $.each(self.data.storageDrivers(), function(index, storageDriver) {
                if (self.data.target() !== undefined && storageDriver.storageDriverID() === (self.data.name() + self.data.target().machineId())) {
                    return true;
                }
                if (storageDriver.mountpointBFS() !== undefined && storageDriver.mountpointBFS() !== "") {
                    bfs_mountpoints.push(storageDriver.mountpointBFS());
                    storagedriver_mountpoints.push(storageDriver.mountpointBFS());
                }
                return true;
            });

            var readCacheSizeBytes = self.data.readCacheSize() * 1024 * 1024 * 1024;
            var writeCacheSizeBytes = self.data.writeCacheSize() * 1024 * 1024 * 1024;
            var readCacheSizeAvailableBytes = self.data.readCacheAvailableSize() + self.data.sharedSize();
            var writeCacheSizeAvailableBytes = self.data.writeCacheAvailableSize() + self.data.sharedSize();
            var sharedAvailableModulus = self.data.sharedSize() - self.data.sharedSize() % (1024 * 1024 * 1024);
            var readCacheAvailableModulus = self.data.readCacheAvailableSize() - self.data.readCacheAvailableSize() % (1024 * 1024 * 1024);
            var writeCacheAvailableModulus = self.data.writeCacheAvailableSize() - self.data.writeCacheAvailableSize() % (1024 * 1024 * 1024);
            if (readCacheSizeBytes > readCacheSizeAvailableBytes) {
                fields.push('readCacheSize');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.over_allocation'));
            }
            if (writeCacheSizeBytes > writeCacheSizeAvailableBytes) {
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.over_allocation'));
            }
            if (readCacheSizeBytes + writeCacheSizeBytes > self.data.readCacheAvailableSize() + self.data.writeCacheAvailableSize() + sharedAvailableModulus) {
                fields.push('readCacheSize');
                fields.push('writeCacheSize');
                reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.over_allocation'));
            }
            var valid = reasons.length === 0;
            var unique_fields = fields.filter(generic.arrayFilterUnique);
            var unique_reasons = reasons.filter(generic.arrayFilterUnique);
            return { value: valid, reasons: unique_reasons, fields: unique_fields };
        });

        // Functions
        self.addReadCache = function() {
            var value = self.data.mtptCustomRC();
            if (value !== undefined && value !== '') {
                if (!self.data.mtptCustomRCs().contains(value)) {
                    self.data.mtptCustomRCs.push(value);
                }
                if (!self.data.mtptReadCaches().contains(value)) {
                    self.data.mtptReadCaches.push(value);
                }
                self.data.mtptCustomRC('');
            }
        };
        self.addWriteCache = function() {
            var value = self.data.mtptCustomWC();
            if (value !== undefined && value !== '') {
                if (!self.data.mtptCustomWCs().contains(value)) {
                    self.data.mtptCustomWCs.push(value);
                }
                if (!self.data.mtptWriteCaches().contains(value)) {
                    self.data.mtptWriteCaches.push(value);
                }
                self.data.mtptCustomWC('');
            }
        };
        self.activate = function() {
            if (data.extendVpool() === true) {
                self.loadStorageRoutersHandle = api.get('storagerouters', {
                        queryparams: {
                        contents: 'storagedrivers',
                        sort: 'name'
                    }
                }).done(function(data) {
                        var guids = [], srdata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            srdata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRouters,
                            function(guid) {
                                return new StorageRouter(guid);
                            }, 'guid'
                        );
                        $.each(self.data.storageRouters(), function(index, storageRouter) {
                            storageRouter.fillData(srdata[storageRouter.guid()]);
                        });
                        if (self.data.target() === undefined && self.data.storageRouter() !== undefined) {
                            self.data.target(self.data.storageRouter);
                        }
                    });

                if (data.vPool() !== undefined) {
                    self.data.vPool().load('storagedrivers', { skipDisks: true })
                        .then(function() {
                            self.data.storageDriver(new StorageDriver(self.data.vPool().storageDriverGuids()[0]));
                            self.data.storageDriver().load()
                                .then(function() {
                                    self.data.storageIP(self.data.storageDriver().storageIP())
                                });
                            self.data.vPool().backendType().load();
                            self.data.backend(self.data.vPool().backendType().name());
                            self.data.name(self.data.vPool().name());
                        })
                }
            }
        }
    };
});

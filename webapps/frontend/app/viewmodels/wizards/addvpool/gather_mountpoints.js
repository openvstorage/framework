// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
        self.activateResult = { valid: true, reasons: [], fields: [] };

        // Computed
        self.canContinue = ko.computed(function() {
            var reasons = [], fields = [];
            var readCacheSizeBytes = self.data.readCacheSize() * 1024 * 1024 * 1024;
            var writeCacheSizeBytes = self.data.writeCacheSize() * 1024 * 1024 * 1024;
            var readCacheSizeAvailableBytes = self.data.readCacheAvailableSize() + self.data.sharedSize();
            var writeCacheSizeAvailableBytes = self.data.writeCacheAvailableSize() + self.data.sharedSize();
            var sharedAvailableModulus = self.data.sharedSize() - self.data.sharedSize() % (1024 * 1024 * 1024);
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
            if (self.activateResult.valid === false) {
                valid = false;
                fields.push.apply(fields, self.activateResult.fields);
                reasons.push.apply(reasons, self.activateResult.reasons);
            }
            var unique_fields = fields.filter(generic.arrayFilterUnique);
            var unique_reasons = reasons.filter(generic.arrayFilterUnique);
            return { value: valid, reasons: unique_reasons, fields: unique_fields };
        });

        self.activate = function() {
            if (data.extendVpool() === true) {
                self.loadStorageRoutersHandle = api.get('storagerouters', { queryparams: {
                        contents: 'storagedrivers',
                        sort: 'name'
                }})
                    .done(function(data) {
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
                    });
                if (self.data.target() === undefined && self.data.storageRouter() !== undefined) {
                    self.data.target(self.data.storageRouter);
                }
                api.post('storagerouters/' + self.data.target().guid() + '/get_metadata')
                    .then(self.shared.tasks.wait)
                    .then(function(data) {
                        self.data.mountpoints(data.mountpoints);
                        self.data.partitions(data.partitions);
                        self.data.ipAddresses(data.ipaddresses);
                        self.data.arakoonFound(data.arakoon_found);
                        self.data.sharedSize(data.shared_size);
                        self.data.readCacheAvailableSize(data.readcache_size);
                        self.data.writeCacheAvailableSize(data.writecache_size);
                        self.data.readCacheSize(Math.floor(data.readcache_size / 1024 / 1024 / 1024));
                        self.data.writeCacheSize(Math.floor((data.writecache_size + data.shared_size) / 1024 / 1024 / 1024));
                    })
                    .done(function() {
                        self.activateResult = { valid: true, reasons: [], fields: [] };
                        var requiredRoles = ['READ', 'WRITE', 'SCRUB'];
                        if (self.data.arakoonFound() === false) {
                            requiredRoles.push('DB');
                        }
                        $.each(self.data.partitions(), function(role, partitions) {
                           if (requiredRoles.contains(role) && partitions.length > 0) {
                               generic.removeElement(requiredRoles, role);
                           }
                        });
                        if (requiredRoles.contains('DB')) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_arakoon'));
                            generic.removeElement(requiredRoles, 'DB');
                        }
                        $.each(requiredRoles, function(index, role) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_role', { what: role }));
                        });
                        if (self.data.backend() === 'distributed' && self.data.mountpoints().length === 0) {
                            self.activateResult.valid = false;
                            self.activateResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_mountpoints'));
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
                            self.data.backend(self.data.vPool().backendType().code().toLowerCase());
                            self.data.name(self.data.vPool().name());
                        })
                }
            }
        }
    };
});

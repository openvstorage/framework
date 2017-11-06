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
define(['jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared', 'ovs/errors',
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/vpool/vpool', 'viewmodels/containers/storagedriver/configuration',
    'viewmodels/services/backend', 'viewmodels/services/storagerouter', 'viewmodels/services/vpool'
],function($, ko, generic, api, shared, errors, StorageRouter, VPool, StorageDriverParams, backendService, storageRouterService, vpoolService){
    "use strict";
    // This data is not a singleton but a constructor
    return function(storageRouter, vPool, completed) {
        // Default values
        var isExtend = (vPool !== undefined);
        vPool = isExtend ? vPool : new VPool();
        storageRouter = storageRouter === undefined? new StorageRouter() : storageRouter;
        completed = (completed === undefined)? $.Deferred() : completed;

        var self = this;
        // Properties
        self.completed                          = completed;
        // General vPool changes
        self.configParams                       = undefined;  // Params related to general configs (sco size, dtl ...) Undefined as a viewmodel will be set
        self.backendData                        = undefined;  // Params related to the backend. Undefined as a viewmodel will be set
        // Storage driver changes
        self.cachingData                        = undefined;  // Params related to fragment cache and block cache. Undefined as a viewmodel will be set
        self.storageDriverParams                = undefined;  // Params related to the StorageDriver in general (proxies, globalWriteBuffer, storageIp) Undefined as a viewmodel will be set
        // Shared across the pages
        // Handles
        self.loadBackendsHandle     = undefined;
        self.loadAvailableStorageRoutersHandle  = undefined;
        self.loadStorageRoutersHandle           = undefined;

        // Observables
        self._storageRouter                      = ko.observable(storageRouter);
        self.vPool                              = ko.observable(vPool);
        self.isExtend                           = ko.observable(isExtend);
        // Data observables
        self.storageRouterMap                   = ko.observableDictionary({});
        self.albaPresetMap                      = ko.observable({});
        self.backends                           = ko.observableArray([]);
        self.invalidBackendInfo                 = ko.observable();
        self.loadingBackends                    = ko.observable();
        self.loadingStorageRouters              = ko.observable();
        self.loadingMetadata                    = ko.observable();
        self.globalWriteBufferMax               = ko.observable();  // Used to detect over allocation
        self.srPartitions                       = ko.observable();
        self.storageRoutersAvailable            = ko.observableArray([]);
        self.storageRoutersUsed                 = ko.observableArray([]);
        self.vPools                             = ko.observableArray([]);

        // Computed
        self.hasCacheQuota = ko.pureComputed(function() {
            var storageRouter = undefined;
            // These observables should only change once during the lifetime of the wizard and will cause less recomputing
            var storageRouters = [].concat(self.storageRoutersUsed(), self.storageRoutersAvailable());
            if (storageRouters.length > 0) {
                storageRouter = storageRouters[0];
            }
            return storageRouter.supportsCacheQuota();
        });
        self.scrubAvailable = ko.pureComputed(function() {
            // Scrub available is returned for all storagerouters (bad api design?)
            var mappedStorageRouters = self.storageRouterMap.values();
            if (mappedStorageRouters.length > 0) {
                return mappedStorageRouters[0].scrub_avaible
            }
            return false;
        });
        self.supportsBlockCache = ko.pureComputed(function() {
            var storageRouter = undefined;
            // These observables should only change once during the lifetime of the wizard and will cause less recomputing
            var storageRouters = [].concat(self.storageRoutersUsed(), self.storageRoutersAvailable());
            if (storageRouters.length > 0) {
                storageRouter = storageRouters[0];
            }
            return storageRouter.supportsBlockCache()
        });
        self.storageRouter = ko.computed({
            // Computed to act as a subscription
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                return self._storageRouter();
            },
            write: function(storageRouter) {
                // Mutate the backend info
                self._storageRouter(storageRouter);
                try  {  // Metadata might still be loading at this point
                    self.setWriteBuffer(self.storageRouter().guid());
                } catch(error) {
                    if (error.code !== 'str_not_found') {
                        throw error // Throw it again
                    }
                }
            }
        });

        // Functions
        self.fillData = function() {
            var requiredObservables = [];
            var missingObservables = [];
            $.each(requiredObservables, function(index, obs) {
                if (ko.utils.unwrapObservable(obs) === undefined) {
                    missingObservables.push(obs);
                }
            });
            if (missingObservables.length > 0) {
                throw new Error('The wizard does not have the necessary data to continue.')
            }
            // Fire up some asynchronous calls
            self.loadBackends();
            self.loadVPools();
            self.loadStorageRouters()
                .then(function(data) {
                    self.loadingMetadata(true);
                    // Load in metadata about these storagerouters
                    var storageRouters = [].concat(data.used, data.available);
                    var calls = [];
                    $.each(storageRouters, function(index, storageRouter) {
                        calls.push(
                            storageRouterService.getMetadata(storageRouter.guid())
                                .then(function(data) {
                                    self.storageRouterMap.set(storageRouter.guid(), data)
                                })
                        )
                    });
                    return $.when.apply($, calls)  // Return this Promise to chain it for the always
                        .done(function() {
                            // loadStorageRouters will have set a storagerouter so set some extra data
                            self.setWriteBuffer(self.storageRouter().guid());
                        });
                })
                .always(function() {
                    self.loadingMetadata(false)
                });
            // Set all configurable data
            self.storageDriverParams = new StorageDriverParams();
            self.backendData = self.vPool().getBackendInfo(true);
            self.cachingData = self.vPool().getCachingData(self.storageRouter().guid(), true, true);
            self.configParams = self.vPool().getConfiguration(true);
        };

        /**
         * Retrieves metadata from the cache
         * @param storageRouterGuid: Guid of the StorageRouter
         */
        self.getStorageRouterMetadata = function(storageRouterGuid) {
            if (!self.storageRouterMap.contains(storageRouterGuid)) {
                throw new errors.OVSError('str_not_found', 'No information about Storagerouter {0}'.format([storageRouterGuid]))
            }
            // Do some additional calculation
            var srData = self.storageRouterMap.get(storageRouterGuid, false)();
            var writeCacheSize = 0;
            $.each(srData.partitions.WRITE, function(index, info) {
                if (info['usable'] === true) {
                    writeCacheSize += info['available'];
                }
            });
            return {metadata: srData, writeCacheSize: writeCacheSize}
        };
        /**
         * Set the global write buffer values
         * @param storageRouterGuid: Guid of the StorageRouter
         */
        self.setWriteBuffer = function(storageRouterGuid) {
            var metadata = self.getStorageRouterMetadata(storageRouterGuid);
            var globalWriteBufferMax = metadata.writeCacheSize / Math.pow(1024, 3);
            self.globalWriteBufferMax(globalWriteBufferMax);
            self.storageDriverParams.globalWriteBuffer(globalWriteBufferMax);  // Initially set it to the max write buffer
        };
        self.filterBackendsByLocationKey = function(locationKey) {
            if (locationKey === undefined) {
                return self.backends();
            }
            return ko.utils.arrayFilter(self.backends(), function(backend) {
                return backend.locationKey.toLowerCase().startsWith(locationKey);
            });
        };
        self.buildLocationKey = function(connectionInfo) {
            if (connectionInfo === undefined || connectionInfo.isLocalBackend() === true) {
                return 'local';
            }
            return '{0}:{1}'.format([ko.utils.unwrapObservable(connectionInfo.host), ko.utils.unwrapObservable(connectionInfo.port)])
        };
        self.getBackend = function(backendGuid) {
            var currentList = self.backends();
            var currentFilters = {'backend_guid': backendGuid};
            $.each(currentFilters, function(itemKey, filterValue){
                currentList = ko.utils.arrayFilter(currentList, function(item) {
                    return item[itemKey] === filterValue;
                });
            });
            return currentList.length === 0 ? undefined : currentList[0];
        };
        self.getPreset = function(albaBackendGuid, presetName) {
            if (albaBackendGuid in self.albaPresetMap()) {
                var backendPreset = self.albaPresetMap()[albaBackendGuid];
                if (presetName in backendPreset) {
                    return backendPreset[presetName];
                }
                return undefined;
            }
            return undefined;
        };
        self.getDistinctBackends = function(backends) {
            /**
             * Filter out backend duplicates
             * @param backends: array of backends
             * @type backends: {Array}
             * @return {Array}
             */
            var seen = [];
            return ko.utils.arrayFilter(backends, function(backend) {
                return !seen.contains(backend.backend_guid) && seen.push(backend.backend_guid);
            });
        };
        /**
         * Loads in all backends for the current supplied data
         * All data is loaded in the backends variable. The key for remote connection is composed of ip:port
         * @param connectionInfo: Object with connection information (optional)
         * @returns {Promise}
        */
        self.loadBackends = function(connectionInfo) {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadBackendsHandle);
                var queryParams = {
                    contents: 'available'
                };
                var relayInfo = {};
                if (connectionInfo !== undefined && connectionInfo.isLocalBackend() === false) {
                    relayInfo.relay = 'relay/';
                    relayInfo.ip = connectionInfo.host();
                    relayInfo.port = connectionInfo.port();
                    relayInfo.client_id = connectionInfo.client_id().replace(/\s+/, "");
                    relayInfo.client_secret = connectionInfo.client_secret().replace(/\s+/, "");
                }
                self.loadingBackends(true);
                self.invalidBackendInfo(false);
                self.loadBackendsHandle = backendService.loadAlbaBackends(queryParams, relayInfo)
                    .done(function(data) {
                        var calls = [];
                        var availableBackends = self.backends();
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                queryParams.contents = 'name,ns_statistics,presets,usages,backend';
                                calls.push(backendService.loadAlbaBackend(item.guid, queryParams, relayInfo)
                                    .then(function(data) {
                                        var backendSize = data.usages.size;
                                        if ((backendSize !== undefined && backendSize > 0)) {
                                            // Add some metadata about the location
                                            data.locationKey = self.buildLocationKey(connectionInfo);
                                            availableBackends.push(data);
                                            self.albaPresetMap()[data.guid] = {};
                                            $.each(data.presets, function (_, preset) {
                                                self.albaPresetMap()[data.guid][preset.name] = preset;
                                            });
                                        }
                                    })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function() {
                                availableBackends = self.getDistinctBackends(availableBackends);
                                if (availableBackends.length > 0) {
                                    var sortFunction = function(backend1, backend2) {
                                        return backend1.name.toLowerCase() < backend2.name.toLowerCase() ? -1 : 1;
                                    };
                                    availableBackends = availableBackends.sort(sortFunction);
                                    self.backends(availableBackends);
                                }
                                self.loadingBackends(false);
                            })
                            .done(deferred.resolve(self.backends()))
                            .fail(function() {
                                availableBackends = self.getDistinctBackends(availableBackends);
                                self.backends(availableBackends);
                                self.loadingBackends(false);
                                self.invalidBackendInfo(true);
                                deferred.reject();
                            });
                    })
                    .fail(function() {
                        self.loadingBackends(false);
                        self.invalidBackendInfo(true);
                        deferred.reject();
                    });
            }).promise();
        };
        self.loadVPools = function() {
            return vpoolService.loadVPools({contents: ''})
                .then(function(data) {
                    var guids = [], vpData = {};
                    $.each(data.data, function (index, item) {
                        guids.push(item.guid);
                        vpData[item.guid] = item;
                    });
                    generic.crossFiller(
                        guids, self.vPools,
                        function (guid) {
                            return new VPool(guid);
                        }, 'guid'
                    );
                    $.each(self.vPools(), function (index, vpool) {
                        if (guids.contains(vpool.guid())) {
                            vpool.fillData(vpData[vpool.guid()]);
                        }
                    });
                });
        };
        /**
         * Load up the StorageRouters and map them as used or available
         */
        self.loadStorageRouters = function(){
            self.loadingStorageRouters(true);
            var promise;
            if (self.isExtend() === true) {
                promise = self.vPool().loadStorageRouters();
            } else {
                promise = $.Deferred(function (deferred) {
                    deferred.resolve();
                }).promise();
            }
            return promise
                .then(function () {
                    generic.xhrAbort(self.loadStorageRoutersHandle);
                    return self.loadStorageRoutersHandle = storageRouterService.loadStorageRouters({contents: 'storagedrivers,features', sort: 'name'})
                        .then(function (data) {
                            var guids = [], srdata = {};
                            $.each(data.data, function (index, item) {
                                guids.push(item.guid);
                                srdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageRoutersAvailable,
                                function (guid) {
                                    if (self.vPool() === undefined || !self.vPool().storageRouterGuids().contains(guid)) {
                                        return new StorageRouter(guid);
                                    }
                                }, 'guid'
                            );
                            generic.crossFiller(
                                guids, self.storageRoutersUsed,
                                function (guid) {
                                    if (self.vPool() !== undefined && self.vPool().storageRouterGuids().contains(guid)) {
                                        return new StorageRouter(guid);
                                    }
                                }, 'guid'
                            );
                            $.each(self.storageRoutersAvailable(), function (index, storageRouter) {
                                storageRouter.fillData(srdata[storageRouter.guid()]);
                            });
                            $.each(self.storageRoutersUsed(), function (index, storageRouter) {
                                storageRouter.fillData(srdata[storageRouter.guid()]);
                            });
                            self.storageRoutersAvailable.sort(function (sr1, sr2) {
                                return sr1.name() < sr2.name() ? -1 : 1;
                            });
                            self.storageRoutersUsed.sort(function (sr1, sr2) {
                                return sr1.name() < sr2.name() ? -1 : 1;
                            });
                            if (self.storageRouter().guid() === undefined && self.storageRoutersAvailable().length > 0) {
                                self.storageRouter(self.storageRoutersAvailable()[0]);
                            }
                            return {
                                used: self.storageRoutersUsed(),
                                available: self.storageRoutersAvailable()
                            }
                        })
                        .always(function() {
                            self.loadingStorageRouters(false)
                        })
                });
        };

        // Fill in the required data
        self.fillData();
    };
});

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
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/vpool/vpool',
    'viewmodels/services/backend', 'viewmodels/services/storagerouter'
],function($, ko, generic, api, shared, errors, StorageRouter, VPool, backendService, storageRouterService){
    "use strict";
    var singleton;
    singleton = function() {
        var wizardData = {
            storageDriver:                      ko.observable(),
            storageRouter:                      ko.observable(),
            vPool:                              ko.observable(),
            // Changes
            // General vPool changes
            configParams:                       undefined,  // Params related to general configs (sco size, dtl ...) Undefined as a viewmodel will be set
            // Storage driver changes
            cachingData:                        undefined,  // Params related to fragment cache and block cache. Undefined as a viewmodel will be set
            storageDriverParams:                undefined,  // Params related to the StorageDriver in general (proxies, globalWriteBuffer, storageIp) Undefined as a viewmodel will be set
            // Shared across the pages
            // Handles
            loadBackendsHandle:                 undefined,
            loadAvailableStorageRoutersHandle:  undefined,
            loadStorageRoutersHandle:           undefined,
            // Data
            storageRouterMap:                   ko.observableDictionary({}),
            albaPresetMap:                      ko.observable({}),
            backends:                           ko.observableArray([]),
            invalidBackendInfo:                 ko.observable(),
            loadingBackends:                    ko.observable(),
            loadingStorageRouters:              ko.observable(),
            globalWriteBufferMax:               ko.observable(),  // Used to detect over allocation
            srPartitions:                       ko.observable(),
            storageRoutersAvailable:            ko.observableArray([]),
            storageRoutersUsed:                 ko.observableArray([])
        };

        // Computed
        wizardData.hasCacheQuota = ko.pureComputed(function() {
            return wizardData.storageRouter() !== undefined &&
                wizardData.storageRouter().features() !== undefined &&
                wizardData.storageRouter().features().alba.features.contains('cache-quota');
        });
        wizardData.hasEE = ko.pureComputed(function() {
            return wizardData.storageRouter() !== undefined &&
                wizardData.storageRouter().features() !== undefined &&
                wizardData.storageRouter().features().alba.edition === 'enterprise';
        });

        // Functions
        wizardData.fillData = function() {
            var requiredObservables = [wizardData.storageRouter, wizardData.vPool];
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
            wizardData.loadBackends();
            wizardData.loadStorageRouters()
                .then(function(data) {
                    wizardData.loadingStorageRouters(true);
                    // Load in metadata about these storagerouters
                    var storageRouters = [].concat(data.used, data.available);
                    var calls = [];
                    $.each(storageRouters, function(index, storageRouter) {
                        calls.push(
                            storageRouterService.getMetadata(storageRouter.guid())
                                .then(function(data) {
                                    wizardData.storageRouterMap.set(storageRouter.guid(), data)
                                })
                        )
                    });
                    return $.when.apply($, calls)  // Return this Promise to chain it for the always
                        .done(function() {
                           // loadStorageRouters will have set a storagerouter so set some extra data
                            var metadata = wizardData.getStorageRouterMetadata(wizardData.storageRouter().guid());
                            var globalWriteBufferMax = metadata.writeCacheSize / Math.pow(1024, 3);
                           wizardData.globalWriteBufferMax(globalWriteBufferMax);
                           wizardData.storageDriverParams.globalWriteBuffer(globalWriteBufferMax);  // Initially set it to the max write buffer
                        });
                })
                .always(function() {
                    wizardData.loadingStorageRouters(false)
                });
            // Set all configurable data
            var vpool = wizardData.vPool() === undefined ? new VPool() : wizardData.vPool();
            wizardData.cachingData =vpool.getCachingData(wizardData.storageRouter().guid(), true, true);
            wizardData.configParams = vpool.getConfiguration(true);

        };
        /**
         * Retrieves metadata from the cache
         * @param storageRouterGuid
         */
        wizardData.getStorageRouterMetadata = function(storageRouterGuid) {
            if (!wizardData.storageRouterMap.contains(storageRouterGuid)) {
                throw new errors.OVSError('str_not_found', 'No information about Storagerouter {0}'.format([storageRouterGuid]))
            }
            // Do some additional calculation
            var srData = wizardData.storageRouterMap.get(storageRouterGuid, false)();
            var writeCacheSize = 0;
            $.each(srData.partitions.WRITE, function(index, info) {
                if (info['usable'] === true) {
                    writeCacheSize += info['available'];
                }
            });
            return {metadata: srData, writeCacheSize: writeCacheSize}

        };
        wizardData.filterBackendsByLocationKey = function(locationKey) {
            if (locationKey === undefined) {
                return wizardData.backends();
            }
            return ko.utils.arrayFilter(wizardData.backends(), function(backend) {
                return backend.locationKey.toLowerCase().startsWith(locationKey);
            });
        };
        wizardData.buildLocationKey = function(connectionInfo) {
            if (connectionInfo === undefined || connectionInfo.isLocalBackend() === true) {
                return 'local';
            }
            return '{0}:{1}'.format([ko.utils.unwrapObservable(connectionInfo.host), ko.utils.unwrapObservable(connectionInfo.port)])
        };
        wizardData.getBackend = function(backendGuid) {
            var currentList = wizardData.backends();
            var currentFilters = {'backend_guid': backendGuid};
            $.each(currentFilters, function(itemKey, filterValue){
                currentList = ko.utils.arrayFilter(currentList, function(item) {
                    return item[itemKey] === filterValue;
                });
            });
            return currentList.length === 0 ? undefined : currentList[0];
        };
        wizardData.getPreset = function(albaBackendGuid, presetName) {
            if (albaBackendGuid in wizardData.albaPresetMap()) {
                var backendPreset = wizardData.albaPresetMap()[albaBackendGuid];
                if (presetName in backendPreset) {
                    return backendPreset[presetName];
                }
                return undefined;
            }
            return undefined;
        };
        wizardData.getDistinctBackends = function(backends) {
            /**
             * Filter out backend duplicates
             * @param backends: array of backends
             * @type backends: {Array}
             * @return {Array}
             */
            var seen = [];
            return ko.utils.arrayFilter(backends, function(backend) {
                // Add up the two keys
                var uniqueKey = backend.backend_guid;
                if (backend.locationKey) {
                    uniqueKey += backend.locationKey
                }
                return !seen.contains(uniqueKey) && seen.push(uniqueKey);
            });
        };
        /**
         * Loads in all backends for the current supplied data
         * All data is loaded in the backends variable. The key for remote connection is composed of ip:port
         * @param connectionInfo: Object with connection information (optional)
         * @returns {Promise}
        */
        wizardData.loadBackends = function(connectionInfo) {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(wizardData.loadBackendsHandle);
                var relay = '';
                var queryParams = {
                    contents: 'available'
                };
                var remoteInfo = {};
                if (connectionInfo !== undefined && connectionInfo.isLocalBackend() === false) {
                    relay = 'relay/';
                    remoteInfo.ip = connectionInfo.host();
                    remoteInfo.port = connectionInfo.port();
                    remoteInfo.client_id = connectionInfo.client_id().replace(/\s+/, "");
                    remoteInfo.client_secret = connectionInfo.client_secret().replace(/\s+/, "");
                }
                $.extend(queryParams, remoteInfo);
                wizardData.loadingBackends(true);
                wizardData.invalidBackendInfo(false);
                wizardData.loadBackendsHandle = backendService.loadAlbaBackends(queryParams, relay)
                    .done(function(data) {
                        var calls = [];
                        var availableBackends = wizardData.backends();
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                queryParams.contents = 'name,ns_statistics,presets,usages,backend';
                                calls.push(backendService.loadAlbaBackend(item.guid, queryParams, relay)
                                    .then(function(data) {
                                        var backendSize = data.usages.size;
                                        if ((backendSize !== undefined && backendSize > 0)) {
                                            // Add some metadata about the location
                                            data.locationKey = wizardData.buildLocationKey(connectionInfo);
                                            availableBackends.push(data);
                                            wizardData.albaPresetMap()[data.guid] = {};
                                            $.each(data.presets, function (_, preset) {
                                                wizardData.albaPresetMap()[data.guid][preset.name] = preset;
                                            });
                                        }
                                    })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function() {
                                availableBackends = wizardData.getDistinctBackends(availableBackends);
                                if (availableBackends.length > 0) {
                                    var sortFunction = function(backend1, backend2) {
                                        return backend1.name.toLowerCase() < backend2.name.toLowerCase() ? -1 : 1;
                                    };
                                    availableBackends = availableBackends.sort(sortFunction);
                                    wizardData.backends(availableBackends);
                                }
                                wizardData.loadingBackends(false);
                            })
                            .done(deferred.resolve(wizardData.backends()))
                            .fail(function() {
                                availableBackends = wizardData.getDistinctBackends(availableBackends);
                                wizardData.backends(availableBackends);
                                wizardData.loadingBackends(false);
                                wizardData.invalidBackendInfo(true);
                                deferred.reject();
                            });
                    })
                    .fail(function() {
                        wizardData.loadingBackends(false);
                        wizardData.invalidBackendInfo(true);
                        deferred.reject();
                    });
            }).promise();
        };
        /**
         * Load up the StorageRouters and map them as used or available
         */
        wizardData.loadStorageRouters = function(){
            wizardData.loadingStorageRouters(true);
            var promise;
            if (wizardData.vPool() !== undefined) {
                promise = wizardData.vPool().loadStorageRouters();
            } else {
                promise = $.Deferred(function (deferred) {
                    deferred.resolve();
                }).promise();
            }
            return promise
                .then(function () {
                    generic.xhrAbort(wizardData.loadStorageRoutersHandle);
                    return wizardData.loadStorageRoutersHandle = storageRouterService.loadStorageRouters({contents: 'storagedrivers,features', sort: 'name'})
                        .then(function (data) {
                            var guids = [], srdata = {};
                            $.each(data.data, function (index, item) {
                                guids.push(item.guid);
                                srdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, wizardData.storageRoutersAvailable,
                                function (guid) {
                                    if (wizardData.vPool() === undefined || !wizardData.vPool().storageRouterGuids().contains(guid)) {
                                        return new StorageRouter(guid);
                                    }
                                }, 'guid'
                            );
                            generic.crossFiller(
                                guids, wizardData.storageRoutersUsed,
                                function (guid) {
                                    if (wizardData.vPool() !== undefined && wizardData.vPool().storageRouterGuids().contains(guid)) {
                                        return new StorageRouter(guid);
                                    }
                                }, 'guid'
                            );
                            $.each(wizardData.storageRoutersAvailable(), function (index, storageRouter) {
                                storageRouter.fillData(srdata[storageRouter.guid()]);
                            });
                            $.each(wizardData.storageRoutersUsed(), function (index, storageRouter) {
                                storageRouter.fillData(srdata[storageRouter.guid()]);
                            });
                            wizardData.storageRoutersAvailable.sort(function (sr1, sr2) {
                                return sr1.name() < sr2.name() ? -1 : 1;
                            });
                            wizardData.storageRoutersUsed.sort(function (sr1, sr2) {
                                return sr1.name() < sr2.name() ? -1 : 1;
                            });
                            if (wizardData.storageRouter() === undefined && wizardData.storageRoutersAvailable().length > 0) {
                                wizardData.storageRouter(wizardData.storageRoutersAvailable()[0]);
                            }
                            return {
                                used: wizardData.storageRoutersUsed(),
                                available: wizardData.storageRoutersAvailable()
                            }
                        })
                        .always(function() {
                            wizardData.loadingStorageRouters(false)
                        })
                });
        };
        return wizardData;
    };
    return singleton();
});

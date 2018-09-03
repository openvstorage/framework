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
    'ovs/generic', 'ovs/api', 'ovs/shared',
    'viewmodels/containers/shared/base_container',
    'viewmodels/services/storagerouter'],
    function($, ko, generic, api, shared,
             BaseContainer,
             StoragerouterService){

    function ReconfigureVPoolData(vpool, storagerouter, storagedriver){
        var self = this;
        BaseContainer.call(self); // Inheritance

        var requiredObservables = [storagerouter, vpool, storagedriver];
        var missingObservables = [];
        $.each(requiredObservables, function(index, obs) {
            if (ko.utils.unwrapObservable(obs) === undefined) {
                missingObservables.push(obs);
            }
        });
        if (missingObservables.length > 0) {
            throw new Error('The wizard does not have the necessary data to continue.')
        }

        var loadBackendsHandle;
        var loadAvailableStorageRoutersHandle;

        self.storageRouter = storagerouter || {};
        self.vPool = vpool || {};
        self.storageDriver = storagedriver || {};
        self.cachingData = vpool.getCachingData(storagerouter.guid(), true);
        self.configParams = vpool.getConfiguration(true);
        self.loadingBackends = ko.observable(false);
        self.globalWriteBufferMax = ko.observable(null);
        self.srPartitions = ko.observable(null);
        self.backends = ko.observableArray([]);
        self.invalidBackendInfo = ko.observable(false);
        self.globalWriteBuffer = ko.observable(undefined).extend({numeric: {min: 1, max: 10240, allowUndefined: true}, rateLimit: { method: "notifyWhenChangesStop", timeout: 800 }})
        self.proxyAmount = ko.observable(storagedriver.albaProxyGuids().length).extend({numeric: {min: 1, max: 16}});
        self.albaPresetMap = ko.observable({});

        // Fire up some asynchronous calls
        self.loadBackends();
        self.loadStorageRouterMetadata(self.storageRouter.guid())
            .then(function(data) {
                self.srPartitions(data.srData.partitions);
                var sdGlobalWriteBuffer =  self.storageDriver.vpoolBackendInfo().global_write_buffer;
                var totalBuffer = data.writeCacheSize + sdGlobalWriteBuffer;
                self.globalWriteBuffer(sdGlobalWriteBuffer / Math.pow(1024, 3));
                self.globalWriteBufferMax(totalBuffer / Math.pow(1024, 3));
            });

        // Computed
        self.hasCacheQuota = ko.pureComputed(function() {
            return self.storageRouter.supportsCacheQuota()
        });
        self.hasEE = ko.pureComputed(function() {
            return self.storageRouter.isEnterpriseEdition()
        });
    }
    var functions = {
        loadStorageRouterMetadata: function(storageRouterGuid) {
            var self = this;
            if (ko.utils.unwrapObservable(storageRouterGuid) === undefined) {
                throw new Error('Cannot load metadata of an undefined storage router guid')
            }
            return StoragerouterService.getMetadata(storageRouterGuid)
                .then(function (srData) {
                    // Fill in the max global write buffer
                    var writeCacheSize = 0;
                    $.each(srData.partitions.WRITE, function (index, info) {
                        if (info['usable'] === true) {
                            writeCacheSize += info['available'];
                        }
                    });
                    return {srData: srData, writeCacheSize: writeCacheSize}
                });
        },
        filterBackendsByLocationKey: function(locationKey) {
            var self = this;
            if (locationKey === undefined) {
                return self.backends();
            }
            return ko.utils.arrayFilter(self.backends(), function(backend) {
                return backend.locationKey.toLowerCase().startsWith(locationKey);
            });
        },
        buildLocationKey: function(connectionInfo) {
            if (connectionInfo === undefined || connectionInfo.isLocalBackend() === true) {
                return 'local';
            }
            return '{0}:{1}'.format([ko.utils.unwrapObservable(connectionInfo.host), ko.utils.unwrapObservable(connectionInfo.port)])
        },
        getBackend: function(backendGuid) {
            var self = this;
            var currentList = self.backends();
            var currentFilters = {'backend_guid': backendGuid};
            $.each(currentFilters, function(itemKey, filterValue){
                currentList = ko.utils.arrayFilter(currentList, function(item) {
                    return item[itemKey] === filterValue;
                });
            });
            return currentList.length === 0 ? undefined : currentList[0];
        },
        getPreset: function(albaBackendGuid, presetName) {
            var self = this;
            if (albaBackendGuid in self.albaPresetMap()) {
                var backendPreset = self.albaPresetMap()[albaBackendGuid];
                if (presetName in backendPreset) {
                    return backendPreset[presetName];
                }
                return undefined;
            }
            return undefined;
        },
        getDistinctBackends: function(backends) {
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
        },
        loadBackends: function(connectionInfo) {
            var self = this;
            /**
             * Loads in all backends for the current supplied data
             * All data is loaded in the backends variable. The key for remote connection is composed of ip:port
             * @param connectionInfo: Object with connection information (optional)
             * @returns {Promise}
            */
            return $.when().then(function() {
                generic.xhrAbort(self.loadBackendsHandle);
                var relay = '';
                var getData = {
                    contents: 'available'
                };
                var remoteInfo = {};

                if (connectionInfo && connectionInfo.isLocalBackend() === false) {
                    relay = 'relay/';
                    remoteInfo.ip = connectionInfo.host();
                    remoteInfo.port = connectionInfo.port();
                    remoteInfo.client_id = connectionInfo.client_id().replace(/\s+/, "");
                    remoteInfo.client_secret = connectionInfo.client_secret().replace(/\s+/, "");
                }
                $.extend(getData, remoteInfo);
                self.loadingBackends(true);
                self.invalidBackendInfo(false);
                return self.loadBackendsHandle = api.get(relay + 'alba/backends', { queryparams: getData })
                    .then(function(data) {
                        var calls = [];
                        var availableBackends = self.backends();
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                getData.contents = 'name,ns_statistics,presets,usages,backend';
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.guid + '/', { queryparams: getData })
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
                        return $.when.apply($, calls)
                            .then(function() {
                                availableBackends = self.getDistinctBackends(availableBackends);
                                if (availableBackends.length > 0) {
                                    var sortFunction = function(backend1, backend2) {
                                        return backend1.name.toLowerCase() < backend2.name.toLowerCase() ? -1 : 1;
                                    };
                                    availableBackends = availableBackends.sort(sortFunction);
                                    return availableBackends
                                }
                            }, function(error) {
                                availableBackends = self.getDistinctBackends(availableBackends);
                                self.invalidBackendInfo(true);
                                throw error
                            })
                            .always(function(){
                                self.backends(availableBackends);
                            });
                    })
            }).then(function(result) {
                // Return the result of the complete chain. The exception should be captured
                return result
            }, function(error) {
                // Something went wrong during the chain. Assume it was invalid backend info
                self.invalidBackendInfo(true);
                throw error
            }).always(function() {
                // No longer loading at this point
                self.loadingBackends(false);
            })
        },
        parsePreset: function(preset){
            var worstPolicy = 0;
            var policies = [];
            var replication = undefined;
            var policyObject = undefined;
            var policyMapping = ['grey', 'black', 'green'];
            $.each(preset.policies, function (jndex, policy) {
                policyObject = JSON.parse(policy.replace('(', '[').replace(')', ']'));
                var isAvailable = preset.policy_metadata[policy].is_available;
                var isActive = preset.policy_metadata[policy].is_active;
                var inUse = preset.policy_metadata[policy].in_use;
                var newPolicy = {
                    text: policy,
                    color: 'grey',
                    isActive: false,
                    k: policyObject[0],
                    m: policyObject[1],
                    c: policyObject[2],
                    x: policyObject[3]
                };
                if (isAvailable) {
                    newPolicy.color = 'black';
                }
                if (isActive) {
                    newPolicy.isActive = true;
                }
                if (inUse) {
                    newPolicy.color = 'green';
                }
                worstPolicy = Math.max(policyMapping.indexOf(newPolicy.color), worstPolicy);
                policies.push(newPolicy);
            });
            if (preset.policies.length === 1) {
                policyObject = JSON.parse(preset.policies[0].replace('(', '[').replace(')', ']'));
                if (policyObject[0] === 1 && policyObject[0] + policyObject[1] === policyObject[3] && policyObject[2] === 1) {
                    replication = policyObject[0] + policyObject[1];
                }
            }
            return {
                policies: policies,
                name: preset.name,
                compression: preset.compression,
                fragSize: preset.fragment_size,
                encryption: preset.fragment_encryption,
                color: policyMapping[worstPolicy],
                inUse: preset.in_use,
                isDefault: preset.is_default,
                replication: replication
            }
        },

        parsePresets: function(backend) {
            var self = this;
            var presets = [];
            if (ko.utils.unwrapObservable(backend) === undefined) {
                return presets
            }
            $.each(backend.presets, function (index, preset) {
                presets.push(self.parsePreset(preset));
            });
            var sortFunction = function (preset1, preset2) {
                return preset1.name.toLowerCase() < preset2.name.toLowerCase() ? -1 : 1;
            };
            presets = presets.sort(sortFunction);
            return presets
        }
    };
    ReconfigureVPoolData.prototype = $.extend({}, BaseContainer.prototype, functions);
    return ReconfigureVPoolData;

});

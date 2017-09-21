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
define(['jquery', 'knockout', 'ovs/generic', 'ovs/api'], function($, ko, generic, api){
    "use strict";
    var singleton;
    singleton = function() {
        var wizardData = {
            storageDriver:           ko.observable(),
            storageRouter:           ko.observable(),
            vPool:                   ko.observable(),
            // Changes
            cachingData:             undefined,  // Changes related to fragment cache and block cache (not observable as it is viewmodel)
            configParams:            undefined,  // Changes related to general configs (sco size, dtl ...)
            proxyAmount:             ko.observable(),
            // Shared across the pages
            // Handles
            loadBackendsHandle:      undefined,
            // Data
            albaPresetMap:           ko.observable({}),
            backends:                ko.observableArray([]),
            invalidBackendInfo:      ko.observable(),
            loadingBackends:         ko.observable()
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
        wizardData.loadBackends = function(connectionInfo) {
            /**
             * Loads in all backends for the current supplied data
             * All data is loaded in the backends variable. The key for remote connection is composed of ip:port
             * @param connectionInfo: Object with connection information (optional)
             * @returns $.Deferred
            */
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(wizardData.loadBackendsHandle);
                var relay = '';
                var getData = {
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
                $.extend(getData, remoteInfo);
                wizardData.loadingBackends(true);
                wizardData.invalidBackendInfo(false);
                wizardData.loadBackendsHandle = api.get(relay + 'alba/backends', { queryparams: getData })
                    .done(function(data) {
                        var calls = [];
                        var availableBackends = wizardData.backends();
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                getData.contents = 'name,ns_statistics,presets,usages,backend';
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.guid + '/', { queryparams: getData })
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
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                availableBackends = wizardData.getDistinctBackends(availableBackends);
                                wizardData.backends(availableBackends);
                                wizardData.loadingBackends(false);
                                wizardData.invalidBackendInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        wizardData.loadingBackends(false);
                        wizardData.invalidBackendInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };
        wizardData.parsePreset = function(preset){
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
        };

        wizardData.parsePresets = function(backend) {
            var presets = [];
            if (ko.utils.unwrapObservable(backend) === undefined) {
                return presets
            }
            $.each(backend.presets, function (index, preset) {
                presets.push(wizardData.parsePreset(preset));
            });
            var sortFunction = function (preset1, preset2) {
                return preset1.name.toLowerCase() < preset2.name.toLowerCase() ? -1 : 1;
            };
            presets = presets.sort(sortFunction);
            return presets
        };
        return wizardData;
    };
    return singleton();
});

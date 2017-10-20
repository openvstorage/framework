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
    'ovs/shared', 'ovs/api', 'ovs/generic',
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/storagedriver/storagedriver',
    'viewmodels/containers/vpool/vpool',
    'viewmodels/services/backend'
], function ($, ko, shared, api, generic, StorageRouter, StorageDriver, VPool, backendService) {
    "use strict";
    return function (options) {
        var self = this;

        // Variables
        self.data   = options.data;
        self.shared = shared;

        // Observables
        self._storageRouterIpAddresses  = ko.observableArray([]);
        self.loadingPrevalidations      = ko.observable();
        self.preValidateResult          = ko.observable({valid: true, reasons: [], fields: []});

        // Computed
        self.canContinue = ko.pureComputed(function () {
            var showErrors = false;
            var reasons = [], fields = [];
            var requiredRoles = ['DB', 'DTL', 'WRITE'];
            if (self.data.vPool() === undefined) {
                if (!self.data.vPool().name.valid()) {
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_name'));
                } else {
                    $.each(self.data.vPools(), function (index, vpool) {
                        if (vpool.name() === self.data.vPool().name()) {
                            fields.push('name');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.duplicate_name'));
                        }
                    });
                }
            }
            if (self.loadingPrevalidations() === true) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.validating_mountpoint'));
            } else {
                var preValidation = self.preValidateResult();
                if (preValidation.valid === false) {
                    showErrors = true;
                    reasons = reasons.concat(preValidation.reasons);
                    fields = fields.concat(preValidation.fields);
                }
            }
            if (self.data.loadingBackends() === true) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.backends_loading'));
            } else {
                var backendInfo = self.data.backendData.backend_info;
                var connectionInfo = backendInfo.connection_info;
                if (backendInfo.backend_guid() === undefined) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.choose_backend'));
                    fields.push('backend');
                } else if (backendInfo.preset() === undefined) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.choose_preset'));
                    fields.push('preset');
                }
                if (connectionInfo.isLocalBackend() === false && connectionInfo.hasRemoteInfo() === false || self.data.invalidBackendInfo() === true) {
                            reasons.push($.t('ovs:wizards.reconfigure_vpool.gather_fragment_cache.invalid_alba_info'));
                            fields.push('invalid_alba_info');
                        }
            }
            if (self.data.scrubAvailable() === false) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.missing_role', {what: 'SCRUB'}));
            }
            try {
                var partitions = self.data.getStorageRouterMetadata(self.data.storageRouter().guid()).metadata.partitions;
                if (partitions !== undefined) {
                $.each(partitions, function (role, partitions) {
                    if (requiredRoles.contains(role) && partitions.length > 0) {
                        generic.removeElement(requiredRoles, role);
                    }
                });
                $.each(requiredRoles, function (index, role) {
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.missing_role', {what: role}));
                });
            }
            }
            catch (error) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.metadata_loading'));
            }

            if (self.data.storageDriverParams.storageIP() === undefined) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.missing_storage_ip'));
                fields.push('storageip');
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields, showErrors: showErrors };
        });
        self.storageRouterIpAddresses = ko.computed(function() {
            var ipAddresses = [];
            if (self.data.storageRouter() === undefined) {
                return ipAddresses
            } else {
                try{
                    var metadata = self.data.getStorageRouterMetadata(self.data.storageRouter().guid()).metadata;
                    ipAddresses = metadata.ipaddresses;
                } catch (error) {
                    if (error.code !== 'str_not_found') {
                        throw error // Throw it again
                    }
                    // Nothing found, return the empty array
                    return ipAddresses
                }
            }
            self._storageRouterIpAddresses(ipAddresses);
            return ipAddresses;
        });
        self.vpoolBackend = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var backendInfo = self.data.backendData.backend_info;
                var backend = self.data.getBackend(backendInfo.backend_guid());
                if (backend === undefined) {
                    // Return the first of the list
                    var backends = self.getVPoolBackends();
                    if (backends !== undefined && backends.length > 0) {
                        backend = backends[0];
                        self.vpoolBackend(backend)
                    }
                }
                return backend;
            },
            write: function(backend) {
                // Mutate the backend info
                var backendInfo = self.data.backendData.backend_info;
                backendInfo.name(backend.name);
                backendInfo.backend_guid(backend.backend_guid);
                backendInfo.alba_backend_guid(backend.guid);
            }
        });
        self.vpoolBackends = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function () {
                var backends = self.getVPoolBackends();
                if (backends.length === 0) {
                    // Update our Model
                    self.resetBackend();
                }
                return backends;
            }
        });
        self.storageRouterIpAddress = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                // Computed as the change of ip adresses should be accounted for
                var ipAddresses = self._storageRouterIpAddresses();
                var currentIP = self.data.storageDriverParams.storageIP();
                if (!ipAddresses.contains(currentIP)) {
                    // Select the first in the list (if possible)
                    if (ipAddresses.length > 0 ) {
                        currentIP = ipAddresses[0];
                    } else {
                        currentIP = undefined;
                    }
                    // Change our mapped property
                    self.storageRouterIpAddress(currentIP);
                }
                return currentIP;
            },
            write: function(ip) {
               self.data.storageDriverParams.storageIP(ip);
            }
        });
        self.enhancedPreset = ko.pureComputed(function() {
            /**
             * Compute a preset to look like presetName: (1,1,1,1),(2,1,2,1)
             */
            var vpool = self.data.vPool();
            if (vpool === undefined || (vpool.backendPolicies().length === 0 && vpool.backendPreset === undefined)) {
               return undefined
            }
            return backendService.enhancePreset(vpool.backendPreset(), vpool.backendPolicies());
        });
        self.enhancedPresets = ko.pureComputed(function() {
            var presets = self.vpoolBackend() === undefined ? [] : self.vpoolBackend().presets;
            return backendService.parsePresets(presets)
        });
        self.preset = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                var parsedPreset = undefined;
                if (self.vpoolBackend() === undefined) {
                    return parsedPreset
                }
                var backendInfo = self.data.backendData.backend_info;
                var preset = self.data.getPreset(backendInfo.alba_backend_guid(), backendInfo.preset());
                if (preset === undefined) {
                    // No preset could be found for our current setting. Attempt to reconfigure it
                    var enhancedPresets = self.enhancedPresets();
                    if (enhancedPresets.length > 0) {
                        parsedPreset = enhancedPresets[0];
                        self.preset(parsedPreset);  // This will trigger this compute to trigger again but also correct the mistake
                    }
                    return parsedPreset
                }
                return backendService.parsePreset(preset);
            },
            write: function(preset) {
                var backendInfo = self.data.backendData.backend_info;
                backendInfo.preset(preset.name);
            }
        });

        // Functions
        self.loadBackends = function() {
            var connectionInfo = self.data.backendData.backend_info.connection_info;
            return self.data.loadBackends(connectionInfo)
        };
        self.getVPoolBackends = function() {
            // Wrapped function for the computable
            // Issue was when the computed would update the Model when no backends were found, the computed would not
            // return its value and the backend computed would fetch the old values, causing a mismatch
            var connectionInfo = self.data.backendData.backend_info.connection_info;
            return self.data.filterBackendsByLocationKey(self.data.buildLocationKey(connectionInfo));
        };
        self.resetBackend = function() {
            // Will force to recompute everything
            self.vpoolBackend({'name': undefined, 'backend_guid':undefined, 'alba_backend_guid': undefined});
            self.resetPreset();
        };
        self.resetPreset = function() {
            self.preset({'name': undefined});
        };
        self.preValidate = function () {
            var validationResult = {valid: true, reasons: [], fields: []};
            var vpoolName = self.data.vPool().name();
            $.Deferred(function (deferred) {
                self.loadingPrevalidations(true);
                generic.xhrAbort(self.checkMtptHandle);
                self.checkMtptHandle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/check_mtpt', {data: {name: vpoolName}})
                    .then(self.shared.tasks.wait)
                    .done(function (data) {
                        if (data === true) {
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.mtpt_in_use', {what: vpoolName}));
                            validationResult.fields.push('name');
                        }
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function () {
                        self.preValidateResult(validationResult);
                        self.loadingPrevalidations(false);
                    })

            }).promise();
        };

    };
});

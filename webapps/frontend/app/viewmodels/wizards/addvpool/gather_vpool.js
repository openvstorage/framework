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
    'viewmodels/wizards/addvpool/gather_base'
], function ($, ko,
             shared, api, generic,
             BaseStep) {
    "use strict";
    return function (options) {
        var self = this;

        BaseStep.call(self, options);

        self.canChangePreset = !self.data.isExtend();
        // Observables
        self._storageRouterIpAddresses  = ko.observableArray([]);
        self.loadingPrevalidations      = ko.observable();
        self.preValidateResult          = ko.observable({valid: true, reasons: [], fields: []});

        // Computed
        self.canContinue = ko.pureComputed(function () {
            var showErrors = false;
            var reasons = [], fields = [];
            var requiredRoles = ['DB', 'DTL', 'WRITE'];
            if (self.data.vPool().guid() === undefined) {
                if (!self.data.vPool().name.valid()) {
                    fields.push('name');
                    reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.invalid_name'));
                } else {
                    $.each(self.data.vPools(), function (index, vpool) {
                        if (vpool.name() === self.data.vPool().name()) {
                            fields.push('name');
                            reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.duplicate_name'));
                            return false;
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
            if (self.data.loadingStorageRouterInfo()) {
                reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.metadata_loading'));
            } else{
                try {
                    var partitions = self.data.getStorageRouterMetadata(self.data.storageRouter().guid()).metadata.partitions;
                    if (partitions) {
                        $.each(partitions, function (role, partitions) {
                            if (requiredRoles.contains(role) && partitions.length > 0) {
                                requiredRoles.remove(role)
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
            }
            if (!self.data.storageDriverParams.storageIP()) {
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
        self.storageRouterIpAddress = ko.computed({
            deferEvaluation: true,  // Wait with computing for an actual subscription
            read: function() {
                // Computed as the change of ip addresses should be accounted for
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

        // Functions
        self.preValidate = function () {
            var validationResult = {valid: true, reasons: [], fields: []};
            var vpoolName = self.data.vPool().name();
            self.loadingPrevalidations(true);
            generic.xhrAbort(self.checkMtptHandle);
            return self.checkMtptHandle = api.post('storagerouters/' + self.data.storageRouter().guid() + '/check_mtpt', {data: {name: vpoolName}})
                .then(shared.tasks.wait)
                .then(function (data) {
                    if (data === true) {
                        validationResult.valid = false;
                        validationResult.reasons.push($.t('ovs:wizards.add_vpool.gather_vpool.mtpt_in_use', {what: vpoolName}));
                        validationResult.fields.push('name');
                    }
                })
                .always(function () {
                    self.preValidateResult(validationResult);
                    self.loadingPrevalidations(false);
                })
        };

        // Abstract implementations
        self.getBackendInfo = function() {
            return self.data.backendData.backend_info;
        };
        self.getConnectionInfo = function() {
            return self.getBackendInfo().connection_info;
        };

    };
});

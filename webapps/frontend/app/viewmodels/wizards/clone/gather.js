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
    'ovs/api', 'ovs/generic',
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/vdisk/vdisk',
    './data'
], function($, ko, api, generic, StorageRouter, VDisk, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Observables
        self.loaded            = ko.observable(false);
        self.loading           = ko.observable(false);
        self.preValidateResult = ko.observable({ valid: true, reasons: [], fields: [] });

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [], showErrors = false,
                preValidation = self.preValidateResult();
            if (preValidation.valid === false) {
                showErrors = true;
                reasons = reasons.concat(preValidation.reasons);
                fields = fields.concat(preValidation.fields);
            }
            if (self.data.storageRouter() === undefined) {
                valid = false;
                fields.push('vm');
                reasons.push($.t('ovs:wizards.clone.gather.nostoragerouter'));
            }
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.clone.gather.invalid_name'));
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });
        self.cleanedName = ko.computed(function() {
            return generic.cleanDeviceName(self.data.name());
        });

        // Functions
        self.preValidate = function() {
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                if (self.data.vDisk() === undefined || self.data.vDisk().vpoolGuid() === undefined) {
                    deferred.reject();
                    return;
                }
                api.get('vpools/' + self.data.vDisk().vpoolGuid() + '/devicename_exists', { queryparams: { name: self.data.name() }})
                    .done(function(exists) {
                        if (exists) {
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.clone.gather.name_exists'));
                            validationResult.fields.push('name');
                            self.preValidateResult(validationResult);
                            deferred.reject();
                        } else {
                            self.preValidateResult(validationResult);
                            deferred.resolve();
                        }
                    })
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.loading(true);
            return $.Deferred(function(deferred) {
                api.get('vdisks/' + self.data.vDisk().guid() + '/get_target_storagerouters', {
                    queryparams: {
                        contents: '',
                        sort: 'name'
                    }
                })
                    .done(function(data) {
                        var guids = [], sadata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            sadata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.storageRouters,
                            function(guid) {
                                return new StorageRouter(guid);
                            }, 'guid'
                        );
                        $.each(self.data.storageRouters(), function(index, storageRouter) {
                            if (guids.contains(storageRouter.guid())) {
                                storageRouter.fillData(sadata[storageRouter.guid()]);
                            }
                        });
                        self.loaded(true);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function () {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});

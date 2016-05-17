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
    './data'
], function ($, ko, shared, api, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared         = shared;
        self.data           = data;
        self.validateHandle = undefined;

        // Observables
        self.preValidateResult = ko.observable({ valid: true, reasons: [], fields: [] });

        // Computed
        self.canContinue = ko.computed(function () {
            var valid = true, showErrors = false, reasons = [], fields = [], preValidation = self.preValidateResult();
            if (preValidation.valid === false) {
                showErrors = true;
                reasons = reasons.concat(preValidation.reasons);
                fields = fields.concat(preValidation.fields);
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });

        // Functions
        self.preValidate = function() {
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.validateHandle);
                var postData = {
                    license_string: self.data.licenseString(),
                    validate_only: true
                };
                self.validateHandle = api.post('licenses', { data: postData })
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        var valid = true;
                        $.each(data, function(index, component) {
                            if (component === false) {
                                valid = false;
                                return false;
                            }
                        });
                        if (valid === true) {
                            self.data.licenseInfo(data);
                            deferred.resolve();
                        } else {
                            self.data.licenseInfo(undefined);
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.addlicense.gather.invalidlicense'));
                            validationResult.fields.push('licensestring');
                            deferred.reject();
                        }
                    })
                    .fail(function() {
                        self.data.licenseInfo(undefined);
                        validationResult.valid = false;
                        validationResult.reasons.push($.t('ovs:wizards.addlicense.gather.invalidlicense'));
                        validationResult.fields.push('licensestring');
                        deferred.reject();
                    })
                    .always(function() {
                        self.preValidateResult(validationResult);
                    });
            }).promise();
        };
    };
});

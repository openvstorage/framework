// Copyright 2015 iNuron NV
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
        self.next = function() {
            return true;
        };
    };
});

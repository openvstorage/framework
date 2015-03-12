// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
    '../../containers/storagerouter', '../../containers/storagedriver', './data'
], function ($, ko, shared, api, generic, StorageRouter, StorageDriver, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data = data;
        self.checkCinderHandle = undefined;
        self.checkCinderCredentialsHandle = undefined;

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
            // connection validation using filled out keystone credentials
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                    $.Deferred(function(cinderDeffered) {
                        if (self.data.hasCinder() === true && self.data.configCinder() === true) {
                            generic.xhrAbort(self.checkCinderCredentialsHandle);
                            var postData = {
                                    cinder_user: self.data.cinderUser(),
                                    cinder_password: self.data.cinderPassword(),
                                    tenant_name: self.data.cinderTenant(),
                                    controller_ip: self.data.cinderCtrlIP()
                            };
                            self.checkCinderCredentialsHandle = api.post('storagerouters/' + self.data.target().guid() + '/valid_cinder_credentials', { data: postData })
                                .then(self.shared.tasks.wait)
                                .done(function(data) {
                                    if (!data) {
                                        validationResult.valid = false;
                                        validationResult.reasons.push($.t('ovs:wizards.addvpool.gathercinder.invalidcinderinfo'));
                                        validationResult.fields.push('cinderUser');
                                        validationResult.fields.push('cinderPassword');
                                        validationResult.fields.push('cinderTenant');
                                        validationResult.fields.push('cinderCtrlIP');
                                    }
                                    cinderDeffered.resolve();
                                })
                                .fail(cinderDeffered.reject);
                        } else {
                            cinderDeffered.resolve();
                        }
                    }).promise()
                ])
                .always(function() {
                    self.preValidateResult(validationResult);
                    if (validationResult.valid) {
                        deferred.resolve();
                    } else {
                        deferred.reject();
                    }
                });
            }).promise();
        };
        self.next = function() {
            return true;
        };

        // Durandal
        self.activate = function() {
            self.data.hasCinder(undefined);
            generic.xhrAbort(self.checkCinderHandle);
            self.checkCinderHandle = api.post('storagerouters/' + self.data.target().guid() + '/check_cinder')
                .then(self.shared.tasks.wait)
                .done(function (data) {
                    if (data) {
                        self.data.hasCinder(true);
                        self.data.configCinder(true);
                     } else {
                        self.data.hasCinder(false);
                        self.data.configCinder(false);
                     }
                });
        };
    };
});

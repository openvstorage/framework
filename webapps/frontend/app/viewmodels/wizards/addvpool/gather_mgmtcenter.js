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
            return true;
        };
        self.next = function() {
            return true;
        };

        // Durandal
        self.activate = function() {
            self.getMgmtCenterInfo = api.get('storagerouters/' + self.data.target().guid() + '/get_mgmtcenter_info')
                .done(function (data) {
                    if (data.username) {
                        self.data.hasMgmtCenter(true);
                        self.data.integratemgmt(true);
                        self.data.mgmtcenter_user(data.username);
                        self.data.mgmtcenter_name(data.name);
                        self.data.mgmtcenter_ip(data.ip);
                        self.data.mgmtcenter_type(data.type);
                     } else {
                        self.data.hasMgmtCenter(false);
                        self.data.integratemgmt(false);
                     }
                });
        };
    };
});

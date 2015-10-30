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
    'knockout', 'plugins/dialog', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api',
    '../containers/license', '../wizards/addlicense/index'
], function(ko, dialog, $, shared, generic, api, License, AddLicenseWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.widgets        = [];
        self.shared         = shared;
        self.guard          = { authenticated: true };
        self.generic        = generic;
        self.licensesHandle = {};
        self.licenseHeaders = [
            { key: 'component',  value: $.t('ovs:generic.component'),  width: 250       },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: 250       },
            { key: 'metadata',   value: $.t('ovs:generic.metadata'),   width: undefined }
        ];

        // Observable
        self.licenses    = ko.observableArray([]);
        self.name        = ko.observable('').extend({ regex: /^[a-zA-Z0-9\-' .]{3,}$/ });
        self.company     = ko.observable('');
        self.email       = ko.observable().extend({ regex: /^[a-zA-Z0-9\-' .+]{3,}@[a-zA-Z0-9\-' .+]{3,}$/ });
        self.phone       = ko.observable('');
        self.agreement   = ko.observable('');
        self.newsletter  = ko.observable(true);
        self.registering = ko.observable(false);
        self.registered  = ko.observable(false);

        // Computed
        self.canRegister = ko.computed(function() {
            var valid = true, fields = [];
            if (!self.name.valid()) {
                valid = false;
                fields.push('name');
            }
            if (!self.email.valid()) {
                valid = false;
                fields.push('email');
            }
            if (!self.agreement()) {
                valid = false;
                fields.push('agreement');
            }
            return { value: valid, fields: fields };
        });
        self.hasFreeLicense = ko.computed(function() {
            if (self.shared.identification() === undefined) {
                return false;
            }
            var hasFree = false,
                freeToken = 'free_' + self.shared.identification().cluster_id;
            $.each(self.licenses(), function(index, license) {
                if (license.token() === freeToken) {
                    hasFree = true;
                    return false;
                }
            });
            return hasFree;
        });

        // Functions
        self.addLicense = function() {
            dialog.show(new AddLicenseWizard({
                modal: true
            }));
        };
        self.loadLicenses = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.licensesHandle[page])) {
                    var options = {
                        sort: 'component,name,valid_until',
                        page: page,
                        contents: '_dynamics'
                    };
                    self.licensesHandle[page] = api.get('licenses', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new License(guid);
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.register = function() {
            self.registering(true);
            var postData = {
                license_string: '',
                validate_only: '',
                registration_parameters: {
                    name: self.name(),
                    company: self.company(),
                    email: self.email(),
                    phone: self.phone(),
                    newsletter: self.newsletter()
                }
            };
            api.post('licenses', { data: postData })
                .then(self.shared.tasks.wait)
                .done(function() {
                    generic.alertSuccess($.t('ovs:licenses.request.success'), $.t('ovs:licenses.request.successmsg'));
                    self.registering(false);
                    self.registered(true);
                })
                .fail(function() {
                    generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:licenses.request.registering') }));
                    self.registering(false);
                });
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
        };
    };
});

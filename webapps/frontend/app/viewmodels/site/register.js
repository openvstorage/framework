// Copyright 2015 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'knockout', 'plugins/dialog', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api'
], function(ko, dialog, $, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.widgets        = [];
        self.shared         = shared;
        self.guard          = { authenticated: true };
        self.generic        = generic;

        // Observable
        self.name        = ko.observable('').extend({ regex: /^[a-zA-Z0-9\-' .]{3,}$/ });
        self.company     = ko.observable('');
        self.email       = ko.observable().extend({ regex: /^[a-zA-Z0-9\-' .+]{3,}@[a-zA-Z0-9\-' .+]{3,}$/ });
        self.phone       = ko.observable('');
        self.agreement   = ko.observable('');
        self.newsletter  = ko.observable(true);
        self.registering = ko.observable(false);

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

        // Functions
        self.register = function() {
            self.registering(true);
            var postData = {
                name: self.name(),
                company: self.company(),
                email: self.email(),
                phone: self.phone(),
                newsletter: self.newsletter()
            };
            api.post('register', { data: postData })
                .then(self.shared.tasks.wait)
                .done(function() {
                    generic.alertSuccess($.t('ovs:register.request.success'), $.t('ovs:register.request.successmsg'));
                    self.registering(false);
                    var current = self.shared.registration();
                    current.registered = true;
                    self.shared.registration(current);
                })
                .fail(function() {
                    generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:register.request.registering') }));
                    self.registering(false);
                    var current = self.shared.registration();
                    current.registered = false;
                    self.shared.registration(current);
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

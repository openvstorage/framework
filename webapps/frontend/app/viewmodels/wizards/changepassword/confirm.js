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
    'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, ko, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;

        // Observables
        self.currentPassword = ko.observable('');
        self.newPassword     = ko.observable('');
        self.newPassword2    = ko.observable('');

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.currentPassword() === '') {
                valid = false;
                fields.push('currentpassword');
                reasons.push($.t('ovs:wizards.changepassword.confirm.entercurrent'));
            } else if (self.currentPassword() !== self.shared.authentication.password()) {
                valid = false;
                fields.push('currentpassword');
                reasons.push($.t('ovs:wizards.changepassword.confirm.currentinvalid'));
            }
            if (self.newPassword() === '') {
                valid = false;
                fields.push('newpassword');
                reasons.push($.t('ovs:wizards.changepassword.confirm.enternew'));
            } else if (self.newPassword() !== self.newPassword2()) {
                valid = false;
                fields.push('newpassword');
                fields.push('newpassword2');
                reasons.push($.t('ovs:wizards.changepassword.confirm.shouldmatch'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                api.post('users/' + self.shared.authentication.token + '/set_password', {
                        current_password: self.currentPassword(),
                        new_password: self.newPassword()
                    })
                    .done(function() {
                        generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:generic.messages.savesuccessfully', { what: $.t('ovs:generic.password') }));
                        shared.authentication.password(self.newPassword());
                        deferred.resolve();
                    })
                    .fail(function(error) {
                        generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.changepassword.confirm.updatingpassword') }));
                        deferred.reject(error);
                    });
            }).promise();
        };
    };
});

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
    'ovs/shared', 'ovs/generic', 'ovs/api',
    './data'
], function($, ko, shared, generic, api, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data   = data;

        // Observables
        self.newPassword     = ko.observable('');
        self.newPassword2    = ko.observable('');

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
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
                api.post('users/' + data.user().guid() + '/set_password', { data: { new_password: self.newPassword() } })
                    .done(function() {
                        generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:generic.messages.savesuccessfully', { what: $.t('ovs:generic.password') }));
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

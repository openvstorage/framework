// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, ko, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        self.shared = shared;

        self.currentPassword = ko.observable('');
        self.newPassword = ko.observable('');
        self.newPassword2 = ko.observable('');
        self.canContinue = ko.computed(function() {
            if (self.currentPassword() === '') {
                return { value: false, reason: $.t('ovs:wizards.changepassword.confirm.entercurrent') };
            }
            if (self.currentPassword() !== self.shared.authentication.password()) {
                return { value: false, reason: $.t('ovs:wizards.changepassword.confirm.currentinvalid') };
            }
            if (self.newPassword() === '') {
                return { value: false, reason: $.t('ovs:wizards.changepassword.confirm.enternew') };
            }
            if (self.newPassword() !== self.newPassword2()) {
                return { value: false, reason: $.t('ovs:wizards.changepassword.confirm.shouldmatch') };
            }
            return { value: true, reason: undefined };
        });

        self.finish = function() {
            return $.Deferred(function(deferred) {
                api.post('users/' + self.shared.authentication.token + '/set_password', {
                        current_password: self.currentPassword(),
                        new_password: self.newPassword()
                    })
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

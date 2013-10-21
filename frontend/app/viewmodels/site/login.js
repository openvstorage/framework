define(['ovs/shared', 'knockout', 'ovs/authentication'], function (shared, ko, authentication) {
    "use strict";
    return {
        // System
        shared: shared,
        authentication: authentication,

        // Data
        displayname: 'Login',
        description: 'Please login into the Open vStorage management interface',
        username: ko.observable(),
        password: ko.observable(),
        loggedin: ko.observable(false),
        failed:   ko.observable(false),

        // Functions
        login: function() {
            var self = this;
            self.failed(false);
            self.authentication.login(self.username(), self.password())
                               .done(function () {
                                   self.loggedin(true);
                               })
                               .fail(function () {
                                   self.password('');
                                   self.failed(true);
                               });
        },

        // Durandal
        activate: function() {
            setTimeout(function() {
                $('#inputUsername').focus();
            }, 250);
        }
    };
});
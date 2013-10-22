define(['ovs/shared', 'knockout', 'ovs/authentication'], function (shared, ko, authentication) {
    "use strict";
    return function () {
        var self = this;

        // System
        self.shared = shared;
        self.authentication = authentication;

        // Data
        self.displayname = ko.observable('Login');
        self.description = ko.observable('Please login into the Open vStorage management interface');
        self.username = ko.observable();
        self.password = ko.observable();
        self.loggedin = ko.observable(false);
        self.failed = ko.observable(false);

        // Functions
        self.login = function() {
            self.failed(false);
            self.authentication.login(self.username(), self.password())
                               .done(function () {
                                   self.loggedin(true);
                               })
                               .fail(function () {
                                   self.password('');
                                   self.failed(true);
                               });
        };

        // Durandal
        self.activate = function () {
            setTimeout(function () {
                $('#inputUsername').focus();
            }, 250);
        };
    };
});
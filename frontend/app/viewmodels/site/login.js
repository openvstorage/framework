define([
    'jquery', 'knockout',
    'ovs/shared'
], function($, ko, shared) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared = shared;

        // Data
        self.displayName = ko.observable('Login');
        self.description = ko.observable('Please login into the Open vStorage management interface');
        self.username    = ko.observable();
        self.password    = ko.observable();
        self.loggedIn    = ko.observable(false);
        self.failed      = ko.observable(false);

        // Functions
        self.login = function() {
            self.failed(false);
            self.shared.authentication.login(self.username(), self.password())
                .done(function() {
                    self.loggedIn(true);
                })
                .fail(function() {
                    self.password('');
                    self.failed(true);
                });
        };

        // Durandal
        self.activate = function() {
            setTimeout(function() {
                $('#inputUsername').focus();
            }, 250);
        };
    };
});
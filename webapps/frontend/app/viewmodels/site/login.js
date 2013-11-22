// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout', 'plugins/router', 'plugins/history',
    'ovs/shared'
], function($, ko, router, history, shared) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared = shared;

        // Data
        self.username    = ko.observable();
        self.password    = ko.observable();
        self.loggedIn    = ko.observable(false);
        self.failed      = ko.observable(false);
        self.referrer    = undefined;

        // Functions
        self.login = function() {
            self.failed(false);
            self.shared.authentication.login(self.username(), self.password())
                .done(function() {
                    self.loggedIn(true);
                    if (self.referrer) {
                        router.navigate(self.referrer);
                    }
                })
                .fail(function() {
                    self.password('');
                    self.failed(true);
                });
        };

        // Durandal
        self.activate = function() {
            self.referrer = window.localStorage.getItem('referrer');
            setTimeout(function() {
                $('#inputUsername').focus();
            }, 250);
        };
    };
});
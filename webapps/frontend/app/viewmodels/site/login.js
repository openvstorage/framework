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
/*global define, window */
define([
    'jquery', 'knockout', 'plugins/router',
    'ovs/shared'
], function($, ko, router, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared   = shared;
        self.referrer = undefined;

        // Observables
        self.username = ko.observable();
        self.password = ko.observable();
        self.loggedIn = ko.observable(false);
        self.failed   = ko.observable(false);

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
            window.setTimeout(function() {
                $('#inputUsername').focus();
            }, 250);
        };
    };
});

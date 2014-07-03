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

// Copyright 2014 iNuron NV
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
/*global define, window */
define([
    'plugins/router', 'jquery', 'knockout',
    'ovs/generic', 'ovs/shared'
], function(router, $, ko, generic, shared){
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.onLoggedIn  = [];
        self.onLoggedOut = [];
        self.required    = false;
        self.metadata    = {};

        // Observables
        self.accessToken = ko.observable();

        // Computed
        self.loggedIn = ko.computed(function() {
            return self.accessToken() !== undefined;
        });

        // Functions
        self.login = function(username, password) {
            return $.Deferred(function(deferred) {
                var callData = {
                    type: 'post',
                    data: {
                        grant_type: 'password',
                        username: username,
                        password: password
                    },
                    contentType: 'application/x-www-form-urlencoded',
                    headers: { Accept: 'application/json; version=*' }
                };
                $.ajax('/api/oauth2/token/', callData)
                    .done(function(result) {
                        self.accessToken(result.access_token);
                        window.localStorage.setItem('accesstoken', result.access_token);
                        self.dispatch(true)
                            .always(deferred.resolve);
                    })
                    .fail(function(xmlHttpRequest) {
                        // We check whether we actually received an error, and it's not the browser navigating away
                        if (xmlHttpRequest.readyState === 4 && xmlHttpRequest.status === 502) {
                            generic.validate(shared.nodes);
                        } else if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                            self.accessToken(undefined);
                            deferred.reject({
                                status: xmlHttpRequest.status,
                                statusText: xmlHttpRequest.statusText,
                                readyState: xmlHttpRequest.readyState,
                                responseText: xmlHttpRequest.responseText
                            });
                        } else if (xmlHttpRequest.readyState === 0 && xmlHttpRequest.status === 0) {
                            generic.validate(shared.nodes);
                        }
                    });
            }).promise();
        };
        self.logout = function() {
            self.accessToken(undefined);
            self.metadata = {};
            window.localStorage.removeItem('accesstoken');
            self.dispatch(false)
                .always(function() {
                    router.navigate('');
                });
        };
        self.dispatch = function(login) {
            var i, events = [];
            if (login) {
                for (i = 0; i < self.onLoggedIn.length; i += 1) {
                    events.push(self.onLoggedIn[i]());
                }
            } else {
                for (i = 0; i < self.onLoggedOut.length; i += 1) {
                    events.push(self.onLoggedOut[i]());
                }
            }
            return $.when.apply($, events);
        };
        self.validate = function() {
            return self.loggedIn();
        };
        self.header = function() {
            return 'Bearer ' + self.accessToken();
        };
    };
});

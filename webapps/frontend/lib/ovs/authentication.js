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
    'plugins/router', 'jquery', 'knockout',
    'ovs/shared', 'ovs/api'
], function(router, $, ko,
            shared, api){
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
            // return api.post('/api/oauth2/token/', {
            //     contentType: 'application/x-www-form-urlencoded',
            //     data: {
            //         grant_type: 'password',
            //         username: username,
            //         password: password
            //     }
            // })
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
                            api.validate(shared.nodes);
                        } else if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                            self.accessToken(undefined);
                            deferred.reject({
                                status: xmlHttpRequest.status,
                                statusText: xmlHttpRequest.statusText,
                                readyState: xmlHttpRequest.readyState,
                                responseText: xmlHttpRequest.responseText
                            });
                        } else if (xmlHttpRequest.readyState === 0 && xmlHttpRequest.status === 0) {
                            api.validate(shared.nodes);
                        }
                    });
            }).promise();
        };
        self.logout = function() {
            var remote = self.metadata.mode === 'remote';
            self.accessToken(undefined);
            self.metadata = {};
            window.localStorage.removeItem('accesstoken');
            self.dispatch(false)
                .always(function() {
                    if (remote === true) {
                        window.location.href = 'https://' + window.location.host + '/';
                    } else {
                        router.navigate('');
                    }
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

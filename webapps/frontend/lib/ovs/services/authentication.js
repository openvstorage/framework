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
    'ovs/api',
    'ovs/services/release', 'ovs/services/messaging', 'ovs/services/translation', 'ovs/services/cookie',
    'viewmodels/services/user'
], function(router, $, ko,
            api,
            releaseService, messaging, translation, cookieService,
            userService){
    "use strict";

    function User(guid, username, roles) {
        var self = this;

        // Public
        self.guid = ko.observable(guid || null);
        self.username = ko.observable(username || null);
        self.roles = ko.observableArray(roles || []);

        // Computed
        self.canManage = ko.pureComputed(function() {
            return self.roles().contains('write')
        });
        self.canWrite = ko.pureComputed(function() {
            return self.roles().contains('write')
        });
        self.canRead = ko.pureComputed(function() {
            return self.roles().contains('read')
        })
    }
    /**
     * Authentication service
     * Handles authentication
     * @constructor
     */
    function Authentication() {
        var self = this;

        // Variables
        self.onLoggedIn = [
            messaging.start.call(messaging),
            function () {
                // Retrieve the current user details
                return $.when().then(function() {
                    return miscService.metadata()
                        .then(function (metadata) {
                                if (!metadata.authenticated) {
                                    // This shouldn't be the case, but is checked anyway.
                                    self.logout();
                                    throw new Error('User was not logged in. Logging out')
                                }
                                self.metadata = metadata.authentication_metadata;
                                self.user.username(metadata.username);
                                self.user.guid(metadata.userguid);
                                self.user.roles(metadata.roles);
                                releaseService.releaseName = metadata.release.name;
                                return self.user.guid()
                            })
                        })
                        .then(userService.fetchUser)
                        .then(function (data) {
                            translation.setLanguage.call(translation, data.language);
                        })
            },
            function () {  // Handle event type messages
                messaging.subscribe.call(messaging, 'EVENT', notifications.handleEvent);
            }
        ];
        self.onLoggedOut = [
            function() { translation.resetLanguage.call(translation)},
            function() { return $.when().then(function() { messaging.stop.call(messaging)})}
        ];
        self.required = false;
        self.metadata = {};
        self.user = new User();
        // Observables
        self.accessToken = ko.observable();

        // Computed
        self.loggedIn = ko.pureComputed(function () {
            return !!(self.accessToken())
        });

        // Retrieve the token once this file is loaded.
        self.retrieveToken()
    }

    Authentication.prototype = {
        /**
         * Retrieves the authentication (if any)
         */
        retrieveToken: function() {
            var token = window.localStorage.getItem('accesstoken'), state, expectedState;
            if (token === null) {
                token = cookieService.getCookie('accesstoken');
                if (token !== null) {
                    state = cookieService.getCookie('state');
                    expectedState = window.localStorage.getItem('state');
                    if (state === null || state !== expectedState) {
                        token = null;
                    } else {
                        window.localStorage.setItem('accesstoken', token);
                    }
                    cookieService.removeCookie('accesstoken');
                    cookieService.removeCookie('state');
                }
            }
            if (token !== null) {
                this.accessToken(token);
            }
            return token
        },
        /**
         * Register a new callback once the service has logged in
         * @param callback: Callback function
         */
        addLogInCallback: function(callback) {
            this.onLoggedIn.push(callback)
        },
        /**
         * Register a new callback once the services has logged out
         * @param callback: Callback function
         */
        addLogOutCallback: function(callback) {
            this.onLoggedOut.push(callback)
        },
        /**
         * Log in
         * @param username: Username to login with
         * @param password: Password to login with
         * @return {Promise<T>}
         */
        login: function(username, password) {
            var self = this;
            return api.post('/oauth2/token/', {
                contentType: 'application/x-www-form-urlencoded',
                data: {
                    grant_type: 'password',
                    username: username,
                    password: password
                }
            }).then(function(result) {
                self.accessToken(result.access_token);
                window.localStorage.setItem('accesstoken', result.access_token);
                return self.dispatch.call(self, true)
            });
        },
        /**
         * Logout of the application
         */
        logout: function() {
            var remote = this.metadata.mode === 'remote';
            this.accessToken(undefined);
            this.metadata = {};
            window.localStorage.removeItem('accesstoken');
            this.dispatch(false)
                .always(function() {
                    if (remote === true) {
                        window.location.href = 'https://' + window.location.host + '/';
                    } else {
                        router.navigate('');
                    }
                });
        },
        /**
         * Resolve all registered callbacks
         * The callbacks were either registered to happen when the user logs in or when the user logs out
         * @param login: Was the user logged in or not
         * @return {Promise<T>}
         */
        dispatch: function(login) {
            var events = [];
            var arr = this.onLoggedIn;
            if (!login) {
                arr = this.onLoggedOut;
            }
            $.each(arr, function(index, promise) {
                events.push(promise)
            });
            return $.when.apply($, events);
        },
        /**
         * Generate the beared token
         * @return {string}
         */
        generateBearerToken: function() {
            return 'Bearer ' + this.accessToken();
        }

    };
    // Return a new instance. Only one of these will be available throughout the application
    return new Authentication()
});

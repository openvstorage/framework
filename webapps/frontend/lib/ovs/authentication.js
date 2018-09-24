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

    /**
     * Authentication service
     * Handles authentication
     * @constructor
     */
    function Authentication() {
        var self = this;

        // Variables
        self.onLoggedIn = [];
        self.onLoggedOut = [];
        self.required = false;
        self.metadata = {};

        // Observables
        self.accessToken = ko.observable();

        // Computed
        self.loggedIn = ko.pureComputed(function () {
            return !!(self.accessToken())
        });
    }

    Authentication.prototype = {
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
            var i, events = [];
            if (login) {
                for (i = 0; i < this.onLoggedIn.length; i += 1) {
                    events.push(this.onLoggedIn[i]());
                }
            } else {
                for (i = 0; i < this.onLoggedOut.length; i += 1) {
                    events.push(this.onLoggedOut[i]());
                }
            }
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

// Copyright (C) 2018 iNuron NV
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
/*global define */
define(['jquery',
    'ovs/api', 'ovs/services/authentication'],
    function($,
             api, authentication) {
    "use strict";

    // Here for documentation purposes
    var ReadyStates = Object.freeze({
        UNSENT: 0,
        OPENED: 1,
        HEADERS_RECEIVED: 2,
        LOADING: 3,  // Downloading
        DONE: 4
    });
    var StatusCodes = Object.freeze({
        OK: 200,
        FORBIDDEN: 403,
        UNAUTHORIZED: 401,
        BAD_GATEWAY: 502,
        REQUEST_UNCOMPLETE: 0  // Browser implementation. When the request is not complete, the statuscode will be 0 by default
    });

    /**
     * Registers a couple of interceptors
     * Loaded on app startup
     */
    function addHTTPInterceptors(){
        /**
         * Add the authorization header is the user is currently logged in
         * This way the API service should not be aware of any authentication state
         * Resolve circular dependency of authentication <-> api
         */
        $(document).ajaxSend(function(event, jqXhr) {
            if (authentication.loggedIn()) {
                jqXhr.setRequestHeader('Authorization', authentication.generateBearerToken());
            }
        });
        /**
         * Handle ajaxErrors.
         * This way the API service should not be aware of any caveats and special error handling
         * Resolve circular dependency of authentication <-> api
         */
        $(document).ajaxError(function(event, jqXhr, settings, thrownError) {
            // Check if it is not the browser navigating away but an actual error
            if (jqXhr.readyState === ReadyStates.DONE) {
                if (jqXhr.status === StatusCodes.BAD_GATEWAY) {
                    // Current API host is not responding
                    api.failover();
                } else if ([StatusCodes.FORBIDDEN, StatusCodes.UNAUTHORIZED].contains(jqXhr.status)) {
                    var responseData = $.parseJSON(jqXhr.responseText);
                    if (responseData.error === 'invalid_token') {
                        authentication.logout();
                    }
                }
            } else if (jqXhr.readyState === ReadyStates.UNSENT && jqXhr.status === StatusCodes.REQUEST_UNCOMPLETE) {
                // Default state of an XHR. Could mean a timeout.
                // Relay might be given. The relay could have timed out because it took too long to fetch
                api.failover();
            }
            throw jqXhr;
        });
    }
    addHTTPInterceptors()
});

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
define(['jquery'],
    function($) {
    "use strict";

    var TokenStates = Object.freeze({
        PENDING: 'pending'
    });
    /**
     * Handles XHR tokens
     * These methods should be within the api.js. Currently a separate file for dependency loading within generic.js
     * @constructor
     */
    function XHRService(){ }

    var functions = {
        /**
         * Abort an xhr request
         * @param token: Token to abort
         */
        xhrAbort: function(token) {
            if (token !== undefined && token.state && token.state() === TokenStates.PENDING) {
                try {
                    token.abort();
                } catch (error) {
                    // Ignore these errors
                }
            }
        },
        /**
         * Check if the xhr is complete
         * @param token: Token to check
         * @returns {boolean}
         */
        xhrCompleted: function(token) {
            return !(token !== undefined && token.state && token.state() === TokenStates.PENDING);
        },
        /**
         * Extract the error message from an XHRRequest
         * @param error: Error object to extract message from
         * @param namespace: Error description translation path
         * @returns {string}
         */
        extractErrorMessage: function(error, namespace) {
            if (error.hasOwnProperty('responseText')) {
                try {
                    var key, message, obj = $.parseJSON(error.responseText);
                    if (obj.hasOwnProperty('error')) {
                        key = (namespace === undefined ? 'ovs' : namespace) + ':generic.api_errors.' + obj.error;
                        message = $.t(key);
                        if (message === key) {
                            if (obj.hasOwnProperty('error_description')) {
                                return obj.error_description;
                            }
                            return obj.error;
                        }
                        return message;
                    }
                    return error.responseText;
                } catch(exception) {
                    if (error.hasOwnProperty('status') && error.status === 404) {
                        return $.t((namespace === undefined ? 'ovs' : namespace) + ':generic.api_errors.not_found');
                    }
                }
            }
            return error;
        }
    };

    XHRService.prototype = $.extend({}, functions);
    return new XHRService()
});

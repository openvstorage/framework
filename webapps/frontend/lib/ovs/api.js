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
    'jquery', 'ovs/shared', 'ovs/generic'
], function($, shared, generic) {
    'use strict';
    var APITypes = Object.freeze({
        GET: 'GET',
        DELETE: 'DELETE',
        POST: 'POST',
        PUT: 'PUT',
        PATCH: 'PATCH'
    });
    // Here for documentation purposes
    var ReadyStates = Object.freeze({
        UNSENT: 0,
        OPENED: 1,
        HEADERS_RECEIVED: 2,
        LOADING: 3,  // Downloading
        DONE: 4
    });
    var StatusCodes = Object.freeze({
        FORBIDDEN: 403,
        UNAUTHORIZED: 401,
        BAD_GATEWAY: 502,
        REQUEST_UNCOMPLETE: 0  // Browser implementation. When the request is not complete, the statuscode will be 0 by default
    })
    function APIService() {

        self.default_timeout = 120 * 1000;  // Milliseconds
        self.default_relay = {relay: ''}
    }

    // Public
    var functions = {
        get: function(api, options) {
            return sendRequest.call(this, api, options, APITypes.GET);
        },
        del: function(api, options) {
            return sendRequest.call(this, api, options, APITypes.DELETE);
        },
        post: function(api, options) {
            return sendRequest.call(this, api, options, APITypes.POST);
        },
        put: function(api, options) {
            return sendRequest.call(this, api, options, APITypes.PUT);
        },
        patch: function(api, options) {
            return sendRequest.call(this, api, options, APITypes.PATCH);
        }
    };

    // Private
    /**
     * Asynchronously sleep. Used to chain methods
     * @param time: Time to sleep (milliseconds)
     * @param value: Value to resolve/reject into
     * @param reject: Reject value
     * @returns {Promise}
     */
    function delay(time, value, reject) {
        return new $.Deferred(function(deferred) {
            setTimeout(function() {
                if (reject) {
                    return deferred.reject(value)
                }
                return deferred.resolve(value)
            })
        }).promise()
    }

    function sendRequest(api, options, type) {
        var self = this;
        options = options || {};

        var querystring = [],
            deferred = $.Deferred();
        var callData = {
            type: type,
            timeout: generic.tryGet(options, 'timeout', 1000 * 120),
            contentType: 'application/json',
            headers: { Accept: 'application/json; version=*' }
        };
        var queryParams = options.queryparams || {};
        var relayParams = options.relayParams || {relay: ''};
        var data = generic.tryGet(options, 'data');

        if (generic.objectEquals(relayParams, {})) {
            relayParams = {relay: ''}
        }
        // Copy over as we will mutate these objects
        queryParams = $.extend({}, queryParams);
        relayParams = $.extend({}, relayParams);
        if (relayParams.ip !== undefined && [undefined, ''].contains(relayParams.relay)) {
            // Default relay and clean id and secret
            relayParams.relay = 'relay/';
            relayParams.client_id = relayParams.client_id.replace(/\s+/, "");
            relayParams.client_secret = relayParams.client_secret.replace(/\s+/, "");
        }
        if (relayParams.relay && !relayParams.relay.endsWith('/')) {
            relayParams.relay = relayParams.relay + '/';
        }
        // Add relay params to query params, looping in favor of extending because of the warning
        $.each(relayParams, function(key, value) {
            if (key === 'relay') { return true; }
            if (key in queryParams) {
                console.warn('Relay information is overruling the query param {}'.format([key]))
            }
            queryParams[key] = value;
        });
        queryParams.timestamp = generic.getTimestamp();
        for (var key in queryParams) {
            if (queryParams.hasOwnProperty(key)) {
                querystring.push(key + '=' + encodeURIComponent(queryParams[key]));
            }
        }
        if (type !== 'GET' || !$.isEmptyObject(data)) {
            callData.data = JSON.stringify(data);
        }
        if (shared.authentication.validate()) {
            callData.headers.Authorization = shared.authentication.header();
        }
        var start = generic.getTimestamp();
        var call = '/api/' + relayParams.relay + api + (api === '' ? '?' : '/?') + querystring.join('&');
        return $.ajax(call, callData)
            .then(function(data) {
                var timing = generic.getTimestamp() - start;
                if (timing > 1000 && log === true) {
                    generic.log('API call to ' + call + ' took ' + timing + 'ms', 'info')
                }
                return data;
            })
            .then(function(data){
                return data
            }, function(error) {
                // Check if it is not the browser navigating away but an actual error
                if (error.readyState === ReadyStates.DONE) {
                    if (error.status === StatusCodes.BAD_GATEWAY) {
                        generic.validate(shared.nodes);
                        window.setTimeout(function () {
                        deferred.reject({
                            status: error.status,
                            statusText: error.statusText,
                            readyState: error.readyState,
                            responseText: error.responseText
                        });
                    }, 11000);
                    } else if ([StatusCodes.FORBIDDEN, StatusCodes.UNAUTHORIZED].contains(error.status)) {
                        var responseData = $.parseJSON(error.responseText);
                        if (responseData.error === 'invalid_token') {
                            shared.authentication.logout();
                        }

                        deferred.reject({
                            status: error.status,
                            statusText: error.statusText,
                            readyState: error.readyState,
                            responseText: error.responseText
                        });
                    }
                } else if (error.readyState === ReadyStates.UNSENT && error.status === StatusCodes.REQUEST_UNCOMPLETE) {
                    generic.validate(shared.nodes);
                    window.setTimeout(function () {
                        deferred.reject({
                            status: error.status,
                            statusText: error.statusText,
                            readyState: error.readyState,
                            responseText: error.responseText
                        });
                    }, 11000);
                }
                // Throw it again
                throw error;
            });
    }

    APIService.prototype = Object.extend(APIService.prototype, functions);
    return new APIService();
});


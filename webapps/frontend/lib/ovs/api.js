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
    'jquery',
    'ovs/shared', 'ovs/generic',
    'ovs/services/xhr', 'ovs/services/log'
], function($,
            shared, generic,
            xhrService, logService) {
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
    });

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
        },
        /**
         * Validate which nodes are responsive and possibly migrate the UI over to a responsive one
         * @param nodes: Nodes to check
         */
        validate: function(nodes) {
            var i, node, check, checkAndRedirect;
            check = function(node) {
                return $.ajax(node + '/api/?timestamp=' + (new Date().getTime()), {
                    type: 'GET',
                    contentType: 'application/json',
                    dataType: 'json',
                    timeout: 5000,
                    headers: { Accept: 'application/json' }
                });
            };
            checkAndRedirect = function(node) {
                check(node)
                    .done(function() {
                        window.location.href = node;
                    });
            };
            check('https://' + window.location.hostname)
                .fail(function() {
                    for (i = 0; i < nodes.length; i += 1) {
                        node = nodes[i];
                        checkAndRedirect('https://' + node);
                    }
                    window.setTimeout(function() {
                        location.reload(true);
                    }, 5000);
                });
        }
    };

    // Private
    function sendRequest(api, options, type) {
        options = options || {};
        var log = generic.tryGet(options, 'log', true);
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
        var data = options.data || {};

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
                if (timing > 1000 && log) {
                    logService.log('API call to ' + call + ' took ' + timing + 'ms', 'info')
                }
                return data;
            })
            .then(function(data){
                return data
            }, function(error) {
                // Check if it is not the browser navigating away but an actual error
                if (error.readyState === ReadyStates.DONE) {
                    if (error.status === StatusCodes.BAD_GATEWAY) {
                        validate(shared.nodes);
                        return generic.delay(11 * 1000, error, true)
                    } else if ([StatusCodes.FORBIDDEN, StatusCodes.UNAUTHORIZED].contains(error.status)) {
                        var responseData = $.parseJSON(error.responseText);
                        if (responseData.error === 'invalid_token') {
                            shared.authentication.logout();
                        }
                        throw error;
                    }
                } else if (error.readyState === ReadyStates.UNSENT && error.status === StatusCodes.REQUEST_UNCOMPLETE) {
                    validate(shared.nodes);
                    return generic.delay(11 * 1000, error, true)
                }
                // Throw it again
                throw error;
            });
    }

    // Inheriting from the xhrService. Should be obsolete once all dependency loading is resolved
    APIService.prototype = $.extend(APIService.prototype, functions, xhrService.prototype);
    return new APIService();
});


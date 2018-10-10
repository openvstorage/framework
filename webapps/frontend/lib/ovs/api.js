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
    'ovs/generic',
    'ovs/services/xhr', 'ovs/services/log'
], function($,
            generic,
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
        OK: 200,
        FORBIDDEN: 403,
        UNAUTHORIZED: 401,
        BAD_GATEWAY: 502,
        REQUEST_UNCOMPLETE: 0  // Browser implementation. When the request is not complete, the statuscode will be 0 by default
    });
    var TextStates = Object.freeze({
        SUCCESS: 'success',
        ERROR: 'error',
        TIMEOUT: 'timeout'
    });

    function MockedReplies() {
        this.replies = {}
    }
    MockedReplies.prototype = {
        /**
         * Simulates a reply
         * The reply data has to be present within this object
         * @param url: URL to simulate for
         */
        simulateReply: function(url) {
            for (var key in this.replies) {
                if (!this.replies.hasOwnProperty(key)) {
                    continue
                }
                var data = this.replies[key];
                if (url.startsWith(key)){
                    if (data.error) {
                        throw data.data
                    }
                    return data.data
                }
            }
        },
        addReply: function(url, data, error) {
            if (!url.startsWith('/api')) {
                url = '/api/' + url
            }
            this.replies[url] = {data: data, error: error}
        },
        removeReply: function(url) {
            delete this.replies[url]
        }
    };
    function APIService() {
        this.testMode = false;
        this.testReplies = new MockedReplies();

        this.defaultTimeout = 120 * 1000;  // Milliseconds
        this.defaultRelay = {relay: ''};
        this.defaultContentType = 'application/json';

        this.nodes= [];
        if (window.localStorage.hasOwnProperty('nodes') && window.localStorage.nodes !== null) {
            this.nodes = $.parseJSON(window.localStorage.nodes);
        }

    }

    // Public
    var functions = {
        /**
         * Set the IP of nodes of this cluster
         * @param nodes
         */
        setNodes: function(nodes) {
            this.nodes = nodes;
            window.localStorage.setItem('nodes', JSON.stringify(nodes));
        },
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
         * Check to failover the GUI. Returns a deferred that will throw the error that was passed
         * @returns {Promise<T>}
         */
        failover: function() {
            getResponsiveNodes.call(this).then(function(candidates) {
                if (! candidates) {
                    console.error('No API is currently responsive. Reloading in the 5 seconds in hopes of fixing the issue');
                    window.setTimeout(function() {
                        location.reload(true);
                    }, 5000);
                } else {
                    if (!candidates.contains(convertToURL(window.location.hostname))) {
                        // Relay the UI to the first candidate
                        console.warn('Current API is no longer responsive. Migrating to a different host in 5 seconds');
                        window.setTimeout(function() {
                            window.location.href = candidates[0];
                        }, 5000);
                    }
                }
            });
    }
    };

    /**
     * Converts an IP to a URL
     * @param ip: IP to convert
     * @returns String
     */
    function convertToURL(ip) {
        if (!ip.startsWith('https://')) {
            return 'https://' +ip
        }
        return ip
    }

    /**
     * Validates if the host is still responsive.
     * When it resolves, the host is responsive, if it fails, the host is not
     * @param host: Host to check
     * @returns {Promise<T>}
     */
    function validateHost(host) {
        return $.ajax(host + '/api/?timestamp=' + (generic.getTimestamp()), {
            type: 'GET',
            contentType: 'application/json',
            dataType: 'json',
            timeout: 5 * 1000,
            headers: { Accept: 'application/json' }
        });
    }
    /**
     * Validate which nodes are responsive and possibly migrate the UI over to a responsive one
     * @param nodes: Nodes to check
     * @returns {Promise<Array<String>>}
     */
    function getResponsiveNodes(nodes) {
        nodes = nodes || this.nodes;

        function filterAndReturnURL() {
            var args = Array.prototype.slice.call(arguments);
            // Map and filter the results from the API calls. Map the host URL to the index
            return args.reduce(function (availableHosts, ajaxResult, index) {
                // Data returned by the ajax call is [data, textStatus, jqXHR] or [jqXHR, textStatus, errorThrown]
                var textStatus = ajaxResult[1];
                if (textStatus === TextStates.SUCCESS) {
                    availableHosts.push(convertToURL(nodes[index]))
                }
                return availableHosts
            }, [])
        }
        return generic.whenAll.apply(null, nodes.map(function (node) {
                    var hostAddress = convertToURL(node);
                    return validateHost(hostAddress);
                })).then(
                    filterAndReturnURL,  // Success
                    filterAndReturnURL)  // Error. Map all responding nodes to the url
    }
    /**
     * Sends Ajax calls
     * When testmode is enabled, it will mock the API calls instead and return
     * @param url: Url to send to
     * @param data: Data to send
     * @returns {Promise<T>}
     */
    function sendAjax(url, data) {
        var self = this;
        return $.when().then(function() {
            if (self.testMode) {
                return self.testReplies.simulateReply(url)
            }
            return $.ajax(url, data)
        })
    }
    // Private
    /**
     * Send a request towards the server
     * Authentication is handled by the http interceptor
     * Most frequent errors are handled by the http interceptor
     * @param api: API call to make
     * @param options: Options to take in consideration
     * @param type: Type of call
     * @return {Promise<T>}
     */
    function sendRequest(api, options, type) {
        var self = this;
        options = options || {};
        var log = generic.tryGet(options, 'log', true);
        var querystring = [];
        var callData = {
            type: type,
            timeout: generic.tryGet(options, 'timeout', this.defaultTimeout),
            contentType: generic.tryGet(options, 'contentType', this.defaultContentType),
            headers: { Accept: 'application/json; version=*' }
        };
        var queryParams = options.queryparams || {};
        var relayParams = options.relayParams || this.defaultRelay;
        var data = options.data || {};

        // Copy over as we will mutate these objects
        queryParams = $.extend({}, queryParams);
        relayParams = $.extend({}, relayParams);
        if (relayParams.ip && relayParams.relay) {
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
            if (callData.contentType === this.defaultContentType) {
                callData.data = JSON.stringify(data);
            } else {
                callData.data = data;
            }
        }
        var start = generic.getTimestamp();
        var relay = relayParams.relay || '';
        var call = '/api/' + relay + api + (api === '' ? '?' : '/?') + querystring.join('&');
        return sendAjax.call(this, call, callData)
            .then(function(data) {
                var timing = generic.getTimestamp() - start;
                if (timing > 1000 && log) {
                    logService.log('API call to ' + call + ' took ' + timing + 'ms', 'info')
                }
                return data;
            })
    }

    /**
     * Test the redirection functionality
     * Used for testing
     */
    function testRedirect() {
        var api = new APIService();
        api.testMode = true;
        api.testReplies.addReply('test', {readyState: ReadyStates.DONE, status: StatusCodes.BAD_GATEWAY}, true);
        api.get('test')
    }
    // Inheriting from the xhrService. Should be obsolete once all dependency loading is resolved
    APIService.prototype = $.extend({}, functions, xhrService.prototype);
    return new APIService();
});


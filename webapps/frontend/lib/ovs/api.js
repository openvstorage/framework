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
    function call(api, options, type) {
        var querystring = [], key, callData, jqXhr,
            deferred = $.Deferred(), queryparams, data;

        options = options || {};

        queryparams = generic.tryGet(options, 'queryparams', {});
        queryparams.timestamp = generic.getTimestamp();
        for (key in queryparams) {
            if (queryparams.hasOwnProperty(key)) {
                querystring.push(key + '=' + encodeURIComponent(queryparams[key]));
            }
        }

        callData = {
            type: type,
            timeout: generic.tryGet(options, 'timeout', 1000 * 120),
            contentType: 'application/json',
            headers: { Accept: 'application/json; version=*' }
        };
        data = generic.tryGet(options, 'data');
        if (type !== 'GET' || !$.isEmptyObject(data)) {
            callData.data = JSON.stringify(data);
        }
        if (shared.authentication.validate()) {
            callData.headers.Authorization = shared.authentication.header();
        }
        jqXhr = function(log) {
            var start = generic.getTimestamp(),
                call = '/api/' + api + (api === '' ? '?' : '/?') + querystring.join('&');
            return $.ajax(call, callData)
                .then(function(data) {
                    var timing = generic.getTimestamp() - start;
                    if (timing > 1000 && log === true) {
                        generic.log('API call to ' + call + ' took ' + timing + 'ms', 'info')
                    }
                    return data;
                })
                .done(deferred.resolve)
                .fail(function (xmlHttpRequest) {
                    // We check whether we actually received an error, and it's not the browser navigating away
                    if (xmlHttpRequest.readyState === 4 && xmlHttpRequest.status === 502) {
                        generic.validate(shared.nodes);
                        window.setTimeout(function () {
                            deferred.reject({
                                status: xmlHttpRequest.status,
                                statusText: xmlHttpRequest.statusText,
                                readyState: xmlHttpRequest.readyState,
                                responseText: xmlHttpRequest.responseText
                            });
                        }, 11000);
                    } else if (xmlHttpRequest.readyState === 4 && (xmlHttpRequest.status === 403 || xmlHttpRequest.status === 401)) {
                        var data = $.parseJSON(xmlHttpRequest.responseText);
                        if (data.error === 'invalid_token') {
                            shared.authentication.logout();
                        }
                        deferred.reject({
                            status: xmlHttpRequest.status,
                            statusText: xmlHttpRequest.statusText,
                            readyState: xmlHttpRequest.readyState,
                            responseText: xmlHttpRequest.responseText
                        });
                    } else if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                        deferred.reject({
                            status: xmlHttpRequest.status,
                            statusText: xmlHttpRequest.statusText,
                            readyState: xmlHttpRequest.readyState,
                            responseText: xmlHttpRequest.responseText
                        });
                    } else if (xmlHttpRequest.readyState === 0 && xmlHttpRequest.status === 0) {
                        generic.validate(shared.nodes);
                        window.setTimeout(function () {
                            deferred.reject({
                                status: xmlHttpRequest.status,
                                statusText: xmlHttpRequest.statusText,
                                readyState: xmlHttpRequest.readyState,
                                responseText: xmlHttpRequest.responseText
                            });
                        }, 11000);
                    }
                });
        }(generic.tryGet(options, 'log', true));
        return deferred.promise(jqXhr);
    }
    function get(api, options) {
        return call(api, options, 'GET');
    }
    function del(api, options) {
        return call(api, options, 'DELETE');
    }
    function post(api, options) {
        return call(api, options, 'POST');
    }
    function put(api, options) {
        return call(api, options, 'PUT');
    }
    function patch(api, options) {
        return call(api, options, 'PATCH');
    }

    return {
        get  : get,
        del  : del,
        post : post,
        put  : put,
        patch: patch
    };
});

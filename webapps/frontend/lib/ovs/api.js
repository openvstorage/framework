// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
                querystring.push(key + '=' + queryparams[key]);
            }
        }

        callData = {
            type: type,
            timeout: generic.tryGet(options, 'timeout', 1000 * 60 * 60),
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
        jqXhr = $.ajax('/api/' + api + '/?' + querystring.join('&'), callData)
            .done(deferred.resolve)
            .fail(function(xmlHttpRequest) {
                // We check whether we actually received an error, and it's not the browser navigating away
                if (xmlHttpRequest.readyState === 4 && xmlHttpRequest.status === 502) {
                    generic.validate(shared.nodes);
                    window.setTimeout(function() {
                        deferred.reject({
                            status: xmlHttpRequest.status,
                            statusText: xmlHttpRequest.statusText,
                            readyState: xmlHttpRequest.readyState,
                            responseText: xmlHttpRequest.responseText
                        });
                    }, 11000);
                } else if (xmlHttpRequest.readyState === 4 && xmlHttpRequest.status === 403 &&
                           xmlHttpRequest.responseText === '{"detail": "invalid_token"}') {
                    shared.authentication.logout();
                } else if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                    deferred.reject({
                        status: xmlHttpRequest.status,
                        statusText: xmlHttpRequest.statusText,
                        readyState: xmlHttpRequest.readyState,
                        responseText: xmlHttpRequest.responseText
                    });
                } else if (xmlHttpRequest.readyState === 0 && xmlHttpRequest.status === 0) {
                    generic.validate(shared.nodes);
                    window.setTimeout(function() {
                        deferred.reject({
                            status: xmlHttpRequest.status,
                            statusText: xmlHttpRequest.statusText,
                            readyState: xmlHttpRequest.readyState,
                            responseText: xmlHttpRequest.responseText
                        });
                    }, 11000);
                }
            });
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

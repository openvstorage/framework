// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery',
    'ovs/shared', 'ovs/generic'
], function($, shared, generic) {
    'use strict';
    function call(api, data, filter, type) {
        var querystring = [], key, callData, cookie, jqXhr,
            deferred = $.Deferred();

        filter = filter || {};
        filter.timestamp = generic.getTimestamp();
        for (key in filter) {
            if (filter.hasOwnProperty(key)) {
                querystring.push(key + '=' + filter[key]);
            }
        }

        callData = {
            type: type,
            timeout: 1000 * 60 * 60,
            contentType: 'application/json',
            data: JSON.stringify(data),
            headers: { }
        };
        cookie = generic.getCookie('csrftoken');
        if (cookie !== undefined) {
            callData.headers['X-CSRFToken'] = cookie;
        }
        if (shared.authentication.validate()) {
            callData.headers.Authorization = shared.authentication.header();
        }
        jqXhr = $.ajax('/api/internal/' + api + '/?' + querystring.join('&'), callData)
            .done(deferred.resolve)
            .fail(function(xmlHttpRequest, textStatus, errorThrown) {
                // We check whether we actually received an error, and it's not the browser navigating away
                if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                    deferred.reject(xmlHttpRequest, textStatus, errorThrown);
                }
            });
        return deferred.promise(jqXhr);
    }
    function get(api, data, filter) {
        return call(api, data, filter, 'GET');
    }
    function del(api, data, filter) {
        return call(api, data, filter, 'DELETE');
    }
    function post(api, data, filter) {
        return call(api, data, filter, 'POST');
    }

    return {
        get : get,
        del : del,
        post: post
    };
});
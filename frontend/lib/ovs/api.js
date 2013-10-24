define(['ovs/authentication', 'ovs/generic'], function(authentication, generic) {
   "use strict";
    function call(api, data, filter, type) {
        var querystring = [], key;

        filter = filter || {};
        filter.timestamp = generic.gettimestamp();
        for (key in filter) {
            if (filter.hasOwnProperty(key)) {
                querystring.push(key + '=' + filter[key]);
            }
        }

        return $.ajax('/api/internal/' + api + '/?' + querystring.join('&'), {
            type: type,
            contentType: 'application/json',
            data: JSON.stringify(data),
            headers: {
                'Authorization': authentication.header(),
                'X-CSRFToken': generic.get_cookie('csrftoken')
            }
        });
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
// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'plugins/router', 'jquery', 'knockout',
    'ovs/generic', 'ovs/shared'
], function(router, $, ko, generic, shared){
    "use strict";
    return function() {
        var self = this;

        self.onLoggedIn  = [];
        self.onLoggedOut = [];
        self.token       = undefined;
        self.required    = false;

        self.username = ko.observable();
        self.password = ko.observable();
        self.loggedIn = ko.observable(false);

        self.login = function(username, password) {
            return $.Deferred(function(deferred) {
                var callData, cookie;
                callData = {
                    type: 'post',
                    data: ko.toJSON({
                        'username': username,
                        'password': password
                    }),
                    contentType: 'application/json',
                    headers: {}
                };
                cookie = generic.getCookie('csrftoken');
                if (cookie !== undefined) {
                    callData.headers['X-CSRFToken'] = cookie;
                }
                $.ajax('/api/auth/', callData)
                    .done(function(result) {
                        var i, events = [];
                        self.token = result.token;
                        self.username(username);
                        self.password(password);
                        self.loggedIn(true);
                        for (i = 0; i < self.onLoggedIn.length; i += 1) {
                            events.push(self.onLoggedIn[i]());
                        }
                        $.when.apply($, events).always(deferred.resolve);
                    })
                    .fail(function(xmlHttpRequest) {
                        // We check whether we actually received an error, and it's not the browser navigating away
                        if (xmlHttpRequest.readyState === 4 && xmlHttpRequest.status === 502) {
                            generic.validate(shared.nodes);
                        } else if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                            self.token = undefined;
                            self.username(undefined);
                            self.password(undefined);
                            self.loggedIn(false);
                            deferred.reject();
                        } else if (xmlHttpRequest.readyState === 0 && xmlHttpRequest.status === 0) {
                            generic.validate(shared.nodes);
                        }
                    });
            }).promise();
        };
        self.logout = function() {
            var i, events = [];
            self.token = undefined;
            self.username(undefined);
            self.password(undefined);
            self.loggedIn(false);
            for (i = 0; i < self.onLoggedOut.length; i += 1) {
                events.push(self.onLoggedOut[i]());
            }
            $.when.apply($, events)
                .always(function() {
                    router.navigate('');
                });
        };
        self.validate = function() {
            return self.token !== undefined;
        };
        self.header = function() {
            return 'Token ' + self.token;
        };
    };
});

define([
    'plugins/router', 'jquery', 'knockout',
    'ovs/generic'
], function(router, $, ko, generic){
    "use strict";
    return function() {
        var self = this;

        self.onLoggedIn = [];
        self.token      = undefined;
        self.required   = false;

        self.username = ko.observable();
        self.password = ko.observable();
        self.loggedIn = ko.observable(false);

        self.login = function(username, password) {
            return $.Deferred(function(deferred) {
                var callData = {
                        type: 'post',
                        data: ko.toJSON({
                            'username': username,
                            'password': password
                        }),
                        contentType: 'application/json',
                        headers: {}
                    },
                    cookie = generic.getCookie('csrftoken');
                if (cookie !== undefined) {
                    callData.headers['X-CSRFToken'] = cookie;
                }
                $.ajax('/api/auth/', callData)
                    .done(function(result) {
                        var i;
                        self.token = result.token;
                        self.username(username);
                        self.password(password);
                        self.loggedIn(true);
                        for (i = 0; i < self.onLoggedIn.length; i += 1) {
                            self.onLoggedIn[i]();
                        }
                        deferred.resolve();
                    })
                    .fail(function(xmlHttpRequest) {
                        // We check whether we actually received an error, and it's not the browser navigating away
                        if (xmlHttpRequest.readyState !== 0 && xmlHttpRequest.status !== 0) {
                            self.token = undefined;
                            self.username(undefined);
                            self.password(undefined);
                            self.loggedIn(false);
                            deferred.reject();
                        }
                    });
            }).promise();
        };
        self.logout = function() {
            self.token = undefined;
            self.username(undefined);
            self.password(undefined);
            self.loggedIn(false);
            router.navigate('');
        };
        self.validate = function() {
            return self.token !== undefined;
        };
        self.header = function() {
            return 'Token ' + self.token;
        };
    };
});
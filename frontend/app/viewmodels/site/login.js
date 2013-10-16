define(['ovs/shared', 'knockout', 'ovs/authentication'], function (shared, ko, authentication) {
    "use strict";
    return {
        // Shared data
        shared: shared,
        authentication: authentication,
        // Data
        displayname: 'Login',
        description: 'Please login into the Open vStorage management interface',
        username: ko.observable(),
        password: ko.observable(),
        loggedin: ko.observable(false),

        // Functions
        login: function() {
            var self = this;
            self.authentication.login(self.username(), self.password())
                               .done(function () {
                                   self.loggedin(true);
                               });
        },

        // Durandal
        activate: function() { }
    };
});
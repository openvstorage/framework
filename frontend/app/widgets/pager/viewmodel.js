define(['durandal/composition'], function(composition) {
    "use strict";
    return function () {
        var self = this;

        self.activate = function (settings) {
            self.settings = settings;
        };
    };
});
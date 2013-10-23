define(['durandal/app'], function (app){
    "use strict";
    return function () {
        var self = this;

        self.init = function (load, interval) {
            self.load = load;
            self.interval = interval;
        };
        self.start = function () {
            self.refresh_timeout = window.setInterval(function () {
                self.load();
            }, self.interval);
        };
        self.stop = function () {
            if (self.refresh_timeout !== undefined) {
                window.clearInterval(self.refresh_timeout);
            }
        };
        self.run = function () {
            self.load();
        };
    };
});
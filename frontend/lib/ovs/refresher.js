define(['durandal/app'], function (app){
    "use strict";
    return function () {
        var self = this;

        self.init = function (namespace, load, interval) {
            self.load = load;
            self.interval = interval;
            self.subscr_start = app.on(namespace + '.refresher:start', self.start);
            self.subscr_stop = app.on(namespace + '.refresher:stop', self.stop);
            self.subscr_run = app.on(namespace + '.refresher:run', self.run);
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
        self.destroy = function () {
            if (self.subscr_start !== undefined) {
                self.subscr_start.off();
            }
            if (self.subscr_stop !== undefined) {
                self.subscr_stop.off();
            }
            if (self.subscr_run !== undefined) {
                self.subscr_run.off();
            }
        };
    };
});
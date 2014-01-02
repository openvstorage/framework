// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(function(){
    "use strict";
    return function() {
        var self = this;

        self.init = function(load, interval) {
            self.load = load;
            self.interval = interval;
        };
        self.start = function() {
            self.refreshTimeout = window.setInterval(function() {
                self.load();
            }, self.interval);
        };
        self.stop = function() {
            if (self.refreshTimeout !== undefined) {
                window.clearInterval(self.refreshTimeout);
            }
        };
        self.run = function() {
            self.load();
        };
    };
});
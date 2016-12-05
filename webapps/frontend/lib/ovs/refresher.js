// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define, window */
define(['jquery'], function($){
    "use strict";
    return function() {
        var self = this;

        self.refreshTimeout = undefined;
        self.skipPause = false;

        self.init = function(load, interval) {
            self.load = load;
            self.interval = interval;
            self.running = false;
        };
        self.start = function() {
            self.stop();
            self.refreshTimeout = window.setInterval(function() {
                self.run();
            }, self.interval);
        };
        self.stop = function() {
            if (self.refreshTimeout !== undefined) {
                window.clearInterval(self.refreshTimeout);
                self.refreshTimeout = undefined;
            }
        };
        self.setInterval = function(interval) {
            self.interval = interval;
            if (self.refreshTimeout !== undefined) {
                self.stop();
                self.start();
            }
        };
        self.setLoad = function(load) {
            self.load = load;
        };
        self.run = function() {
            var chainDeferred = $.Deferred(), chainPromise = chainDeferred.promise();
            chainDeferred.resolve();
            chainPromise
                .then(function() {
                    self.running = true;
                })
                .then(self.load)
                .always(function() {
                    self.running = false;
                    if (self.skipPause === true) {
                        self.skipPause = false;
                        self.run();
                    }
                });
        };
    };
});

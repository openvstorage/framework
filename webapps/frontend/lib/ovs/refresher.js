// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

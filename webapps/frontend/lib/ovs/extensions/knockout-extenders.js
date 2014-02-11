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
define(['knockout'], function(ko) {
    "use strict";
    ko.extenders.numeric = function(target, settings) {
        var computed;
        computed = ko.computed({
            read: target,
            write: function(newValue) {
                var parsedValue = parseInt(newValue, 10);
                if (isNaN(parsedValue)) {
                    parsedValue = 0;
                }
                if (settings.hasOwnProperty('min')) {
                    parsedValue = Math.max(settings.min, parsedValue);
                }
                target(parsedValue);
                target.notifySubscribers(parsedValue);
            }
        }).extend({ notify: 'always' });
        computed(target());
        return computed;
    };
});

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
/*global define, window */
define(['knockout', 'ovs/generic'], function(ko, generic) {
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
                if (settings.hasOwnProperty('max')) {
                    parsedValue = Math.min(settings.max, parsedValue);
                }
                if ((target() !== undefined ? target().toString() : 'undefined') !== newValue) {
                    target(parsedValue);
                    target.notifySubscribers(parsedValue);
                }
            }
        }).extend({ notify: 'always' });
        computed(target());
        return computed;
    };
    ko.extenders.smooth = function(target, settings) {
        var computed;
        computed = ko.computed({
            read: target,
            write: function(newValue) {
                var diff, stepSize, decimals, execute, currentValue = target();
                if (currentValue === undefined) {
                    target(newValue);
                } else {
                    diff = newValue - currentValue;
                    if (diff !== 0) {
                        decimals = Math.max(
                            (currentValue.toString().split('.')[1] || []).length,
                            (newValue.toString().split('.')[1] || []).length
                        );
                        stepSize = generic.ceil(diff / generic.tryGet(settings, 'steps', 3), decimals);
                        stepSize = stepSize === 0 ? 1 : stepSize;
                        execute = function() {
                            if (Math.abs(newValue - currentValue) > Math.abs(stepSize)) {
                                currentValue += stepSize;
                                target(currentValue);
                                window.setTimeout(execute, 75);
                            } else {
                                target(newValue);
                            }
                        };
                        window.setTimeout(execute, 75);
                    }
                }
            }
        }).extend({ notify: 'always' });
        computed(target());
        return computed;
    };
    ko.extenders.format = function(target, formatter) {
        var computed;
        computed = ko.computed({
            read: function() {
                return formatter(target());
            },
            write: target
        }).extend({ notify: 'always' });
        computed(target());
        computed.raw = target;
        return computed;
    };
    ko.extenders.regex = function(target, regex) {
        var computed, valid = ko.observable(false);
        computed = ko.computed({
            read: target,
            write: function(newValue) {
                target(newValue);
                if (newValue !== undefined) {
                    valid(newValue.match(regex) !== null);
                } else {
                    valid(false);
                }
                target.notifySubscribers(newValue);
            }
        }).extend({ notify: 'always' });
        computed(target());
        computed.valid = valid;
        return computed;
    };
});

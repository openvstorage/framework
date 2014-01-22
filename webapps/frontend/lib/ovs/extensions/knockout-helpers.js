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
define(['knockout', 'ovs/generic'], function(ko, generic) {
    "use strict";
    ko.smoothObservable = function(initialValue, formatFunction) {
        var formattedValue = ko.observable(initialValue),
            rawValue = ko.observable(initialValue), computed;
        computed = ko.computed({
            read: function() {
                return formattedValue();
            },
            write: function(newValue) {
                generic.smooth(formattedValue, rawValue(), newValue, 3, formatFunction);
                rawValue(newValue);
            }
        });
        computed.raw = rawValue;
        return computed;
    };
    ko.deltaObservable = function(formatFunction) {
        var formattedValue = ko.observable(), rawValue = ko.observable(), initialized = ko.observable(false),
            timestamp, newTimestamp, previousCounter, delta, timeDelta, result, newRaw;
        result = ko.computed({
            read: function() {
                return formattedValue();
            },
            write: function(newCounter) {
                newTimestamp = (new Date()).getTime();
                if (typeof newCounter === 'object') {
                    newTimestamp = newCounter.timestamp;
                    newCounter = newCounter.value;
                }
                if ((typeof previousCounter) === 'undefined') {
                    previousCounter = newCounter;
                    timestamp = newTimestamp;
                } else {
                    delta = newCounter - previousCounter;
                    timeDelta = (newTimestamp - timestamp) / 1000;
                    if (timeDelta > 0) {
                        newRaw = Math.max(0, delta / timeDelta);
                        rawValue(newRaw);
                        if (formatFunction.call) {
                            formattedValue(formatFunction(newRaw));
                        } else {
                            formattedValue(newRaw);
                        }
                        timestamp = newTimestamp;
                        previousCounter = newCounter;
                        initialized(true);
                    }
                }
            }
        });
        result.initialized = initialized;
        result.raw = rawValue;
        return result;
    };
    ko.smoothDeltaObservable = function(formatFunction) {
        var formattedValue = ko.observable(), rawValue = ko.observable(), initialized = ko.observable(false),
            timestamp, newTimestamp, previousCounter, delta, timeDelta, newValue, result;
        result = ko.computed({
            read: function() {
                return formattedValue();
            },
            write: function(newCounter) {
                newTimestamp = (new Date()).getTime();
                if (typeof newCounter === 'object') {
                    newTimestamp = newCounter.timestamp;
                    newCounter = newCounter.value;
                }
                if ((typeof previousCounter) === 'undefined') {
                    previousCounter = newCounter;
                    timestamp = newTimestamp;
                } else {
                    delta = newCounter - previousCounter;
                    timeDelta = (newTimestamp - timestamp) / 1000;
                    if (timeDelta > 0) {
                        newValue = Math.max(0, delta / timeDelta);
                        generic.smooth(formattedValue, rawValue(), newValue, 3, formatFunction);
                        rawValue(newValue);
                        timestamp = newTimestamp;
                        previousCounter = newCounter;
                        initialized(true);
                    }
                }
            }
        });
        result.initialized = initialized;
        result.raw = rawValue;
        return result;
    };
});

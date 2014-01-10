// license see http://www.openvstorage.com/licenses/opensource/
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

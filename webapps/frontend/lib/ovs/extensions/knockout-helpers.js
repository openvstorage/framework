/*global define, window */
define(['knockout', 'ovs/generic'], function(ko, generic) {
    "use strict";
    ko.smoothObservable = function(initialValue, steps) {
        var workingValue = ko.observable(initialValue);
        return ko.computed({
            read: function() {
                return workingValue();
            },
            write: function(newValue) {
                generic.smooth(workingValue, newValue, steps);
            }
        });
    };
    ko.deltaObservable = function() {
        var workingValue = ko.observable(), initialized = ko.observable(false),
            timestamp, newTimestamp, previousCounter, delta, timeDelta, result;
        result = ko.computed({
            read: function() {
                return workingValue();
            },
            write: function(newCounter) {
                if ((typeof previousCounter) === 'undefined') {
                    previousCounter = newCounter;
                    timestamp = (new Date()).getTime();
                } else {
                    delta = newCounter - previousCounter;
                    newTimestamp = (new Date()).getTime();
                    timeDelta = (newTimestamp - timestamp) / 1000;
                    workingValue(Math.round((delta / timeDelta) * 100) / 100);
                    timestamp = newTimestamp;
                    previousCounter = newCounter;
                    initialized(true);
                }
            }
        });
        result.initialized = initialized;
        return result;
    };
    ko.smoothDeltaObservable = function() {
        var workingValue = ko.observable(), initialized = ko.observable(false),
            timestamp, newTimestamp, previousCounter, delta, timeDelta, result;
        result = ko.computed({
            read: function() {
                return workingValue();
            },
            write: function(newCounter) {
                if ((typeof previousCounter) === 'undefined') {
                    previousCounter = newCounter;
                    timestamp = (new Date()).getTime();
                } else {
                    delta = newCounter - previousCounter;
                    newTimestamp = (new Date()).getTime();
                    timeDelta = (newTimestamp - timestamp) / 1000;
                    generic.smooth(workingValue, Math.round((delta / timeDelta) * 100) / 100);
                    timestamp = newTimestamp;
                    previousCounter = newCounter;
                    initialized(true);
                }
            }
        });
        result.initialized = initialized;
        return result;
    };
});

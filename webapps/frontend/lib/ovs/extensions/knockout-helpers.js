/*global define */
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
    ko.deltaObservable = function(decimals) {
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
                    workingValue(generic.round(delta / timeDelta, decimals));
                    timestamp = newTimestamp;
                    previousCounter = newCounter;
                    initialized(true);
                }
            }
        });
        result.initialized = initialized;
        return result;
    };
    ko.smoothDeltaObservable = function(decimals) {
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
                    generic.smooth(workingValue, generic.round(delta / timeDelta, decimals));
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

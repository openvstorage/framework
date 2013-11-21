/*global define, window */
define(['knockout'], function(ko) {
    "use strict";
    ko.smoothObservable = function(initialValue, steps) {
        var stepsSetting = steps,
            workingValue = ko.observable(initialValue);
        return ko.computed({
            read: function() {
                return workingValue();
            },
            write: function(newValue) {
                var startValue, diff, stepSize, decimals, execute, initial;
                if (stepsSetting === undefined) {
                    stepsSetting = 3;
                }
                initial = (typeof workingValue()) === 'undefined';
                if (initial) {
                    workingValue(newValue);
                } else {
                    startValue = workingValue() || 0;
                    diff = newValue - startValue;
                    if (diff !== 0) {
                        decimals = Math.max((startValue.toString().split('.')[1] || []).length, (newValue.toString().split('.')[1] || []).length);
                        stepSize = decimals === 0 ? Math.round(diff / steps) : Math.round(diff / steps * (10 * decimals)) / (10 * decimals);
                        execute = function() {
                            var current = workingValue();
                            if (Math.abs(newValue - current) > Math.abs(stepSize)) {
                                workingValue(workingValue() + stepSize);
                                window.setTimeout(execute, 75);
                            } else {
                                workingValue(newValue);
                            }
                        };
                        window.setTimeout(execute, 75);
                    }
                }
            }
        });
    };
    ko.deltaObservable = function() {
        var workingValue = ko.observable(),
            timestamp, newTimestamp, previousCounter, delta, timeDelta;
        return ko.computed({
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
                }
            }
        });
    };
});

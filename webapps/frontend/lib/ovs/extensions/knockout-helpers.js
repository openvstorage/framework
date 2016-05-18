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
/*global define */
define(['knockout'], function(ko) {
    "use strict";
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
                    if (timeDelta <= 0) {
                        timeDelta = 1;
                    }
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
        });
        result.initialized = initialized;
        result.raw = rawValue;
        return result;
    };
    ko.splitRows = function(columns, array) {
        return ko.computed(function () {
            var result = [], row;
            // Loop through items and push each item to a row array that gets pushed to the final result
            for (var i = 0, j = array().length; i < j; i++) {
                if (i % columns === 0) {
                    if (row) {
                        result.push({ items: row });
                    }
                    row = [];
                }
                row.push(array()[i]);
            }
            // Push the final row
            if (row) {
                result.push({ items: row });
            }
            return result;
        });
    };
});

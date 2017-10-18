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
define(['knockout', 'ovs/generic'], function(ko, generic) {
    "use strict";
    /**
     * Extend a numeric observable to set a minimum/maximum and potentially validate on it
     * @param target: The target Observable (provided by .extend method)
     * @param settings: Settings to take in (Supplied, possible settings are:
     * {min: minimal numeric value,
      * max : maximal numeric value,
      * allowUndefined: set to undefined instead of min when it is not defined
      * validateMin: validate instead of setting to min
      * validateMax: validate instead of setting to max
     * @return {*|void}
     */
    ko.extenders.numeric = function(target, settings) {
        var valid = ko.observable(true);
        var computed = ko.computed({
            read: target,
            write: function(newValue) {
                var parsedValue = parseInt(newValue, 10);
                if (isNaN(parsedValue)) {
                    if (settings.hasOwnProperty('allowUndefined') && settings.allowUndefined === true) {
                        if (target() === undefined) {
                            return;
                        }
                        target(undefined);
                        target.notifySubscribers(undefined);
                        return;
                    }
                    parsedValue = 0;
                }
                if (computed.hasOwnProperty('min') && computed.min !== undefined) {
                    if (settings.hasOwnProperty('validate') && settings.validate === true) {
                        valid(parsedValue < computed.min)
                    } else {
                        parsedValue = Math.max(computed.min, parsedValue);
                    }
                }
                if (computed.hasOwnProperty('max') && computed.max !== undefined) {
                    if (settings.hasOwnProperty('validate') && settings.validate === true) {
                        valid(parsedValue > computed.max)
                    } else {
                        parsedValue = Math.min(computed.max, parsedValue);
                    }
                }
                if ((target() !== undefined ? target().toString() : 'undefined') !== newValue) {
                    target(parsedValue);
                    target.notifySubscribers(parsedValue);
                }
            }
        }).extend({ notify: 'always' });
        computed.min = generic.tryGet(settings, 'min', undefined);
        computed.max = generic.tryGet(settings, 'max', undefined);
        computed.valid = valid;
        computed(target());
        return computed;
    };
    ko.extenders.smooth = function(target, settings) {
        var computed;
        computed = ko.computed({
            read: target,
            write: function(newValue) {
                var diff, stepSize, decimals, execute, currentValue = target();
                if (currentValue === undefined || currentValue === null) {
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
                        execute = function(safety) {
                            if (Math.abs(newValue - currentValue) > Math.abs(stepSize) && safety >= 0) {
                                currentValue += stepSize;
                                target(currentValue);
                                window.setTimeout(function() { execute(safety - 1); }, 75);
                            } else {
                                target(newValue);
                            }
                        };
                        window.setTimeout(function() { execute(stepSize); }, 75);
                    }
                }
            }
        }).extend({ notify: 'always' });
        computed(target());
        return computed;
    };
    ko.extenders.format = function(target, formatter) {
        var computed = ko.computed({
            read: function() {
                return formatter(target());
            },
            write: target
        }).extend({ notify: 'always' });
        computed(target());
        computed.raw = target;
        return computed;
    };
    ko.extenders.removeWhiteSpaces = function(target) {
        if (target() === undefined) {
            return target
        }
        var computed = ko.computed({
            read: target,
            write: function(newValue) {
                target(newValue.replace(/ /g, ''))
            }
        }).extend({ notify: 'always' });
        computed(target());
        computed.raw = target;
        return computed;
    };
    ko.extenders.regex = function(target, regex) {
        var computed, valid = ko.observable(false), optional = false;
        if (regex.hasOwnProperty("optional")) {
            optional = regex.optional;
            regex = regex.regex;
        }

        computed = ko.computed({
            read: target,
            write: function(newValue) {
                target(newValue);
                if (newValue !== undefined) {
                    valid(newValue.match(regex) !== null);
                } else {
                    valid(optional);
                }
                target.notifySubscribers(newValue);
            }
        }).extend({ notify: 'always' });
        computed(target());
        computed.valid = valid;
        return computed;
    };
    ko.extenders.identifier = function(target, identifier) {
        target.identifier = identifier;
        return target;
    };
});

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
/*global define, window, document, location */
define(['jquery', 'knockout',
        'ovs/services/notifications', 'ovs/services/log', 'ovs/services/xhr'],
    function($, ko,
             notificationsService, logService, xhrService) {
    "use strict";

    /**
     * Generic Service which holds generic methods
     * Wraps around the pnotify plugin for jquery
     * @constructor
     */
    function GenericService() {
        // Add a number of methods to the prototypes of built in objects
        GenericService.prototype.patchPrototypes()
    }

    // Public
    var properties = {
        ipRegex: /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/,
        hostRegex: /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/,
        nameRegex: /^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$/,
        vdiskNameRegex: /^[0-9a-zA-Z][\-_a-zA-Z0-9]+[a-zA-Z0-9]$/
    };

    var patchFunctions = {
        /**
         * Adds a number of methods to the built in objects
         */
        patchPrototypes: function(){
            /**
             * Retrieve an item at the requested index
             * @param index: Index to search item om
             * @param prop: Optional: property to retrieve instead of the full object
             */
            Array.prototype.getItemUnwrap = function(index, prop) {
                var foundItem = undefined;
                if (index > this.length - 1 || index < 0){
                    return foundItem;
                }
                foundItem = ko.utils.unwrapObservable(this[index]);
                if (typeof prop !== 'undefined') {
                    foundItem = ko.utils.unwrapObservable(foundItem[prop]);
                }
                return foundItem;
            };
            /**
             * Only works on a sorted list
             * Returns the index of the item search for or -1 if not found
             * Faster than indexing or contains
             * Worst case time: O(log(n)
             * @param value: Value to look for
             * @param prop: Optional prop to get from the found value
             * @returns {Number}
             */
            Array.prototype.brSearch = function(value, prop) {
                if (this.length === 0) {
                    return -1;
                }
                var middleIndex = Math.floor(this.length / 2);
                var middleItem = this.getItemUnwrap(middleIndex, prop);
                if (this.length === 1 && middleItem !== value) {
                    return -1;  // Item not present
                }
                if (value === middleItem) {
                    return middleIndex;
                }
                if (value > middleItem) {
                    var additionalIndex = this.slice(middleIndex + 1, this.length).brSearch(value, prop);
                    if (additionalIndex === -1) {
                        return -1;
                    }
                    return middleIndex + 1 + additionalIndex;
                }
                return this.slice(0, middleIndex).brSearch(value, prop);
            };
            /**
             * Only works on a sorted list
             * Returns the index of the 1st element found (in case multiple identical would be present) or -1 if none found
             * Faster than indexing or contains
             * Worst case time: O(log(n)
             * @param value: Value to look for
             * @param prop: Optional prop to get from the found value
             * @param start: Start index to start looking for the value
             * @param end: End index to stop looking for the value
             * @returns {Number}
             */
            Array.prototype.brSearchFirst = function(value, prop, start, end) {
                start = start === undefined ? 0 : start;
                end = end === undefined ? this.length - 1 : end;
                if (end < start) {
                    return -1;
                }
                var middleIndex = Math.floor(start + (end - start) / 2);
                var middleItem = this.getItemUnwrap(middleIndex, prop);
                var previousItem = this.getItemUnwrap(middleIndex - 1, prop);
                if ((previousItem === undefined || value > previousItem) && middleItem === value) {
                    return middleIndex;
                }
                if (value > middleItem) {
                    // Don't use slice here since we potentially lose duplicate values
                    return this.brSearchFirst(value, prop, (middleIndex + 1), end);
                }
                return this.brSearchFirst(value, prop, start, (middleIndex - 1));
            };
            /**
             * Only works on a sorted list
             * Returns the index of the last element found (in case multiple identical would be present) or -1 if none found
             * Faster than indexing or contains
             * Worst case time: O(log(n)
             * @param value: Value to look for
             * @param prop: Optional prop to get from the found value
             * @param start: Start index to start looking for the value
             * @param end: End index to stop looking for the value
             * @returns {Number}
             */
            Array.prototype.brSearchLast = function(value, prop, start, end) {
                start = start === undefined ? 0 : start;
                end = end === undefined ? this.length - 1 : end;
                if (end < start) {
                    return -1;
                }
                var middleIndex = Math.floor(start + (end - start) / 2);
                var middleItem = this.getItemUnwrap(middleIndex, prop);
                var nextItem = this.getItemUnwrap(middleIndex + 1, prop);
                if ((nextItem === undefined || value < nextItem) && middleItem === value) {
                    return middleIndex;
                }
                if (value < middleItem) {
                    // Don't use slice here since we potentially lose duplicate values
                    return this.brSearchLast(value, prop, start, (middleIndex - 1));
                }
                return this.brSearchLast(value, prop, (middleIndex + 1), end);
            };
            /**
             * Check if the arrays are identical
             * @param other: Other array to check
             * @returns {boolean}
             */
            Array.prototype.equals = function(other) {
                if (!other) {
                    return false;
                }
                if (this.length !== other.length) {
                    return false;
                }
                return this.every(function(element, index, self) {
                    if (element instanceof Array && other[index] instanceof Array) {
                        return element.equals(other[index])
                    }
                    return element === other[index]
                });
            };
            /**
             * Check if the array contains the given element
             * Alternative to indexOf. Faster on smaller arrays
             * @param element: Element to look for in the array
             * @returns {boolean}
             */
            Array.prototype.contains = function(element) {
                for (var i = 0; i < this.length; i += 1) {
                    if (element === this[i]) {
                        return true;
                    }
                }
                return false;
            };
            /**
             * Removes an element from the list
             * @param element: Element to remove
             */
            Array.prototype.remove = function(element) {
                var index = this.indexOf(element);
                if (index > -1) {
                    this.splice(index, 1);
                }
            };
            Array.prototype.getUnqiue = function() {
                return this.filter(function(item, pos, self) {
                    return self.indexOf(item) === pos;
                });
            };
            /**
             * Check if the array contains an element where an element has a property with a value
             * @param property: Property to check
             * @param value: Value that the property should have
             * @returns {boolean}
             */
            Array.prototype.hasElementWithProperty = function(property, value) {
                return this.some(function(element) {
                    return element.hasOwnProperty(property) && element[property] === value
                });
            };
            /**
             * Format a string. Strings to format should contain '{INDEX}'
             * The {} parts will be substituted by the string supplied in the arguments array
             * Accepts any number of arguments. The position of the arguments is used for substitutions
             * @returns {string}
             */
            String.prototype.format = function () {
                var args = Array.prototype.slice.call(arguments);
                return this.replace(String.prototype.format.regex, function(item) {
                    var intVal = parseInt(item.substring(1, item.length - 1));
                    var replace;
                    if (intVal >= 0) {
                        replace = args[intVal];
                    } else if (intVal === -1) {
                        replace = "{";
                    } else if (intVal === -2) {
                        replace = "}";
                    } else {
                        replace = "";
                    }
                    return replace;
                });
            };
            /**
             * Add the regex prop to the format function. This will avoid re-declarating the same regex over and over
             * @type {RegExp}
             */
            String.prototype.format.regex = new RegExp("{-?[0-9]+}", "g");
            /**
             * Stringifies the object and avoids circular references
             * Proven useful while debugging complex object
             * @param obj: Object to stringify
             * @param replacer: Replacer function which accepts a key value pair (see stringify doc)
             * @param space: Identation space (see stringify doc)
             * @returns {string}
             */
            JSON.stringifyOnce = function (obj, replacer, space) {
                var cache = [];
                var json = JSON.stringify(obj, function(key, value) {
                    if (typeof value === 'object' && value !== null) {
                        if (cache.indexOf(value) !== -1) {
                            // Circular reference found, discard key
                            return;
                        }
                        // Store value in our collection
                        cache.push(value);
                    }
                    return replacer ? replacer(key, value) : value;
                }, space);
                cache = null;
                return json;
            };
        }
    };
    var promiseFunctions = {
        /**
         * Determine if the passed object is a promise
         * @param obj: Object to test
         * @returns {boolean}
         */
        isPromise: function(obj) {
            return obj && typeof obj.then === 'function' &&
              String($.Deferred().then) === String(obj.then);
        },
        /**
         * Asynchronously sleep. Used to chain methods
         * @param time: Time to sleep (milliseconds)
         * @param value: Value to resolve/reject into
         * @param reject: Reject value
         * @returns {Promise}
         */
        delay: function(time, value, reject) {
            return new $.Deferred(function(deferred) {
                setTimeout(function() {
                    if (reject) {
                        return deferred.reject(value)
                    }
                    return deferred.resolve(value)
                }, time)
            }).promise()
        },
        /**
         * Chain promises more neatly instead of writing .then yourselves
         * Used the .then(function()) {return new Promise)
         * All of the data of the previous callback can be used in the next one (eg. chainPromises([api.get('test'), function(testAPIData) { console.log(testAPIData}]
         * Calling .done on the return value will ensure that all previous chained promises have been completed
         * @param callbackList: list of callbacks to use
         */
        chainPromises: function(callbackList) {
            return callbackList.reduce(function(chain, func){
                chain ? chain.then(func) : func();
            }, null)
        },
        /**
         * Augmentation of $.when. $.when aborts when one of the supplied arguments would reject
         * This variation returns all responses of every promise
         * Behaviour:
         *  - done: Called when all promises are resolved
         *  - Called when all promises have a final state (resolved/rejected) and at least one of them is rejected.
         *  - Always: Called when all promises have a final state (resolved/rejected)
         *  - progress: Initially called when at least one promise notified a "progress" by deferred.notify()
         *  and all other promises are either resolved/rejected/progressed.
         *  After that, this callback will continue to be invoked for each individual pending promise "progress" notification
         *  until all of them are finalized.
         */
        whenAll: function() {
            var masterDeferred = $.Deferred();
            var deferreds = [];
            var progressDeferreds = [];
            var allFulfilled = false;  // Track if all passed promises are done
            var initialProgress = true;
            var hasRejected = false;  // Track if any promise rejected

            /**
             * Notifies the master deferred, which is the result of all passed promises
             * Notifying calls all callbacks hooked to this deferred, resolving the current promises
             */
			function notifyMasterDeferred() {
			    var args;
			    if (progressDeferreds.length > 1) {
			        // Notify for every progressDeferred. progressDeferred.__latestArgs__ contains the result of the promise
                    // and these results are subsequently used to notify
			        args = $.makeArray(progressDeferreds).map(function(progressDeferred) {
			            if (progressDeferred.__latestArgs__.length > 1) {
			                return $.makeArray(progressDeferred.__latestArgs__)
			            }
			            else if (progressDeferred.__latestArgs__.length === 1) {
			                return progressDeferred.__latestArgs__[0]
			            }
			            return void 0  // Undefined. Used to combat undefined = 1 overriden
                    })
                } else {
			        // Results were passed for a single promise. Notify used the results
			        args = arguments
                }
				masterDeferred.notify.apply(masterDeferred, args);
			}

            /**
             * Resolve the given deferred. Used as callback for each promise passed to the parent function
             * Resolved progressDeferred which are used to track the total state of all promises
             * @param progressDeferred: Deferred to track progress
             * @param deferred: Deferred that wraps around the promise
             */
			function afterFulfillment(progressDeferred, deferred) {
			    var args = Array.prototype.slice.call(arguments, 2); // Additional arguments. Contains results of the promise
                deferred.resolve.apply(deferred, args);
                if (initialProgress) {
                    progressDeferred.__latestArgs__ = args;
                    progressDeferred.resolve.apply(progressDeferred, args);
                }
            }

            // Track state of every promise
            $.each(arguments, function(index, promise) {
                var deferred = $.Deferred();  // Deferred to track if the API call succeeded
                var progressDeferred = $.Deferred();  // Deferred to track the progress of the API calls
                if (promise && promise.then) {
                    promise.then(
                        function() {
                            var args = Array.prototype.slice.call(arguments);
                            afterFulfillment.apply(this, [progressDeferred, deferred].concat(args)) // Success
                        },
                        function() {  // Fail
                            var args = Array.prototype.slice.call(arguments);
                            hasRejected = true;
                            afterFulfillment.apply(this, [progressDeferred, deferred].concat(args));
                        },
                        function() {  // Progress notification
                            progressDeferred.__latestArgs__ = arguments;
                            if (initialProgress) {
                                // Progress received while there are still promises who aren't fulfilled/progressed
                                progressDeferred.resolve.apply(progressDeferred, arguments);
                            }
                            // All promises are either fulfilled or progressed. Notify the progress of this promise
                            else {
                                notifyMasterDeferred.apply(null, arguments);
                            }
                        }
                    );
                }
                else {
                    // Passed argument was not a promise
                    deferred.resolve(promise);
                    progressDeferred.__latestArgs__ = [promise];
                    progressDeferred.resolve(promise);
                }
                deferreds.push(deferred);
                progressDeferreds.push(progressDeferred);
            });

            $.when.apply($, deferreds).done(function() {
                allFulfilled = true;
                var fn;
                if (hasRejected) {
                    // One or more promise was reject. Reject the whole master promise
                    fn = masterDeferred.reject;
                } else {
                    fn = masterDeferred.resolve;
                }
                // Reject or resolve with the results of every promise
                fn.apply(masterDeferred, arguments);
            });
            /*
             If all progressDeferred are done, the master deferred should send out a progressed signal.
             */
            $.when.apply($, progressDeferreds).done(function() {
                if (!allFulfilled) {
                    // Not all promises have been completed but progress notifications were for every call:
                    // propagate these notifications through the masterDeferred
                    notifyMasterDeferred.apply(null, arguments);
                    initialProgress = false;
                }
            });
            return masterDeferred.promise();
        }
    };
    var formatFunction = {
        /**
         * Round a numeric value down to a number of decimals to display
         * @param value: Value to round
         * @param decimals: Number of decimals to display
         * @returns {number}
         */
        round: function(value, decimals) {
            decimals = decimals || 0;
            if (decimals === 0) {
                return Math.round(value);
            }
            var factor = Math.pow(10, decimals);
            return Math.round(value * factor) / factor;
        },
        /**
         * Round a numeric value up to a number of decimals to display
         * @param value: Value to ceil
         * @param decimals: Number of decimals to display
         * @returns {number}
         */
        ceil: function(value, decimals) {
            decimals = decimals || 0;
            if (decimals === 0) {
                return Math.ceil(value);
            }
            var factor = Math.pow(10, decimals);
            return Math.ceil(value * factor) / factor;
        },
        /**
         * Parse a numeric value to a string contains the requested amount of decimals
         * @param value: Value to parse
         * @param decimals: Number of decimals to use
         * @returns {string}
         */
        setDecimals: function(value, decimals) {
            decimals = decimals || 2;
            var parts = [];
            if (isNaN(value)) {
                parts = ["0"];
            } else {
                parts = value.toString().split('.');
            }

            if (decimals <= 0) {
                return parts[0];
            }
            if (parts.length === 1) {
                var i, newString = '';
                for (i = 0; i < decimals; i += 1) {
                    newString += '0';
                }
                parts.push(newString);
            }
            while (parts[1].length < decimals) {
                parts[1] = parts[1] + '0';
            }
            return parts[0] + '.' + parts[1];
        },
        /**
         * Format the number of bytes to a readable format
         * @param value: Byte value
         * @returns {string}
         */
        formatBytes: function(value) {
            var units, counter;
            units = ['b', 'kib', 'mib', 'gib', 'tib', 'pib'];
            counter = 0;
            while (value >= 1000) {
                value = value / 1024;
                counter += 1;
            }
            return GenericService.prototype.setDecimals(GenericService.prototype.round(value, 2), 2) + ' ' + $.t('ovs:generic.units.' + units[counter]);
        },
        /**
         * Format a number of bytes /s to a readable format
         * @param value: Byte value
         * @returns {string}
         */
        formatSpeed: function(value) {
            var units, counter;
            units = ['b', 'kib', 'mib', 'gib', 'tib', 'pib'];
            counter = 0;
            while (value >= 1000) {
                value = value / 1024;
                counter += 1;
            }
            return GenericService.prototype.setDecimals(GenericService.prototype.round(value, 2), 2) + ' ' + $.t('ovs:generic.units.' + units[counter] + 's');
        },
        /**
         * Formats a value to contain a seperator which makes bigger numbers easier to read
         * @param value: Number value
         * @returns {string}
         */
        formatNumber: function(value) {
            if (typeof value !== "undefined") {
                value = GenericService.prototype.round(value).toString();
                var regex = /(\d+)(\d{3})/;
                while (regex.test(value)) {
                    value = value.replace(regex, '$1' + $.t('ovs:generic.thousandseparator') + '$2');
                }
            }
            return value;
        },
        /**
         * Format a percentage
         * @param value: Percentage value to format
         * @param allowNan: Convert NaN values to 0 %
         * @returns {string}
         */
        formatPercentage: function(value, allowNan) {
            if (isNaN(value)) {
                if (!allowNan) {
                    throw Error('Non-numeric value passed to format')
                }
                return "0 %";
            }
            value = Math.round(value * 10000) / 100;
            return GenericService.prototype.formatNumber(value) + ' %';
        },
        /**
         * Smooth out number transitions
         * @param observable: Observable ojbect to smooth out
         * @param initialValue: Initial value of the observable
         * @param targetValue: Value to smooth to
         * @param steps: Number of steps to take
         * @param formatFunction: Function to format the value with
         */
        smooth: function(observable, initialValue, targetValue, steps, formatFunction) {
            formatFunction = GenericService.prototype.isFunction(formatFunction) ? formatFunction: function(x) { return x};
            var diff, stepSize, decimals, execute, current = initialValue;
            if (initialValue === undefined) {
                observable(formatFunction(targetValue));
            } else {
                diff = targetValue - initialValue;
                if (diff !== 0) {
                    decimals = Math.max((initialValue.toString().split('.')[1] || []).length, (targetValue.toString().split('.')[1] || []).length);
                    stepSize = GenericService.prototype.ceil(diff / steps, decimals);
                    stepSize = stepSize === 0 ? 1 : stepSize;
                    execute = function() {
                        if (Math.abs(targetValue - current) > Math.abs(stepSize)) {
                            current += stepSize;
                            observable(formatFunction(current));
                            window.setTimeout(execute, 75);
                        } else {
                            observable(formatFunction(targetValue));
                        }
                    };
                    window.setTimeout(execute, 75);
                }
            }
        },
        /**
         * Pads any number of characters until the length is met
         * @param value: Value to pad onto
         * @param character: Character to pad with
         * @param length: Length to match
         * @returns {string}
         */
        padRight: function(value, character, length) {
            while (value.length < length) {
                value += character;
            }
            return value;
        }
    };
    var objectFunctions = {
        tryGet: function(object, key, fallback) {
            if (object !== undefined && object.hasOwnProperty(key)) {
                return object[key];
            }
            return fallback;
        },
        trySet: function(observable, object, key, formatFunction) {
            if (object !== undefined && object.hasOwnProperty(key)) {
                if (GenericService.prototype.isFunction(formatFunction)) {
                    observable(formatFunction(object[key]));
                } else {
                    observable(object[key]);
                }
            }
        },
        objectEquals: function(object1, object2) {
            var self = this;
            // If both object1 and object2 are null or undefined and exactly the same
            var objectValuesEqual= function() {
                return Object.keys(object1).every(function(element, index, arr) {
                    return object1.hasOwnProperty(element)
                        && object2.hasOwnProperty(element)
                        && (object1[element] === object2[element]
                            || (self.isObject(object1[element])
                                && self.objectEquals(object1[element], object2[element])))
                })
            };

            return object1 === object2
                || ((self.isObject(object1) && GenericService.prototype.isObject(object2))
                    && object1.constructor === object2.constructor
                    && Object.keys(object1).equals(Object.keys(object2))
                    && objectValuesEqual());
        },
        /**
         * Sorts on objects keys.
         * By convention, most browsers will retain the order of keys in an object in the order that they were added.
         * But don't expect it to always work
         * @param object: object to sort
         * @param func: sorting function
         * @returns {{}}
         */
        sortObject: function(object, func) {
            var sorted = {},
                key, array = [];

            for (key in object) {
                if (object.hasOwnProperty(key)) {
                    array.push(key);
                }
            }
            array.sort(func);
            for (key = 0; key < array.length; key++) {
                sorted[array[key]] = object[array[key]];
            }
            return sorted;
        },
        /**
         * Reset all properties to undefined. Handles observables too
         * @param obj: Object to clean
         * @param depth: Recursive depth. Defaults to 0, the passed object only
         * @param ignoredProps: Properties to ignore
         */
        cleanObject: function(obj, depth, ignoredProps) {
            // Reset all properties to undefined (props can also be observables)
            var currentDepth = 0;
            depth = 0 || depth;
            // Argument validation
            if (typeof ignoredProps !== undefined) {
                if (Object.prototype.toString.call( ignoredProps ) !== '[object Array]') {
                    throw new Error('Ignored props should be an Array')
                }
            } else {
                ignoredProps = []
            }
            var props = [];
            do {
                var fetchedProps = Object.getOwnPropertyNames(obj)
                    .sort()
                    .filter(function(prop, index, arr) {
                        return !prop.startsWith('__') &&                        // ignore requirejs props
                            !ignoredProps.contains(prop) &&                     // Not in ignored props
                            (typeof obj[prop] !== 'function' ||                 // Only the observables / non-function
                            (ko.isObservable(obj[prop]) && !ko.isComputed(obj[prop]))) &&
                            prop !== 'constructor' &&                           // Not the constructor
                            (index === 0 || prop !== arr[index - 1]) &&         // Not overriding in this prototype
                            !props.contains(prop)                               // Not overridden in a child
                    });
                props = props.concat(fetchedProps);
                currentDepth += 1;  // Might go deeper after here
            }
            while (
                depth >= currentDepth &&
                (obj = Object.getPrototypeOf(obj))  // Walk-up the prototype chain
            );
            $.each(props, function(index, prop) {
                if (ko.isObservable(obj[prop])) {
                    if (obj[prop].isObservableArray) {  // ObservableArray
                        obj[prop]([]);
                    } else if (obj[prop].isObservableDictionary) {
                        obj[prop].removeAll();
                    }
                } else {
                    obj[prop] = undefined;
                }
            })
        },
        isEmpty: function(value) {
            return ['', null, undefined].contains(value);
        }
    };
    var typeFunctions = {
        /**
         * Determines if the specified object is...an object. ie. Not an array, string, etc.
         * @method isObject
         * @param {object} object The object to check.
         * @return {boolean} True if matches the type, false otherwise.
         */
        isObject: function(object) {
            return object === Object(object);
        },
        /**
         * Check if the passed object is a function
         * @param functionToCheck: Object to check if it is a function
         * @returns {boolean}
         */
        isFunction: function(functionToCheck) {
            var getType = {};
            return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
        }
    };
    var sortingFunctions = {
        numberSort: function(itemA, itemB) {
            if ((itemA === undefined || itemA === null) && (itemB !== undefined && itemB !== null)) {
                return -1;
            }
            if ((itemA === undefined || itemA === null) && (itemB === undefined || itemB === null)) {
                return 0;
            }
            if ((itemA !== undefined && itemA !== null) && (itemB === undefined || itemB === null)) {
                return 1;
            }
            var regexAlpha = /[\d]+/g,
            regexNumber = /[^\d]+/g,
            partA = itemA.replace(regexAlpha, ''),
            partB = itemB.replace(regexAlpha, '');
            if (partA === partB) {
                partA = parseInt(itemA.replace(regexNumber, ''), 10);
                partB = parseInt(itemB.replace(regexNumber, ''), 10);
                return partA === partB ? 0 : (partA > partB ? 1 : -1);
            }
            return partA > partB ? 1 : -1;
        },
        ipSort: function(ipA, ipB) {
            var ipAParts = ipA.split('.');
            var ipBParts = ipB.split('.');
            for (var i = 0; i < 4; i += 1) {
                var result = GenericService.prototype.numberSort(ipAParts[i], ipBParts[i]);
                if (result !== 0) {
                    return result;
                }
            }
            return 0;
        }
    };
    var functions = {
        getTimestamp: function() {
            return new Date().getTime();
        },
        deg2rad: function(deg) {
            return deg * Math.PI / 180;
        },
        crossFiller: function(newKeyList, objectList, objectLoader, key, clean) {
            //               Arr.        Obs. Arr    Function      Obs.
            var i, j, currentKeyList = [], loadedObject;
            for (i = 0; i < objectList().length; i += 1) {
                currentKeyList.push(objectList()[i][key]());
            }
            for (i = 0; i < newKeyList.length; i += 1) {
                if ($.inArray(newKeyList[i], currentKeyList) === -1) {
                    // One of the new keys is not yet in our current key list. This means
                    // we'll have to load the object.
                    loadedObject = objectLoader(newKeyList[i]);
                    if (loadedObject !== undefined) {
                        objectList.push(loadedObject);
                    }
                }
            }
            if (clean !== false) {
                for (i = 0; i < currentKeyList.length; i += 1) {
                    if ($.inArray(currentKeyList[i], newKeyList) === -1) {
                        // One of the existing keys is not in the new key list anymore. This means
                        // we'll have to remove the object
                        for (j = 0; j < objectList().length; j += 1) {
                            if (objectList()[j][key]() === currentKeyList[i]) {
                                objectList.splice(j, 1);
                                break;
                            }
                        }
                    }
                }
            }
        },
        getHash: function(length) {
            if (length === undefined) {
                length = 16;
            }
            var text = '', possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', i;
            for(i = 0; i < length; i += 1) {
                text += possible.charAt(Math.floor(Math.random() * possible.length));
            }
            return text;
        }
    };
    GenericService.prototype = $.extend({}, properties, functions, patchFunctions, promiseFunctions, formatFunction, objectFunctions, typeFunctions, sortingFunctions);
    // Backwards compatibility. Certain methods were moved to different files. Should be changed asap
    GenericService.prototype = $.extend(GenericService.prototype, Object.getPrototypeOf(notificationsService), Object.getPrototypeOf(logService), Object.getPrototypeOf(xhrService));
    return new GenericService();
});

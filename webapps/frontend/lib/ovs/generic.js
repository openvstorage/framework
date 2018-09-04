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
define(['jquery', 'knockout', 'jqp/pnotify'], function($, ko) {
    "use strict";

    // Constants
    var ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;
    var hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    var nameRegex = /^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$/;
    var vdiskNameRegex = /^[0-9a-zA-Z][\-_a-zA-Z0-9]+[a-zA-Z0-9]$/;

    function getTimestamp() {
        return new Date().getTime();
    }
    function deg2rad(deg) {
        return deg * Math.PI / 180;
    }
    function setDecimals(value, decimals) {
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
    }
    function round(value, decimals) {
        decimals = decimals || 0;
        if (decimals === 0) {
            return Math.round(value);
        }
        var factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }
    function ceil(value, decimals) {
        decimals = decimals || 0;
        if (decimals === 0) {
            return Math.ceil(value);
        }
        var factor = Math.pow(10, decimals);
        return Math.ceil(value * factor) / factor;
    }
    function formatBytes(value) {
        var units, counter;
        units = ['b', 'kib', 'mib', 'gib', 'tib', 'pib'];
        counter = 0;
        while (value >= 1000) {
            value = value / 1024;
            counter += 1;
        }
        return setDecimals(round(value, 2), 2) + ' ' + $.t('ovs:generic.units.' + units[counter]);
    }
    function formatSpeed(value) {
        var units, counter;
        units = ['b', 'kib', 'mib', 'gib', 'tib', 'pib'];
        counter = 0;
        while (value >= 1000) {
            value = value / 1024;
            counter += 1;
        }
        return setDecimals(round(value, 2), 2) + ' ' + $.t('ovs:generic.units.' + units[counter] + 's');
    }
    function formatNumber(value) {
        if (value !== undefined) {
            value = round(value).toString();
            var regex = /(\d+)(\d{3})/;
            while (regex.test(value)) {
                value = value.replace(regex, '$1' + $.t('ovs:generic.thousandseparator') + '$2');
            }
            return value;
        }
        return undefined;
    }
    function formatPercentage(value, allowNan) {
        if (isNaN(value)) {
            if (!allowNan) {
                throw Error('Non-numeric value passed to format')
            }
            return "0 %";
        }
        value = Math.round(value * 10000) / 100;
        return formatNumber(value) + ' %';
    }
    function padRight(value, character, length) {
        while (value.length < length) {
            value += character;
        }
        return value;
    }
    function tryGet(object, key, fallback) {
        if (object !== undefined && object.hasOwnProperty(key)) {
            return object[key];
        }
        return fallback;
    }
    function trySet(observable, object, key, formatFunction) {
        if (object !== undefined && object.hasOwnProperty(key)) {
            if (formatFunction !== undefined && formatFunction.call) {
                observable(formatFunction(object[key]));
            } else {
                observable(object[key]);
            }
        }
    }
    function lower(value) {
        return value.toLowerCase();
    }
    function alert(title, message, type) {
        var data = {
            title: title,
            text: message,
            delay: 6000,
            hide: type !== 'error'
        };
        if (type !== undefined) {
            data.type = type;
        }
        return $.pnotify(data);
    }
    function alertInfo(title, message) {
        return alert(title, message, 'info');
    }
    function alertSuccess(title, message) {
        return alert(title, message, 'success');
    }
    function alertWarning(title, message) {
        return alert(title, message, 'notice');
    }
    function alertError(title, message) {
        return alert(title, message, 'error');
    }
    function keys(object, filter) {
        var allKeys = [], key;
        for (key in object) {
            if (object.hasOwnProperty(key)) {
                if (filter === undefined || filter(key)) {
                    allKeys.push(key);
                }
            }
        }
        return allKeys;
    }
    function xhrAbort(token) {
        if (token !== undefined && token.state && token.state() === 'pending') {
            try {
                token.abort();
            } catch (error) {
                // Ignore these errors
            }
        }
    }
    function xhrCompleted(token) {
        return !(token !== undefined && token.state && token.state() === 'pending');
    }
    function removeElement(array, element) {
        var index = array.indexOf(element);
        if (index !== -1) {
            array.splice(index, 1);
        }
    }
    function smooth(observable, initialValue, targetValue, steps, formatFunction) {
        var diff, stepSize, decimals, execute, current = initialValue;
        if (initialValue === undefined) {
            if (formatFunction && formatFunction.call) {
                observable(formatFunction(targetValue));
            } else {
                observable(targetValue);
            }
        } else {
            diff = targetValue - initialValue;
            if (diff !== 0) {
                decimals = Math.max((initialValue.toString().split('.')[1] || []).length, (targetValue.toString().split('.')[1] || []).length);
                stepSize = ceil(diff / steps, decimals);
                stepSize = stepSize === 0 ? 1 : stepSize;
                execute = function() {
                    if (Math.abs(targetValue - current) > Math.abs(stepSize)) {
                        current += stepSize;
                        if (formatFunction && formatFunction.call) {
                            observable(formatFunction(current));
                        } else {
                            observable(current);
                        }
                        window.setTimeout(execute, 75);
                    } else if (formatFunction && formatFunction.call) {
                        observable(formatFunction(targetValue));
                    } else {
                        observable(targetValue);
                    }
                };
                window.setTimeout(execute, 75);
            }
        }
    }
    function crossFiller(newKeyList, objectList, objectLoader, key, clean) {
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
    }
    function numberSort(itemA, itemB) {
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
    }
    function ipSort(ipA, ipB) {
        var i, result,
            ipAParts = ipA.split('.'),
            ipBParts = ipB.split('.');
        for (i = 0; i < 4; i += 1) {
            result = numberSort(ipAParts[i], ipBParts[i]);
            if (result !== 0) {
                return result;
            }
        }
        return 0;
    }
    function validate(nodes) {
        var i, node, check, checkAndRedirect;
        check = function(node) {
            return $.ajax(node + '/api/?timestamp=' + (new Date().getTime()), {
                type: 'GET',
                contentType: 'application/json',
                dataType: 'json',
                timeout: 5000,
                headers: { Accept: 'application/json' }
            });
        };
        checkAndRedirect = function(node) {
            check(node)
                .done(function() {
                    window.location.href = node;
                });
        };
        check('https://' + window.location.hostname)
            .fail(function() {
                for (i = 0; i < nodes.length; i += 1) {
                    node = nodes[i];
                    checkAndRedirect('https://' + node);
                }
                window.setTimeout(function() {
                    location.reload(true);
                }, 5000);
            });
    }
    function merge(originalObject, newObject, targetObject, keys) {
        // If the target equals the original, the target wasn't updated, so it can updated with the new.
        $.each(keys, function(i, key) {
            if (originalObject.hasOwnProperty(key) && targetObject.hasOwnProperty(key)) {
                if (originalObject[key] === targetObject[key]) {
                    if (newObject.hasOwnProperty(key)) {
                        targetObject[key] = newObject[key];
                    } else {
                        delete targetObject[key];
                    }
                }
            } else if (!originalObject.hasOwnProperty(key) && !targetObject.hasOwnProperty(key)) {
                if (newObject.hasOwnProperty(key)) {
                    targetObject[key] = newObject[key];
                }
            }
        });
    }
    function arrayEquals(array1, array2) {
        var i;
        if (!array2) {
            return false;
        }
        if (array1.length !== array2.length) {
            return false;
        }

        for (i = 0; i < array1.length; i += 1) {
            if (array1[i] instanceof Array && array2[i] instanceof Array) {
                if (!arrayEquals(array1[i], array2[i])) {
                    return false;
                }
            } else if (array1[i] !== array2[i]) {
                return false;
            }
        }
        return true;
    }
    function arrayHasElement(array, element) {
        var i;
        for (i = 0; i < array.length; i += 1) {
            if (element === array[i]) {
                return true;
            }
        }
        return false;
    }
    function arrayHasElementWithProperty(array, property, value) {
        var i;
        for (i = 0; i < array.length; i += 1) {
            var element = array[i];
            if (element.hasOwnProperty(property) && element[property] === value) {
                return true;
            }
        }
        return false;
    }
    function arrayFilterUnique(array) {
        return array.filter(function(item, pos, self) {
            return self.indexOf(item) == pos;
        });
    }
    function getHash(length) {
        if (length === undefined) {
            length = 16;
        }
        var text = '', possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', i;
        for(i = 0; i < length; i += 1) {
            text += possible.charAt(Math.floor(Math.random() * possible.length));
        }
        return text;
    }
    function setCookie(name, value, days) {
        var expires, date;
        if (days !== undefined) {
            date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            expires = '; expires=' + date.toGMTString();
        } else {
            expires = '';
        }
        document.cookie = encodeURIComponent(name) + '=' + encodeURIComponent(value) + expires + '; path=/';
    }
    function getCookie(name) {
        var cookies = document.cookie.split(';'), cookie, i;
        name = encodeURIComponent(name);
        for (i = 0; i < cookies.length; i += 1) {
            cookie = cookies[i];
            while (cookie.charAt(0) === ' ') {
                cookie = cookie.substring(1, cookie.length);
            }
            if (cookie.indexOf(name) === 0) {
                return decodeURIComponent(cookie.substring(name.length + 1, cookie.length));
            }
        }
        return null;
    }
    function removeCookie(name) {
        setCookie(name, '', -1);
    }
    function isEmpty(value) {
        return ['', null, undefined].contains(value) || value === null;
    }
    function extract(object) {
        if (!!(object && object.constructor && object.call && object.apply)) {
            return object();
        }
        return object
    }
    function log(message, severity) {
        if (window.console) {
            if (severity === 'info' || severity === null || severity === undefined) {
                console.log(message);
            } else if (severity === 'warning') {
                console.warn(message);
            } else if (severity === 'error') {
                console.error(message);
            }
        }
    }
    function cleanDeviceName(name) {
        var cleaned = name.replace(/^(\/)+|(\/)+$/g, '').replace(/ /g,"_").replace(/[^a-zA-Z0-9-_\.\/]+/g, "");
        while (cleaned.indexOf('//') > -1) {
            cleaned = cleaned.replace(/\/\//g, '/');
        }
        if (cleaned.length > 4 && cleaned.slice(-4) === '.raw') {
            return cleaned;
        }
        return cleaned + '.raw';
    }
    function extractErrorMessage(error, namespace) {
        if (error.hasOwnProperty('responseText')) {
            try {
                var key, message, obj = $.parseJSON(error.responseText);
                if (obj.hasOwnProperty('error')) {
                    key = (namespace === undefined ? 'ovs' : namespace) + ':generic.api_errors.' + obj.error;
                    message = $.t(key);
                    if (message === key) {
                        if (obj.hasOwnProperty('error_description')) {
                            return obj.error_description;
                        }
                        return obj.error;
                    }
                    return message;
                }
                return error.responseText;
            } catch(exception) {
                if (error.hasOwnProperty('status') && error.status === 404) {
                    return $.t((namespace === undefined ? 'ovs' : namespace) + ':generic.api_errors.not_found');
                }
                return error;
            }
        }
        return error;
    }
    function objectEquals(object1, object2) {
        if (object1 === object2) {
            // If both object1 and object2 are null or undefined and exactly the same
            return true;
        }
        if (!(object1 instanceof Object) || !(object2 instanceof Object)) {
            // If they are not strictly equal, they both need to be Objects
            return false;
        }
        if (object1.constructor !== object2.constructor) {
            // They must have the exact same prototype chain, the closest we can do is
            // test thie constructor.
            return false;
        }
        for (var p in object1) {
            if (!object1.hasOwnProperty(p)) {
                // Other properties were tested using object1.constructor === object2.constructor
                continue;
            }
            if (!object2.hasOwnProperty(p)) {
                // Allows to compare object1[p] and object2[p] when set to undefined
                return false;
            }
            if (object1[p] === object2[p]) {
                // If they have the same strict value or identity then they are equal
                continue;
            }
            if (typeof(object1[p]) !== "object") {
                // Numbers, Strings, Functions, Booleans must be strictly equal
                return false;
            }
            if (!objectEquals(object1[p], object2[p])) {
                // Objects and Arrays must be tested recursively
                return false;
            }
        }
        for (p in object2) {
            if (object2.hasOwnProperty(p) && !object1.hasOwnProperty(p)) {
                // Allows object1[p] to be set to undefined
                return false;
            }
        }
        return true;
    }

    function sortObject(object, func) {
        /**
         * Sorts on objects keys.
         * By convention, most browsers will retain the order of keys in an object in the order that they were added.
         * But don't expect it to always work
         * @param object: object to sort
         * @param func: sorting function
         * @returns {{}}
         */
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
    }
    function cleanObject(obj, depth, ignoredProps) {
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
    }
    // Object should already be observable
    function makeChildrenObservables(observable) {
        if(!ko.isObservable(observable)) return;
        // Loop through its children
        $.each(observable(), function(key, child) {
            if (!ko.isObservable(child)) {
                child = ko.observable(child);
                observable()[key] = child;  // By reference does not work as the data was unwrapped from the observable
                if (typeof child() === "object") {
                    makeChildrenObservables(child);
                }
            }
        });
    }
    function recursiveSubscribe(observable, func) {
        /**
         * Registers a subscribe to all children observables of an object
         * @param observable: Observable to subscribe to
         * @param func: Function to fire on change
         * @return {[]}
         */
        var array = [];
        if(!ko.isObservable(observable)) return array;
        $.each(observable(), function(key, child) {
            if (ko.isObservable(child)) {
                array.push(child.subscribe(func))
            }
        });
    }

    function isObject(o) {
        return o instanceof Object && o.constructor === Object;
    }

    function isFunction(functionToCheck) {
        var getType = {};
        return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
    }
    /**
     * Chain promises more neatly instead of writing .then yourselves
     * Used the .then(function()) {return new Promise)
     * All of the data of the previous callback can be used in the next one (eg. chainPromises([api.get('test'), function(testAPIData) { console.log(testAPIData}]
     * Calling .done on the return value will ensure that all previous chained promises have been completed
     * @param callbackList: list of callbacks to use
     */
    function chainPromises(callbackList) {
        return callbackList.reduce(function(chain, func){
            chain ? chain.then(func) : func();
        }, null)
    }
    function _arrayGetItem(array, prop, index){
        var foundItem = undefined;
        if (index > array.length - 1 || index < 0){
            return foundItem;
        }
        foundItem = array[index];
        if (ko.isObservable(foundItem)) {
            foundItem = foundItem();
        }
        if (typeof prop !== 'undefined') {
            foundItem = foundItem[prop];
            if (ko.isObservable(foundItem)) {
                foundItem = foundItem();
            }
        }
        return foundItem;
    }
    function _arrayBinarySearch(array, value, prop){
        /**
         * Only works on a sorted list
         * Returns the index of the item search for or -1 if not found
         * Faster than indexing or contains
         * Worst case time: O(log(n)
         * @returns: Number
         */
        if (array.length === 0) {
            return -1;
        }
        var middleIndex = Math.floor(array.length / 2);
        var middleItem = _arrayGetItem(array, prop, middleIndex);
        if (array.length === 1 && middleItem !== value) {
            return -1;  // Item not present
        }
        if (value === middleItem) {
            return middleIndex;
        }
        if (value > middleItem) {
            var additionalIndex = array.slice(middleIndex + 1, array.length).brSearch(value, prop);
            if (additionalIndex === -1) {
                return -1;
            }
            return middleIndex + 1 + additionalIndex;
        }
        return array.slice(0, middleIndex).brSearch(value, prop);
    }
    function _arrayBinarySearchFirst(array, value, prop, startIndex, stopIndex){
        /**
         * Only works on a sorted list
         * Returns the index of the 1st element found (in case multiple identical would be present) or -1 if none found
         * Faster than indexing or contains
         * Worst case time: O(log(n)
         * @returns: Number
         */
        startIndex = startIndex === undefined ? 0 : startIndex;
        stopIndex = stopIndex === undefined ? array.length - 1 : stopIndex;
        if (stopIndex < startIndex) {
            return -1;
        }
        var middleIndex = Math.floor(startIndex + (stopIndex - startIndex) / 2);
        var middleItem = _arrayGetItem(array, prop, middleIndex);
        var previousItem = _arrayGetItem(array, prop, middleIndex - 1);
        if ((previousItem === undefined || value > previousItem) && middleItem === value) {
            return middleIndex;
        }
        if (value > middleItem) {
            // Don't use slice here since we potentially lose duplicate values
            return array.brSearchFirst(value, prop, (middleIndex + 1), stopIndex);
        }
        return array.brSearchFirst(value, prop, startIndex, (middleIndex - 1));
    }
    function _arrayBinarySearchLast(array, value, prop, startIndex, stopIndex) {
        /**
         * Only works on a sorted list
         * Returns the index of the last element found (in case multiple identical would be present) or -1 if none found
         * Faster than indexing or contains
         * Worst case time: O(log(n)
         * @returns: Number
         */
        startIndex = startIndex === undefined ? 0 : startIndex;
        stopIndex = stopIndex === undefined ? array.length - 1 : stopIndex;
        if (stopIndex < startIndex) {
            return -1;
        }
        var middleIndex = Math.floor(startIndex + (stopIndex - startIndex) / 2);
        var middleItem = _arrayGetItem(array, prop, middleIndex);
        var nextItem = _arrayGetItem(array, prop, middleIndex + 1);
        if ((nextItem === undefined || value < nextItem) && middleItem === value) {
            return middleIndex;
        }
        if (value < middleItem) {
            // Don't use slice here since we potentially lose duplicate values
            return array.brSearchLast(value, prop, startIndex, (middleIndex - 1));
        }
        return array.brSearchLast(value, prop, (middleIndex + 1), stopIndex);
    }

    Array.prototype.brSearch = function(value, prop) {
        return _arrayBinarySearch(this, value, prop);
    };
    Array.prototype.brSearchFirst = function(value, prop, start, end) {
        return _arrayBinarySearchFirst(this, value, prop, start, end);
    };
    Array.prototype.brSearchLast = function(value, prop, start, end) {
        return _arrayBinarySearchLast(this, value, prop, start, end);
    };
    Array.prototype.equals = function(array) {
        return arrayEquals(this, array);
    };
    Array.prototype.contains = function(element) {
        return arrayHasElement(this, element);
    };
    Array.prototype.remove = function(element) {
        return removeElement(this, element);
    };

    String.prototype.format = function (args) {
			var str = this;
			return str.replace(String.prototype.format.regex, function(item) {
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
    String.prototype.format.regex = new RegExp("{-?[0-9]+}", "g");

    JSON.stringifyOnce = function (obj, replacer, space) {
        // Function to remove all circular references (could prove useful while debugging).
        // Usage: JSON.stringifyOnce(ko.toJS(item));
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

    return {
        // Vars
        ipRegex: ipRegex,
        hostRegex: hostRegex,
        nameRegex: nameRegex,
        vdiskNameRegex: vdiskNameRegex,
        // Functions
        alert: alert,
        alertError: alertError,
        alertInfo: alertInfo,
        alertSuccess: alertSuccess,
        alertWarning: alertWarning,
        arrayFilterUnique: arrayFilterUnique,
        arrayHasElementWithProperty: arrayHasElementWithProperty,
        ceil: ceil,
        cleanDeviceName: cleanDeviceName,
        cleanObject: cleanObject,
        crossFiller: crossFiller,
        deg2rad: deg2rad,
        extract: extract,
        extractErrorMessage: extractErrorMessage,
        formatBytes: formatBytes,
        formatNumber: formatNumber,
        formatPercentage: formatPercentage,
        formatSpeed: formatSpeed,
        getCookie: getCookie,
        getHash: getHash,
        getTimestamp: getTimestamp,
        ipSort: ipSort,
        isEmpty: isEmpty,
        isFunction: isFunction,
        isObject: isObject,
        keys: keys,
        log: log,
        lower: lower,
        makeChildrenObservables: makeChildrenObservables,
        merge: merge,
        objectEquals: objectEquals,
        padRight: padRight,
        recursiveSubscribe: recursiveSubscribe,
        removeCookie: removeCookie,
        removeElement: removeElement,
        round: round,
        setCookie: setCookie,
        setDecimals: setDecimals,
        smooth: smooth,
        sortObject: sortObject,
        tryGet: tryGet,
        trySet: trySet,
        validate: validate,
        xhrAbort: xhrAbort,
        xhrCompleted: xhrCompleted
    };
});

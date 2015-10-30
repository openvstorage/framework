// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define, window, document, location */
define(['jquery', 'jqp/pnotify'], function($) {
    "use strict";
    function getTimestamp() {
        return new Date().getTime();
    }
    function buildString(value, times) {
        var i, returnvalue = '';
        for (i = 0; i < times; i += 1) {
            returnvalue += value.toString();
        }
        return returnvalue;
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
            parts.push(buildString('0', decimals));
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
        units = ['b', 'kib', 'mib', 'gib', 'tib'];
        counter = 0;
        while (value >= 1000) {
            value = value / 1024;
            counter += 1;
        }
        return setDecimals(round(value, 2), 2) + ' ' + $.t('ovs:generic.' + units[counter]);
    }
    function formatSpeed(value) {
        var units, counter;
        units = ['b', 'kib', 'mib', 'gib', 'tib'];
        counter = 0;
        while (value >= 1000) {
            value = value / 1024;
            counter += 1;
        }
        return setDecimals(round(value, 2), 2) + ' ' + $.t('ovs:generic.' + units[counter] + 's');
    }
    function formatRatio(value) {
        return setDecimals(round(value, 2), 2) + ' %';
    }
    function formatShort(value) {
        var units, counter, returnValue;
        units = ['k', 'm', 'g', 't'];
        counter = 0;
        while (value >= 1000) {
            value = value / 1000;
            counter += 1;
        }
        returnValue = setDecimals(round(value, 2), 2);
        if (counter > 0) {
            returnValue += ' ' + $.t('ovs:generic.' + units[counter - 1]);
        }
        return returnValue;
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
    function formatPercentage(value) {
        value = Math.round(value * 10000) / 100;
        return formatNumber(value) + ' %';
    }
    function padRight(value, character, length) {
        while (value.length < length) {
            value += character;
        }
        return value;
    }
    function padLeft(value, character, length) {
        while (value.length < length) {
            value = character + value;
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
            nonblock: true,
            delay: 3000
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
        if (token !== undefined && token.state() === 'pending') {
            try {
                token.abort();
            } catch (error) {
                // Ignore these errors
            }
        }
    }
    function xhrCompleted(token) {
        return !(token !== undefined && token.state() === 'pending');
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
    function syncObservableArray(newArray, objectList, key, clean) {
        var i, j, newKeyList = [], currentKeyList = [];
        for (i = 0; i < objectList().length; i += 1) {
            currentKeyList.push(objectList()[i].key);
        }
        for (i = 0; i < newArray.length; i += 1) {
            newKeyList.push(newArray[i].key);
        }
        for (i = 0; i < newKeyList.length; i += 1) {
            if ($.inArray(newKeyList[i], currentKeyList) === -1) {
                // One of the new keys is not yet in our current key list. This means
                // we'll have to load the object.
                objectList.push(newArray[i]);
            }
        }
        if (clean !== false) {
            for (i = 0; i < currentKeyList.length; i += 1) {
                if ($.inArray(currentKeyList[i], newKeyList) === -1) {
                    // One of the existing keys is not in the new key list anymore. This means
                    // we'll have to remove the object
                    for (j = 0; j < objectList().length; j += 1) {
                        if (objectList()[j].key === currentKeyList[i]) {
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
    function advancedSort(list, properties) {
        list.sort(function(a, b) {
            var i, result;
            for (i = 0; i < properties.length; i += 1) {
                result = numberSort(a[properties[i]](), b[properties[i]]());
                if (result !== 0) {
                    return result;
                }
            }
            return 0;
        });
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
    function overlap(array1, array2) {
        var i, j;
        for (i = 0; i < array1.length; i += 1) {
            for (j = 0; j < array2.length; j += 1) {
                if (array1[i] === array2[j]) {
                    return true;
                }
            }
        }
        return false;
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
    function getColor(index) {
        var colors = ['#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#ffff33', '#a65628', '#f781bf', '#999999'];
        return colors[index % colors.length];
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
    function arrayIsIn(array1, array2) {
        var i;
        for (i = 0; i < array2.length; i += 1) {
            if (arrayEquals(array1, array2[i])) {
                return true;
            }
        }
        return false;
    }
    function stringStartsWith(string1, string2) {
        return string1.indexOf(string2) === 0;
    }
    function getLocalTime(timestamp) {
        var date = new Date(timestamp * 1000);
        return date.toLocaleTimeString();
    }
    function arrayFilterUnique(value, index, array) {
        return array.indexOf(value) === index;
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

    Array.prototype.equals = function(array) {
        return arrayEquals(this, array);
    };
    Array.prototype.nestedIn = function(array) {
        return arrayIsIn(this, array);
    };
    Array.prototype.contains = function(element) {
        return arrayHasElement(this, element);
    };
    String.prototype.startsWith = function(searchString) {
        return stringStartsWith(this, searchString);
    };
    return {
        advancedSort: advancedSort,
        alert: alert,
        alertError: alertError,
        alertInfo: alertInfo,
        alertSuccess: alertSuccess,
        arrayFilterUnique: arrayFilterUnique,
        buildString: buildString,
        ceil: ceil,
        crossFiller: crossFiller,
        deg2rad: deg2rad,
        formatBytes: formatBytes,
        formatNumber: formatNumber,
        formatPercentage: formatPercentage,
        formatRatio: formatRatio,
        formatShort: formatShort,
        formatSpeed: formatSpeed,
        getColor: getColor,
        getCookie: getCookie,
        getHash: getHash,
        getLocalTime: getLocalTime,
        getTimestamp: getTimestamp,
        keys: keys,
        lower: lower,
        merge: merge,
        numberSort: numberSort,
        overlap: overlap,
        padLeft: padLeft,
        padRight: padRight,
        removeCookie: removeCookie,
        removeElement: removeElement,
        round: round,
        setCookie: setCookie,
        setDecimals: setDecimals,
        smooth: smooth,
        syncObservableArray: syncObservableArray,
        tryGet: tryGet,
        trySet: trySet,
        validate: validate,
        xhrAbort: xhrAbort,
        xhrCompleted: xhrCompleted,
        isEmpty: isEmpty
    };
});

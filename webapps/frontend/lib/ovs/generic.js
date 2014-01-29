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
    function getCookie(name) {
        var i, cookie, cookies;
        cookies = document.cookie.split(';');
        for (i = 0; i < cookies.length; i += 1) {
            cookie = $.trim(cookies[i]);
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                return decodeURIComponent(cookie.substring(name.length + 1));
            }
        }
        return undefined;
    }
    function setCookie(name, value, expiry) {
        var date, expires = '';
        if (expiry) {
            date = new Date();
            date.setTime(date.getTime() +
                (tryGet(expiry, 'days', 0) * 24 * 60 * 60 * 1000) +
                (tryGet(expiry, 'hours', 0) * 60 * 60 * 1000) +
                (tryGet(expiry, 'minutes', 0) * 60 * 1000) +
                (tryGet(expiry, 'seconds', 0) * 1000)
            );
            expires = '; expires=' + date.toUTCString();
        }
        document.cookie = name + '=' + value + expires + '; path=/';
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
    function keys(object) {
        var allKeys = [], key;
        for (key in object) {
            if (object.hasOwnProperty(key)) {
                allKeys.push(key);
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
    function crossFiller(newKeyList, objectList, objectLoader, key) {
        //               Arr.        Obs. Arr    Function      Obs.
        var i, j, currentKeyList = [];
        for (i = 0; i < objectList().length; i += 1) {
            currentKeyList.push(objectList()[i][key]());
        }
        for (i = 0; i < newKeyList.length; i += 1) {
            if ($.inArray(newKeyList[i], currentKeyList) === -1) {
                // One of the new keys is not yet in our current key list. This means
                // we'll have to load the object.
                objectList.push(objectLoader(newKeyList[i]));
            }
        }
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
        if (partA == partB) {
            partA = parseInt(itemA.replace(regexNumber, ''), 10);
            partB = parseInt(itemB.replace(regexNumber, ''), 10);
            return partA == partB ? 0 : (partA > partB ? 1 : -1);
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
            return $.ajax(node + '/api/internal/generic/0/?format=jsonp&timestamp=' + (new Date().getTime()), {
                type: 'GET',
                contentType: 'application/jsonp',
                dataType: 'jsonp',
                timeout: 60000
            });
        };
        checkAndRedirect = function(node) {
            check(node)
                .done(function() {
                    window.location.href = node;
                })
                .fail(function() {
                    // something
                });
        };
        check('https://' + window.location.hostname)
            .fail(function() {
                for (i = 0; i < nodes.length; i += 1) {
                    node = nodes[i];
                    checkAndRedirect('https://' + node);
                }
            });
    }

    return {
        getTimestamp    : getTimestamp,
        formatBytes     : formatBytes,
        formatSpeed     : formatSpeed,
        formatRatio     : formatRatio,
        formatShort     : formatShort,
        formatNumber    : formatNumber,
        padRight        : padRight,
        getCookie       : getCookie,
        setCookie       : setCookie,
        tryGet          : tryGet,
        alert           : alert,
        alertInfo       : alertInfo,
        alertSuccess    : alertSuccess,
        alertError      : alertError,
        keys            : keys,
        xhrAbort        : xhrAbort,
        xhrCompleted    : xhrCompleted,
        removeElement   : removeElement,
        smooth          : smooth,
        round           : round,
        ceil            : ceil,
        buildString     : buildString,
        setDecimals     : setDecimals,
        crossFiller     : crossFiller,
        deg2rad         : deg2rad,
        numberSort      : numberSort,
        advancedSort    : advancedSort,
        validate        : validate
    };
});

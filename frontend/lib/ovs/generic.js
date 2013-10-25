define(function() {
    "use strict";
    function getTimestamp() {
        return new Date().getTime();
    }
    function getBytesHuman(value) {
        var units, counter;
        units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
        counter = 0;
        while (value > 2048) {
            value = value / 1024;
            counter += 1;
        }
        return (Math.round(value * 100) / 100).toString() + ' ' + units[counter];
    }
    function padRight(value, character, length) {
        while (value.length < length) {
            value += character;
        }
        return value;
    }
    function getCookie(name) {
        var i, cookie, cookies;
        if (document.cookie && document.cookie !== '') {
            cookies = document.cookie.split(';');
            for (i = 0; i < cookies.length; i++) {
                cookie = $.trim(cookies[i]);
                if (cookie.substring(0, name.length + 1) === (name + '=')) {
                    return decodeURIComponent(cookie.substring(name.length + 1));
                }
            }
        }
        return undefined;
    }
    function tryGet(object, key, fallback) {
        if (object !== undefined && object.hasOwnProperty(key)) {
            return object[key];
        }
        return fallback;
    }
    function alert(title, message, type) {
        $.pnotify({
            title: title,
            text: message,
            nonblock: true,
            delay: 3000,
            type: (type !== undefined ? type : 'notice')
        });
    }
    function alertInfo(title, message) {
        alert(title, message, 'info');
    }
    function alertSuccess(title, message) {
        alert(title, message, 'success');
    }
    function alertError(title, message) {
        alert(title, message, 'error');
    }
    function keys(object) {
        var all_keys = [], key;
        for (key in object) {
            if (object.hasOwnProperty(key)) {
                all_keys.push(key);
            }
        }
        return all_keys;
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
    function removeElement(array, element) {
        var index = array.indexOf(element);
        if (index !== -1) {
            array.splice(index, 1);
        }
    }
    function smooth(observable, target_value, steps) {
        var start_value, diff, step_size, decimals, execute;
        if (steps === undefined) {
            steps = 3;
        }
        start_value = observable() || 0;
        diff = target_value - start_value;
        if (diff !== 0) {
            decimals = Math.max((start_value.toString().split('.')[1] || []).length, (target_value.toString().split('.')[1] || []).length);
            step_size = decimals === 0 ? Math.round(diff / steps) : Math.round(diff / steps * (10 * decimals)) / (10 * decimals);
            execute = function() {
                var current = observable();
                if (current !== target_value) {
                    if (Math.abs(target_value - current) > Math.abs(step_size)) {
                        observable(observable() + step_size);
                        window.setTimeout(execute, 75);
                    } else {
                        observable(target_value);
                    }
                }
            };
            window.setTimeout(execute, 75);
        }
    }

    return {
        getTimestamp : getTimestamp,
        getBytesHuman: getBytesHuman,
        padRight     : padRight,
        getCookie    : getCookie,
        tryGet       : tryGet,
        alert        : alert,
        alertInfo    : alertInfo,
        alertSuccess : alertSuccess,
        alertError   : alertError,
        keys         : keys,
        xhrAbort     : xhrAbort,
        removeElement: removeElement,
        smooth       : smooth
    };
});
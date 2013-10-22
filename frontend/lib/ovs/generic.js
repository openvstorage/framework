define(function() {
    "use strict";
    function gettimestamp() {
        return new Date().getTime();
    }
    function get_bytes_human(value) {
        var units, counter;
        units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
        counter = 0;
        while (value > 2048) {
            value = value / 1024;
            counter += 1;
        }
        return (Math.round(value * 100) / 100).toString() + ' ' + units[counter];
    }
    function padright(value, character, length) {
        while (value.length < length) {
            value += character;
        }
        return value;
    }
    function get_cookie(name) {
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
        return '';
    }
    function tryget(object, key, fallback) {
        if (object.hasOwnProperty(key)) {
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
    function alert_info(title, message) {
        alert(title, message, 'info');
    }
    function alert_success(title, message) {
        alert(title, message, 'success');
    }
    function alert_error(title, message) {
        alert(title, message, 'error');
    }

    return {
        gettimestamp   : gettimestamp,
        get_bytes_human: get_bytes_human,
        padright       : padright,
        get_cookie     : get_cookie,
        tryget         : tryget,
        alert          : alert,
        alert_info     : alert_info,
        alert_success  : alert_success,
        alert_error    : alert_error
    };
});
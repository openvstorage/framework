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

    return {
        gettimestamp   : gettimestamp,
        get_bytes_human: get_bytes_human,
        padright       : padright,
        get_cookie     : get_cookie
    };
});
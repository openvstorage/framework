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

    return {
        gettimestamp   : gettimestamp,
        get_bytes_human: get_bytes_human,
        padright       : padright
    };
});
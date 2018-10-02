// Copyright (C) 2018 iNuron NV
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
define(['jquery'],
    function($) {
    "use strict";

    /**
     * Handles cookies
     * @constructor
     */
    function CookieService(){ }

    var functions = {
        setCookie: function(name, value, days) {
            var expires, date;
            if (days !== undefined) {
                date = new Date();
                date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
                expires = '; expires=' + date.toGMTString();
            } else {
                expires = '';
            }
            document.cookie = encodeURIComponent(name) + '=' + encodeURIComponent(value) + expires + '; path=/';
        },
        getCookie: function(name) {
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
        },
        removeCookie: function(name) {
            this.setCookie(name, '', -1);
        }
    };

    CookieService.prototype = $.extend({}, functions);
    return new CookieService()
});

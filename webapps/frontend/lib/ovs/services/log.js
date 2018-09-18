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
     * Handles logging
     * @constructor
     */
    function LogService(){ }

    var functions = {
        log: function(message, severity) {
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
    };

    LogService.prototype = $.extend({}, functions);
    return new LogService()
});

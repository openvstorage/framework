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
/*global define */
define(['jquery', 'jqp/pnotify'],
    function($) {
    "use strict";
    /**
     * Displays notifications as pop-over
     * @constructor
     */
    var BoxTypes = Object.freeze({
        INFO: 'info',
        SUCCESS: 'success',
        WARNING: 'notice',
        ERROR: 'error'
    });

    function NotificationService(){ }
    var functions = {
        /**
         * Creates an alert box
         * @param title: Title of the box
         * @param message: Message of the box
         * @param type: Type of box
         * @returns {*}
         */
        alert: function(title, message, type) {
            var data = {
                title: title,
                text: message,
                delay: 6000,
                hide: type !== BoxTypes.ERROR
            };
            if (type !== undefined) {
                data.type = type;
            }
            return $.pnotify(data);
        },
        alertInfo: function(title, message) {
            return this.alert(title, message, BoxTypes.INFO);
        },
        alertSuccess: function(title, message) {
            return this.alert(title, message, BoxTypes.SUCCESS);
        },
        alertWarning: function(title, message) {
            return this.alert(title, message, BoxTypes.WARNING);
        },
        alertError: function(title, message) {
            return this.alert(title, message, BoxTypes.ERROR);
        },
        /**
         * Notify the user about a certain event
         * Alerts a small information window
         * @param data
         */
        handleEvent: function(data) {
            // if (data.type === 'foobar') {
            //     this.alertInfo(
            //         $.t('ovs:events.' + data.type),
            //         $.t('ovs:events.' + data.type + '_content', data.metadata)
            //     )
            // }
        }
    };

    NotificationService.prototype = $.extend({}, functions);
    return new NotificationService()
});

// Copyright (C) 2017 iNuron NV
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
define([
    'jquery'
], function ($) {
    /**
     * This context will just enable the default bootstrap modal behaviour
     * This context is registered in the main.js
     */
    return {
        addHost: function (theDialog) {
            $('<div class=\'modal fade\' id=\'my-modal\'></div>').appendTo($('body'));
            theDialog.host = $('#my-modal').get(0);
        },
        removeHost: function () {
            window.setTimeout(function () {
                $('#my-modal').modal('hide');
                $('body').removeClass('modal-open');
                $('.modal-backdrop').remove();
            }, 50);
        },
        compositionComplete: function () {
            var $modal = $('#my-modal');
            $modal.modal({ backdrop: 'static', show: false, keyboard: false });
            $modal.modal('show');
            $modal.find('.autofocus').first().focus();
        },
        attached: null
    }
});
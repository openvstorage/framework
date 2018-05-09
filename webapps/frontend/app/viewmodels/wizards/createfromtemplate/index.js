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
define([
    'jquery', 'ovs/generic',
    '../build', './gather', './data'
], function($, generic, Build, Gather, data) {
    "use strict";
    return function(options) {
        var self = this;
        // Inherit
        Build.call(self);

        // Variables
        self.data = data;

        // Setup
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.create_ft.title')));
        self.modal(generic.tryGet(options, 'modal', false));
        self.data.guid(options.guid);
        self.steps([new Gather()]);
        self.activateStep();

        // Cleaning data
        self.data.amount(0);
        self.data.description('');
        self.data.name(undefined);
        self.data.selectedStorageRouters([]);
        self.data.storageRouters([]);
        self.data.startnr(1);

        // Functions
        self.compositionComplete = function() {
            var i, fields = ['amount', 'startnr'], element;
            for (i = 0; i < fields.length; i += 1) {
                element = $("#" + fields[i]);
                element.on('keypress', function(e) {
                    // Guard keypresses, only accepting numeric values
                    return !(e.which !== 8 && e.which !== 0 && (e.which < 48 || e.which > 57));
                });
                element.on('change', function() {
                    // Guard ctrl+v
                    element.val(Math.max(1, parseInt('0' + element.val(), 10)));
                });
            }
        };
    };
});

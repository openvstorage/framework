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
    '../build', './gather', './confirm', './data'
], function($, generic, Build, Gather, Confirm, data) {
    "use strict";
    return function(options) {
        var self = this;
        Build.call(self);

        // Variables
        self.data = data;

        // Setup
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.clone.title')));
        self.modal(generic.tryGet(options, 'modal', false));
        self.data.vDisk(options.vdisk);
        self.steps([new Gather(), new Confirm()]);
        self.activateStep();

        if (self.data.storageRouters().length > 0) {
            self.data.storageRouter(self.data.storageRouters()[0]);
        } else {
            self.data.storageRouter(undefined);
        }
        self.data.snapshot(undefined);
        if (self.data.vDisk() !== undefined) {
            self.data.name(self.data.vDisk().name().toLowerCase().replace(/ /, '-') + '-clone');
        }
    };
});

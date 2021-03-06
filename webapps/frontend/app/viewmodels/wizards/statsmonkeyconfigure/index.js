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
/*global define */
define([
    'jquery', 'ovs/generic',
    '../build', './gather', './data'
], function($, generic, Build, Gather, Data) {
    "use strict";
    return function(options) {
        var self = this;
        Build.call(self);

        // Variables
        self.data = data;

        // Cleaning data
        // Variables
        var data = new Data(options.newConfig, options.origConfig);

        var stepOptions = {
            data: data
        };

        // Setup
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.stats_monkey_configure.title')));
        self.modal(generic.tryGet(options, 'modal', false));
        self.steps([new Gather(stepOptions)]);
        self.activateStep();
    };
});

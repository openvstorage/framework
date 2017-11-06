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
    'jquery', 'knockout', 'ovs/generic',
    '../build', './data', './gather_config', './gather_vpool', './gather_fragment_cache', './gather_block_cache', './confirm'
], function($, ko, generic, Build, Data, GatherConfig, GatherVPool, GatherFragmentCache, GatherBlockCache, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        // Create a new data instance to be passed around
        var data = new Data(options.storageRouter, options.vPool);
        var stepOptions = {data: data};

        // Inherit from build
        Build.call(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));
        var defaultTitle = $.t('ovs:wizards.add_vpool.title_add');
        if (options.vPool !== undefined) {
            defaultTitle = $.t('ovs:wizards.add_vpool.title_extend');
        }
        self.title(generic.tryGet(options, 'title', defaultTitle));

        self.steps([
            new GatherVPool(stepOptions),
            new GatherFragmentCache(stepOptions),
            new GatherBlockCache(stepOptions),
            new GatherConfig(stepOptions),
            new Confirm(stepOptions)]);

        self.step(0);
        self.activateStep();
    };
});

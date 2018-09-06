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
        // Inherit
        Build.call(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));
        var defaultTitle = $.t('ovs:wizards.reconfigure_vpool.title', {'sr_name': options.storageRouter.name()});
        self.title(generic.tryGet(options, 'title', defaultTitle));

        var data = new Data(options.vPool,
                            options.storageRouter,
                            options.storageDriver);

        var stepOptions = {
            data: data
        };

        self.steps([new GatherVPool(stepOptions),
                    new GatherFragmentCache(stepOptions),
                    new GatherBlockCache(stepOptions),
                    new GatherConfig(stepOptions),
                    new Confirm(stepOptions)]);

        self.step(0);
        self.activateStep();
    };
});

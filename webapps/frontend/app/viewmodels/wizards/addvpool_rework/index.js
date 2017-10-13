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
], function($, ko, generic, build, data, GatherConfig, GatherVPool, GatherFragmentCache, GatherBlockCache, Confirm) {
    "use strict";
    return function(options) {
        var self = this;

        // Inject all data first before building all steps
        generic.cleanObject(data, 0, ['albaPresetMap', 'storageDriverParams', 'storageRouterMap', 'backendData', 'cachingData']);
        data.albaPresetMap({});  // set this to an empty object as cleanObject will set it to undefined
        // Set current data
        data.vPool(options.vPool);
        data.storageRouter(options.storageRouter);
        data.completed = options.completed;
        // Fill in all wizard data which is derived from the options above
        data.fillData();

        build(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));
        var defaultTitle = $.t('ovs:wizards.add_vpool.title_add');
        if (options.vPool !== undefined) {
            defaultTitle = $.t('ovs:wizards.add_vpool.title_extend');
        }
        self.title(generic.tryGet(options, 'title', defaultTitle));

        self.steps([new GatherVPool(), new GatherFragmentCache(), new GatherBlockCache(), new GatherConfig(), new Confirm()]);

        self.step(0);
        self.activateStep();
    };
});

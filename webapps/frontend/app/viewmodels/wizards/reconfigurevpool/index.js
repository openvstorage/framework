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
        build(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));
        var defaultTitle = $.t('ovs:wizards.reconfigure_vpool.title', {'sr_name': options.storageRouter.name()});
        self.title(generic.tryGet(options, 'title', defaultTitle));

        self.steps([new GatherVPool(), new GatherFragmentCache(), new GatherBlockCache(), new GatherConfig(), new Confirm()]);
        data.completed = options.completed;
        self.step(0);
        self.activateStep();

        var cachingData = options.vPool.getCachingData(options.storageRouter.guid());
        // Make cachingData observable for our change monitoring purposes
        cachingData = ko.observable(cachingData);
        generic.makeChildrenObservables(cachingData);
        // Cleaning data
        data.vPool(options.vPool);
        data.storageRouter(options.storageRouter);
        data.storageDriver(options.storageDriver);

        // Set all configurable data
        data.proxyAmount(options.storageDriver.albaProxyGuids().length);
        data.fragmentCache(cachingData().fragmentCache());
        data.blockCache(cachingData().blockCache());
    };
});

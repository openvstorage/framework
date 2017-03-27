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
    '../build', './data', './gather', './confirm'
], function($, generic, build, data, GatherBackend, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.create_hprm_configs.title')));
        data.vPool(options.vPool);
        data.storageRouter(options.storageRouter);
        self.steps([new GatherBackend(), new Confirm()]);
        data.completed = options.completed;
        self.step(0);
        self.activateStep();

        // Cleaning data
        data.albaBackend(undefined);
        data.albaClientID('');
        data.albaClientSecret('');
        data.albaHost('');
        data.albaPort(80);
        data.albaPreset(undefined);
        data.albaUseLocalBackend(true);
        data.cacheOnRead(false);
        data.cacheOnWrite(false);
        data.cacheUseAlba(false);
        data.hprmPort(undefined);
        data.localPath('');
        data.localSize(undefined);
    };
});

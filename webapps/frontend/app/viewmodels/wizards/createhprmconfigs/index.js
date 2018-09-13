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
    '../build', './data', './gather', '../addvpool/gather_fragment_cache', '../addvpool/gather_block_cache', './confirm'
], function($, generic, Build, data, GatherInfo, GatherFragmentCache, GatherBlockCache, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        // Inherit
        Build.call(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.create_hprm_configs.title')));
        data.vPool(options.vPool);
        data.storageRouter(options.storageRouter);
        self.steps([
            new GatherInfo(),
            new GatherFragmentCache({data: data, customlocal: true, allowlocalbackend: true}),
            new GatherBlockCache({data: data, customlocal: true, allowlocalbackend: true}),
            new Confirm()
        ]);
        self.step(0);
        self.activateStep();

        // Required for vPool cache pages that are used in this wizard
        data.backend(undefined);
        data.backends([]);
        data.localHost(false);
        data.storageRoutersUsed([]);
        data.vPoolAdd(true);

        // Cleaning data
        data.hprmPort(undefined);
        data.identifier(data.vPool().name());

        // Fragment cache
        data.backendFC(undefined);
        data.cacheQuotaFC(undefined);
        data.cacheQuotaFCConfigured(false);
        data.clientIDFC('');
        data.clientSecretFC('');
        data.fragmentCacheOnRead(true);
        data.fragmentCacheOnWrite(true);
        data.hostFC('');
        data.localHostFC(true);
        data.localPathFC('');
        data.localSizeFC(undefined);
        data.portFC(80);
        data.presetFC(undefined);
        data.useFC(false);

        // Block cache
        data.backendBC(undefined);
        data.cacheQuotaBC(undefined);
        data.cacheQuotaBCConfigured(false);
        data.blockCacheOnRead(true);
        data.blockCacheOnWrite(true);
        data.clientIDBC('');
        data.clientSecretBC('');
        data.hostBC('');
        data.localHostBC(true);
        data.localPathBC('');
        data.localSizeBC(undefined);
        data.portBC(80);
        data.presetBC(undefined);
        data.supportsBC(true);
        data.useBC(false);
    };
});

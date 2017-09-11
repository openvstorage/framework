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
    '../build', './data', './gather_config', './gather_vpool', './gather_fragment_cache', './gather_block_cache', './confirm'
], function($, generic, build, data, GatherConfig, GatherVPool, GatherFragmentCache, GatherBlockCache, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));

        if (options.vPool !== undefined) {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.add_vpool.title_extend')));
            data.vPool(options.vPool);
            data.storageRouter(options.storageRouter);
        } else {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.add_vpool.title_add')));
            data.vPool(undefined);
            data.storageRouter(undefined);
        }
        self.steps([new GatherVPool(), new GatherFragmentCache(), new GatherBlockCache(), new GatherConfig(), new Confirm()]);
        data.completed = options.completed;
        self.step(0);
        self.activateStep();

        // Cleaning data (main)
        data.backend(undefined);
        data.backends([]);
        data.clientID('');
        data.clientSecret('');
        data.clusterSize(4);
        data.dtlEnabled(true);
        data.dtlMode({name: 'a_sync', disabled: false});
        data.dtlTransportMode({name: 'tcp'});
        data.host('');
        data.localHost(true);
        data.mdsSafety(3);
        data.name('');
        data.partitions(undefined);
        data.port(80);
        data.preset(undefined);
        data.proxyAmount(2);
        data.scoSize(4);
        data.storageIP(undefined);
        data.storageRoutersAvailable([]);
        data.storageRoutersUsed([]);
        data.writeBufferGlobal(1);
        data.writeBufferVolume(undefined);

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
        data.portBC(80);
        data.presetBC(undefined);
        data.supportsBC(true);
        data.useBC(false);
    };
});

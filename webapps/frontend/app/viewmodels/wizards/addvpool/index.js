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
    '../build', './data', './gather_config', './gather_vpool', './gather_backend', './confirm'
], function($, generic, build, data, GatherConfig, GatherVPool, GatherBackend, Confirm) {
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
        self.steps([new GatherVPool(), new GatherBackend(), new GatherConfig(), new Confirm()]);
        data.completed = options.completed;
        self.step(0);
        self.activateStep();

        // Cleaning data
        data.backend(undefined);
        data.backendAA(undefined);
        data.backends([]);
        data.backendsAA([]);
        data.clientID('');
        data.clientIDAA('');
        data.clientSecret('');
        data.clientSecretAA('');
        data.clusterSize(4);
        data.dtlEnabled(true);
        data.dtlMode({name: 'a_sync', disabled: false});
        data.dtlTransportMode({name: 'tcp'});
        data.fragmentCacheOnRead(true);
        data.fragmentCacheOnWrite(true);
        data.host('');
        data.hostAA('');
        data.ipAddresses([]);
        data.localHost(true);
        data.localHostAA(true);
        data.name('');
        data.partitions(undefined);
        data.port(80);
        data.portAA(80);
        data.preset(undefined);
        data.presetAA(undefined);
        data.reUsedStorageRouter(undefined);
        data.scoSize(4);
        data.scrubAvailable(false);
        data.storageIP(undefined);
        data.storageRoutersAvailable([]);
        data.storageRoutersUsed([]);
        data.useAA(false);
        data.writeBufferGlobal(1);
        data.writeBufferVolume(undefined);
    };
});

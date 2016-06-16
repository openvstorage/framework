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
    '../build', './data', './gather_config', './gather_vpool', './gather_cache_info', './gather_mgmtcenter', './gather_backend', './confirm'
], function($, generic, build, data, GatherConfig, GatherVPool, GatherCacheInfo, GatherMgmtCenter, GatherBackend, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));

        if (options.vPool !== undefined) {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.extend_vpool.title')));
            data.vPool(options.vPool);
            data.storageRouter(options.storageRouter);
        } else {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.add_vpool.title')));
            data.vPool(undefined);
            data.storageRouter(undefined);
        }
        self.steps([new GatherVPool(), new GatherBackend(), new GatherConfig(), new GatherCacheInfo(), new GatherMgmtCenter(), new Confirm()]);
        data.completed = options.completed;
        self.step(0);
        self.activateStep();

        // Cleaning data
        data.aaAccesskey('');
        data.aaHost('');
        data.aaLocalHost(true);
        data.aaPort(80);
        data.aaSecretkey('');
        data.accesskey('');
        data.albaAABackend(undefined);
        data.albaAAPreset(undefined);
        data.albaBackend(undefined);
        data.albaBackends([]);
        data.albaPreset(undefined);
        data.backend('alba');
        data.backends(['alba', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed']);
        data.cacheStrategy('on_read');
        data.clusterSize(4);
        data.dedupeMode('non_dedupe');
        data.distributedMtpt(undefined);
        data.dtlEnabled(true);
        data.dtlMode({name: 'a_sync', disabled: false});
        data.dtlTransportMode({name: 'tcp'});
        data.fragmentCacheOnRead(true);
        data.fragmentCacheOnWrite(true);
        data.hasMgmtCenter(false);
        data.host('');
        data.integratemgmt(false);
        data.ipAddresses([]);
        data.localHost(true);
        data.mgmtcenterIp(undefined);
        data.mgmtcenterLoaded(undefined);
        data.mgmtcenterName(undefined);
        data.mgmtcenterType(undefined);
        data.mgmtcenterUser(undefined);
        data.mountpoints([]);
        data.name('');
        data.partitions(undefined);
        data.port(80);
        data.readCacheSize(undefined);
        data.reUsedStorageRouter(undefined);
        data.rdmaEnabled(false);
        data.scoSize(4);
        data.scrubAvailable(false);
        data.secretkey('');
        data.sharedSize(undefined);
        data.storageDriver(undefined);
        data.storageDrivers([]);
        data.storageIP(undefined);
        data.storageRoutersAvailable([]);
        data.storageRoutersUsed([]);
        data.useAA(false);
        data.writeBuffer(undefined);
        data.writeCacheSize(undefined);
    };
});

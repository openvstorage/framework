// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'ovs/generic',
    '../build', './data', './gather_config', './gather_vpool', './gather_cache_info', './gather_mgmtcenter', './confirm'
], function($, generic, build, data, GatherConfig, GatherVPool, GatherCacheInfo, GatherMgmtCenter, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        // Setup
        self.modal(generic.tryGet(options, 'modal', false));

        if (options.vPool !== undefined) {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.extend_vpool.title')));
            data.vPool(options.vPool);
            data.target(options.storageRouter);
        } else {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.add_vpool.title')));
            data.vPool(undefined);
            data.target(undefined);
        }
        self.steps([new GatherVPool(), new GatherConfig(), new GatherCacheInfo(), new GatherMgmtCenter(), new Confirm()]);
        data.completed = options.completed;
        self.step(0);
        self.activateStep();

        // Cleaning data
        data.accesskey('');
        data.albaBackend(undefined);
        data.albaBackends([]);
        data.backend('alba');
        data.backends(['alba', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed']);
        data.cacheStrategy('on_read');
        data.dedupeMode('dedupe');
        data.distributedMtpt(undefined);
        data.dtlEnabled(true);
        data.dtlMode({name: 'a_sync', disabled: false});
        data.dtlTransportMode({name: 'tcp'});
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
        data.rdmaEnabled(false);
        data.scoSize(4);
        data.scrubAvailable(false);
        data.secretkey('');
        data.sharedSize(undefined);
        data.storageDriver(undefined);
        data.storageDrivers([]);
        data.storageIP(undefined);
        data.storageRouters([]);
        data.writeBuffer(undefined);
        data.writeCacheSize(undefined);
    };
});

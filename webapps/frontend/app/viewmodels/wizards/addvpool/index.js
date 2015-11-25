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
    '../build', './data', './gather_config', './gather_vpool', './gather_mountpoints', './gather_mgmtcenter', './confirm'
], function($, generic, build, data, GatherConfig, GatherVPool, GatherMountPoints, IntegrateMgmt, Confirm) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        // Setup
        data.extendVpool(generic.tryGet(options, 'extendVpool', false));
        self.modal(generic.tryGet(options, 'modal', false));

        if (data.extendVpool() === true) {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.extendvpool.title')));
            self.steps([new GatherMountPoints(), new IntegrateMgmt(), new Confirm()]);
            self.storagedriver_guid = "";
            data.storageRouter(options.pendingStorageRouters()[0]);
            data.target(options.pendingStorageRouters()[0]);
        } else {
            self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.addvpool.title')));
            self.steps([new GatherVPool(), new GatherMountPoints(), new GatherConfig(), new IntegrateMgmt(), new Confirm()]);
            data.storageRouter([]);
            data.target(undefined);
        }
        data.completed = options.completed;
        data.vPool(options.vPool);
        data.storageDriver(options.storagedriver);
        self.step(0);
        self.activateStep();

        // Cleaning data
        data.accesskey('');
        data.albaBackend(undefined);
        data.albaBackends(undefined);
        data.backend('alba');
        data.backends(['alba', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed']);
        data.cacheStrategy('on_read');
        data.dedupeMode('dedupe');
        data.distributedMtpt(undefined);
        data.dtlEnabled(false);
        data.dtlLocation('');
        data.dtlMode('no_sync');
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
        data.rdmaEnabled(false);
        data.scoSize(4);
        data.scrubAvailable(false);
        data.secretkey('');
        data.storageDrivers([]);
        data.storageIP(undefined);
        data.storageRouters([]);
        data.writeBuffer(undefined);
    };
});

// Copyright 2014 Open vStorage NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
        data.secretkey('');
        data.backend('local');
        data.mtptTemp('/var/tmp');
        data.mtptBFS(undefined);
        data.mtptReadCaches([]);
        data.mtptWriteCaches([]);
        data.storageIP(undefined);
        data.name('');
        data.host('');
        data.localHost(true);
        data.port(80);
        data.backends(['local', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed', 'alba']);
        data.storageRouters([]);
        data.storageDrivers([]);
        data.mountpoints([]);
        data.readcaches([]);
        data.writecaches([]);
        data.ipAddresses([]);
        data.albaBackends(undefined);
        data.albaBackend(undefined);
        data.integratemgmt(false);
        data.hasMgmtCenter(false);
        data.mgmtcenterUser(undefined);
        data.mgmtcenterIp(undefined);
        data.mgmtcenterType(undefined);
        data.mgmtcenterName(undefined);
        data.mgmtcenterLoaded(undefined);
        data.cacheStrategy('onread');
        data.dedupeMode('dedupe');
        data.dtlEnabled(true);
        data.dtlLocation('');
        data.dtlMode('nosync');
        data.scoSize(4);
        data.writeBuffer(undefined);
    };
});

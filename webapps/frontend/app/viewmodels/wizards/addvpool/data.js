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
    'knockout', 'jquery'
], function(ko, $){
    "use strict";
    var nameRegex, hostRegex, mountpointRegex, ipRegex, singleton;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$/;
    hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    mountpointRegex = /^(\/[a-zA-Z0-9\-_\.]+)+\/?$/;
    ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;

    singleton = function() {
        var wizardData = {
            accesskey:               ko.observable(''),
            albaBackend:             ko.observable(),
            albaBackends:            ko.observableArray(),
            albaPreset:              ko.observable(),
            arakoonFound:            ko.observable(false),
            backend:                 ko.observable('local'),
            backends:                ko.observableArray(['alba', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed', 'local']),
            cacheStrategies:         ko.observableArray(['on_read', 'on_write', 'none']),
            cacheStrategy:           ko.observable(''),
            dedupeMode:              ko.observable(''),
            dedupeModes:             ko.observableArray(['dedupe', 'non_dedupe']),
            dtlEnabled:              ko.observable(true),
            dtlLocation:             ko.observable(''),
            dtlMode:                 ko.observable(''),
            dtlModes:                ko.observableArray(['no_sync', 'a_sync', 'sync']),
            extendVpool:             ko.observable(false),
            hasMgmtCenter:           ko.observable(false),
            host:                    ko.observable('').extend({ regex: hostRegex }),
            integratemgmt:           ko.observable(),
            ipAddresses:             ko.observableArray([]),
            localHost:               ko.observable(true),
            mgmtcenterIp:            ko.observable(),
            mgmtcenterLoaded:        ko.observable(false),
            mgmtcenterName:          ko.observable(),
            mgmtcenterType:          ko.observable(),
            mgmtcenterUser:          ko.observable(),
            mountpointRegex:         mountpointRegex,
            mountpoints:             ko.observableArray([]),
            mtptBFS:                 ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-bfs' }),
            mtptCustomRC:            ko.observable(),
            mtptCustomRCs:           ko.observableArray([]),
            mtptCustomWC:            ko.observable(),
            mtptCustomWCs:           ko.observableArray([]),
            mtptDTL:                 ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-dtl' }),
            mtptMD:                  ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-md' }),
            mtptReadCaches:          ko.observableArray([]), // Final target containing read caches
            mtptTemp:                ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-temp' }),
            mtptWriteCaches:         ko.observableArray([]), // Final target containing write caches
            name:                    ko.observable('').extend({ regex: nameRegex }),
            partitions:              ko.observable(),
            port:                    ko.observable(80).extend({ numeric: { min: 1, max: 65536 } }),
            readCacheSize:           ko.observable(1).extend({numeric: {min: 1, max: 10240}}),
            readCacheAvailableSize:  ko.observable(),
            scoSize:                 ko.observable(4),
            scoSizes:                ko.observableArray([4, 8, 16, 32, 64, 128]),
            secretkey:               ko.observable(''),
            sharedSize:              ko.observable(),
            storageDriver:           ko.observable(),
            storageDrivers:          ko.observableArray([]),
            storageIP:               ko.observable().extend({ regex: ipRegex, identifier: 'storageip' }),
            storageRouter:           ko.observable(),
            storageRouters:          ko.observableArray([]),
            target:                  ko.observable(),
            vPool:                   ko.observable(),
            vPools:                  ko.observableArray([]),
            writeBuffer:             ko.observable(128).extend({numeric: {min: 128, max: 10240}}),
            writeCacheSize:          ko.observable(1).extend({numeric: {min: 1, max: 10240}}),
            writeCacheAvailableSize: ko.observable()
        }, resetAlbaBackends = function() {
            wizardData.albaBackends(undefined);
            wizardData.albaBackend(undefined);
            wizardData.albaPreset(undefined);
        };

        wizardData.accesskey.subscribe(resetAlbaBackends);
        wizardData.secretkey.subscribe(resetAlbaBackends);
        wizardData.host.subscribe(resetAlbaBackends);
        wizardData.port.subscribe(resetAlbaBackends);
        wizardData.localHost.subscribe(function() {
            wizardData.host('');
            wizardData.port(80);
            wizardData.accesskey('');
            wizardData.secretkey('');
            resetAlbaBackends();
        });
        wizardData.scoSize.subscribe(function(size) {
            if (size < 128) {
                wizardData.writeBuffer.min = 128;
            } else {
                wizardData.writeBuffer.min = 256;
            }
            wizardData.writeBuffer(wizardData.writeBuffer());
        });

        return wizardData;
    };
    return singleton();
});

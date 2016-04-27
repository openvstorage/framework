// Copyright 2016 iNuron NV
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
define(['knockout'], function(ko){
    "use strict";
    var nameRegex, hostRegex, ipRegex, singleton;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$/;
    hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;

    singleton = function() {
        var wizardData = {
            // 'aa' stands for Accelerated Alba
            aaAccesskey:             ko.observable('').extend({removeWhiteSpaces: null}),
            aaHost:                  ko.observable('').extend({regex: hostRegex}),
            aaLocalHost:             ko.observable(true),
            aaPort:                  ko.observable(80).extend({numeric: {min: 1, max: 65536}}),
            aaSecretkey:             ko.observable('').extend({removeWhiteSpaces: null}),
            accesskey:               ko.observable('').extend({removeWhiteSpaces: null}),
            albaAABackend:           ko.observable(),
            albaAABackends:          ko.observableArray([]),
            albaAAPreset:            ko.observable(),
            albaBackend:             ko.observable(),
            albaBackends:            ko.observableArray([]),
            albaPreset:              ko.observable(),
            backend:                 ko.observable('alba'),
            backends:                ko.observableArray(['alba', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed']),
            cacheStrategies:         ko.observableArray(['on_read', 'on_write', 'none']),
            cacheStrategy:           ko.observable(''),
            clusterSize:             ko.observable(4),
            clusterSizes:            ko.observableArray([4, 8, 16, 32, 64]),
            dedupeMode:              ko.observable(''),
            dedupeModes:             ko.observableArray(['dedupe', 'non_dedupe']),
            distributedMtpt:         ko.observable(),
            dtlEnabled:              ko.observable(true),
            dtlMode:                 ko.observable(),
            dtlModes:                ko.observableArray([{name: 'no_sync', disabled: false}, {name: 'a_sync', disabled: false}, {name: 'sync', disabled: false}]),
            dtlTransportMode:        ko.observable({name: 'tcp'}),
            dtlTransportModes:       ko.observableArray([{name: 'tcp', disabled: false}, {name: 'rdma', disabled: true}]),
            fragmentCacheOnRead:     ko.observable(true),
            fragmentCacheOnWrite:    ko.observable(false),
            hasMgmtCenter:           ko.observable(false),
            host:                    ko.observable('').extend({regex: hostRegex}),
            integratemgmt:           ko.observable(),
            ipAddresses:             ko.observableArray([]),
            localHost:               ko.observable(true),
            mgmtcenterIp:            ko.observable(),
            mgmtcenterLoaded:        ko.observable(false),
            mgmtcenterName:          ko.observable(),
            mgmtcenterType:          ko.observable(),
            mgmtcenterUser:          ko.observable(),
            mountpoints:             ko.observableArray([]),
            name:                    ko.observable('').extend({regex: nameRegex}),
            partitions:              ko.observable(),
            port:                    ko.observable(80).extend({ numeric: {min: 1, max: 65536}}),
            rdmaEnabled:             ko.observable(false),
            readCacheSize:           ko.observable(1).extend({numeric: {min: 1, max: 10240}}),
            readCacheAvailableSize:  ko.observable(),
            reUsedStorageRouter:     ko.observable(),  // Connection info for this storagerouter will be used for accelerated ALBA
            scrubAvailable:          ko.observable(false),
            scoSize:                 ko.observable(4),
            scoSizes:                ko.observableArray([4, 8, 16, 32, 64, 128]),
            secretkey:               ko.observable('').extend({removeWhiteSpaces: null}),
            sharedSize:              ko.observable(),
            storageDriver:           ko.observable(),
            storageDrivers:          ko.observableArray([]),
            storageIP:               ko.observable().extend({regex: ipRegex, identifier: 'storageip'}),
            storageRouter:           ko.observable(),
            storageRoutersAvailable: ko.observableArray([]),
            storageRoutersUsed:      ko.observableArray([]),
            useAA:                   ko.observable(false),
            v260Migration:           ko.observable(false),
            vPool:                   ko.observable(),
            vPools:                  ko.observableArray([]),
            writeBuffer:             ko.observable(128).extend({numeric: {min: 128, max: 10240}}),
            writeCacheSize:          ko.observable(1).extend({numeric: {min: 1, max: 10240}}),
            writeCacheAvailableSize: ko.observable()
        }, resetAlbaBackends = function() {
            wizardData.albaBackends([]);
            wizardData.albaBackend(undefined);
            wizardData.albaPreset(undefined);
        }, resetAlbaAABackends = function() {
            wizardData.albaAABackends([]);
            wizardData.albaAABackend(undefined);
            wizardData.albaAAPreset(undefined);
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
        wizardData.aaAccesskey.subscribe(resetAlbaAABackends);
        wizardData.aaSecretkey.subscribe(resetAlbaAABackends);
        wizardData.aaHost.subscribe(resetAlbaAABackends);
        wizardData.aaPort.subscribe(resetAlbaAABackends);
        wizardData.aaLocalHost.subscribe(function() {
            wizardData.aaHost('');
            wizardData.aaPort(80);
            wizardData.aaAccesskey('');
            wizardData.aaSecretkey('');
            wizardData.reUsedStorageRouter(undefined);
            resetAlbaAABackends();
        });
        wizardData.scoSize.subscribe(function(size) {
            if (size < 128) {
                wizardData.writeBuffer.min = 128;
            } else {
                wizardData.writeBuffer.min = 256;
            }
            wizardData.writeBuffer(wizardData.writeBuffer());
        });
        wizardData.reUsedStorageRouter.subscribe(function(sr) {
            wizardData.aaHost('');
            wizardData.aaPort(80);
            wizardData.aaAccesskey('');
            wizardData.aaSecretkey('');
            if (sr !== undefined && wizardData.vPool() !== undefined && wizardData.vPool().metadata().hasOwnProperty(sr.guid())) {
                var md = wizardData.vPool().metadata()[sr.guid()];
                if (md.hasOwnProperty('connection')) {
                    wizardData.aaHost(md.connection.host);
                    wizardData.aaPort(md.connection.port);
                    wizardData.aaAccesskey(md.connection.client_id);
                    wizardData.aaSecretkey(md.connection.client_secret);
                }
            }
        });

        // Computed
        wizardData.vPoolAdd = ko.computed(function() {
            return wizardData.vPool() === undefined;
        });
        wizardData.editBackend = ko.computed(function() {
            return wizardData.vPoolAdd() || wizardData.v260Migration();
        });
        wizardData.enhancedPresets = ko.computed(function(){
            if (wizardData.albaBackend() === undefined){
                wizardData.albaPreset(undefined);
                return []
            }
            if (!wizardData.albaBackend().enhancedPresets().contains(wizardData.albaPreset())){
                wizardData.albaPreset(wizardData.albaBackend().enhancedPresets()[0]);
            }
            return wizardData.albaBackend().enhancedPresets();
        });
        wizardData.enhancedAAPresets = ko.computed(function(){
            if (wizardData.albaAABackend() === undefined){
                wizardData.albaAAPreset(undefined);
                return []
            }
            if (!wizardData.albaAABackend().enhancedPresets().contains(wizardData.albaAAPreset())){
                wizardData.albaAAPreset(wizardData.albaAABackend().enhancedPresets()[0]);
            }
            return wizardData.albaAABackend().enhancedPresets();
        });
        return wizardData;
    };
    return singleton();
});

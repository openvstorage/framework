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
define(['jquery', 'knockout'], function($, ko){
    "use strict";
    var nameRegex, hostRegex, ipRegex, singleton, parsePresets;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$/;
    hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;

    parsePresets = function(backend) {
        var presets = [], policies, newPolicy, isAvailable, isActive, inUse,
            policyMapping = ['grey', 'black', 'green'], worstPolicy, replication, policyObject;
        $.each(backend.presets, function(index, preset) {
            worstPolicy = 0;
            policies = [];
            replication = undefined;
            $.each(preset.policies, function(jndex, policy) {
                policyObject = JSON.parse(policy.replace('(', '[').replace(')', ']'));
                isAvailable = preset.policy_metadata[policy].is_available;
                isActive = preset.policy_metadata[policy].is_active;
                inUse = preset.policy_metadata[policy].in_use;
                newPolicy = {
                    text: policy,
                    color: 'grey',
                    isActive: false,
                    k: policyObject[0],
                    m: policyObject[1],
                    c: policyObject[2],
                    x: policyObject[3]
                };
                if (isAvailable) {
                    newPolicy.color = 'black';
                }
                if (isActive) {
                    newPolicy.isActive = true;
                }
                if (inUse) {
                    newPolicy.color = 'green';
                }
                worstPolicy = Math.max(policyMapping.indexOf(newPolicy.color), worstPolicy);
                policies.push(newPolicy);
            });
            if (preset.policies.length === 1) {
                policyObject = JSON.parse(preset.policies[0].replace('(', '[').replace(')', ']'));
                if (policyObject[0] === 1 && policyObject[0] + policyObject[1] === policyObject[3] && policyObject[2] === 1) {
                    replication = policyObject[0] + policyObject[1];
                }
            }
            presets.push({
                policies: policies,
                name: preset.name,
                compression: preset.compression,
                fragSize: preset.fragment_size,
                encryption: preset.fragment_encryption,
                color: policyMapping[worstPolicy],
                inUse: preset.in_use,
                isDefault: preset.is_default,
                replication: replication
            });
        });
        return presets.sort(function(preset1, preset2) {
            return preset1.name.toLowerCase() < preset2.name.toLowerCase() ? -1 : 1;
        });
    };

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
            dedupeMode:              ko.observable('non_dedupe'),
            dedupeModes:             ko.observableArray(['dedupe', 'non_dedupe']),
            distributedMtpt:         ko.observable(),
            dtlMode:                 ko.observable(),
            dtlModes:                ko.observableArray([{name: 'no_sync', disabled: false}, {name: 'a_sync', disabled: false}, {name: 'sync', disabled: false}]),
            dtlTransportMode:        ko.observable({name: 'tcp'}),
            dtlTransportModes:       ko.observableArray([{name: 'tcp', disabled: false}, {name: 'rdma', disabled: true}]),
            fragmentCacheOnRead:     ko.observable(true),
            fragmentCacheOnWrite:    ko.observable(true),
            host:                    ko.observable('').extend({regex: hostRegex}),
            ipAddresses:             ko.observableArray([]),
            localHost:               ko.observable(true),
            mountpoints:             ko.observableArray([]),
            name:                    ko.observable('').extend({regex: nameRegex}),
            partitions:              ko.observable(),
            port:                    ko.observable(80).extend({ numeric: {min: 1, max: 65536}}),
            rdmaEnabled:             ko.observable(false),
            readCacheSize:           ko.observable().extend({numeric: {min: 1, max: 10240, allowUndefined: true}}),
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
            volumedriverEdition:     ko.observable(),
            vPool:                   ko.observable(),
            vPools:                  ko.observableArray([]),
            writeBuffer:             ko.observable(128).extend({numeric: {min: 128, max: 10240}}),
            writeCacheSize:          ko.observable().extend({numeric: {min: 1, max: 10240, allowUndefined: true}}),
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
            if (sr === undefined && !wizardData.localHost()) {
                wizardData.aaHost('');
                wizardData.aaPort(80);
                wizardData.aaAccesskey('');
                wizardData.aaSecretkey('');
            }
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
        wizardData.cacheStrategy.subscribe(function() {
            wizardData.writeCacheSize(undefined);
            wizardData.readCacheSize(undefined);
        });

        // Computed
        wizardData.vPoolAdd = ko.computed(function() {
            return wizardData.vPool() === undefined;
        });
        wizardData.enhancedPresets = ko.computed(function(){
            if (wizardData.albaBackend() === undefined){
                wizardData.albaPreset(undefined);
                return []
            }
            var presets = parsePresets(wizardData.albaBackend()),
                presetNames = [];
            $.each(wizardData.albaBackend().presets, function(_, preset) {
                presetNames.push(preset.name);
            });
            if (wizardData.albaPreset() === undefined) {
                wizardData.albaPreset(presets[0]);
            } else if (!presetNames.contains(wizardData.albaPreset().name)) {
                wizardData.albaPreset(presets[0]);
            }
            return presets;
        });
        wizardData.enhancedAAPresets = ko.computed(function(){
            if (wizardData.albaAABackend() === undefined){
                wizardData.albaAAPreset(undefined);
                return []
            }
            var presets = parsePresets(wizardData.albaAABackend()),
                presetNames = [];
            $.each(wizardData.albaAABackend().presets, function(_, preset) {
                presetNames.push(preset.name);
            });
            if (wizardData.albaAAPreset() === undefined) {
                wizardData.albaAAPreset(presets[0]);
            } else if (!presetNames.contains(wizardData.albaAAPreset().name)) {
                wizardData.albaAAPreset(presets[0]);
            }
            return presets;
        });
        return wizardData;
    };
    return singleton();
});

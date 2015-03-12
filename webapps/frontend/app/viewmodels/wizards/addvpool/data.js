// Copyright 2014 CloudFounders NV
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
define(['knockout', 'jquery'], function(ko, $){
    "use strict";
    var nameRgex, hostRegex, mountpointRegex, ipRegex, singleton;
    nameRgex = /^[0-9a-z]+(\-+[0-9a-z]+)*$/;
    hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    mountpointRegex = /^(\/[a-zA-Z0-9\-_ \.]+)+\/?$/;
    ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;

    singleton = function() {
        var mtptData = {
            target:          ko.observable(),
            files:           ko.observable(),
            accesskey:       ko.observable(''),
            secretkey:       ko.observable(''),
            allowVPool:      ko.observable(true),
            localHost:       ko.observable(true),
            backend:         ko.observable('local'),
            mtptTemp:        ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-temp' }),
            mtptBFS:         ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-bfs' }),
            mtptMD:          ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-md' }),
            mtptReadCaches:  ko.observableArray([]), // Final target containing read caches
            mtptCustomRCs:   ko.observableArray([]),
            mtptCustomRC:    ko.observable(),
            mtptWriteCaches: ko.observableArray([]), // Final target containing write caches
            mtptCustomWCs:   ko.observableArray([]),
            mtptCustomWC:    ko.observable(),
            mtptFOC:         ko.observable().extend({ regex: mountpointRegex, identifier: 'mtpt-foc' }),
            storageIP:       ko.observable().extend({ regex: ipRegex, identifier: 'storageip' }),
            name:            ko.observable('').extend({ regex: nameRgex }),
            host:            ko.observable('').extend({ regex: hostRegex }),
            port:            ko.observable(80).extend({ numeric: { min: 1, max: 65536 } }),
            timeout:         ko.observable(600).extend({ numeric: {}}),
            albaBackend:     ko.observable(),
            backends:        ko.observableArray(['local', 'ceph_s3', 'amazon_s3', 'swift_s3', 'distributed', 'alba']),
            storageRouters:  ko.observableArray([]),
            storageDrivers:  ko.observableArray([]),
            mountpoints:     ko.observableArray([]),
            ipAddresses:     ko.observableArray([]),
            albaBackends:    ko.observableArray(),
            hasCinder:       ko.observable(),
            configCinder:    ko.observable(),
            cinderUser:      ko.observable('admin'),
            cinderPassword:  ko.observable(''),
            cinderTenant:    ko.observable('admin'),
            cinderCtrlIP:    ko.observable('').extend({ regex: ipRegex })
        }, resetAlbaBackends = function() {
            mtptData.albaBackends(undefined);
            mtptData.albaBackend(undefined);
        };

        mtptData.allReadMountpoints = ko.computed(function() {
            var returnValue = [];
            $.each(mtptData.mountpoints(), function(i, e) {
                returnValue.push(e);
            });
            $.each(mtptData.mtptCustomRCs(), function(i, e) {
                returnValue.push(e);
            });
            return returnValue;
        });
        mtptData.readCacheDistributor = ko.computed(function() {
            return mtptData.mtptReadCaches();
        });
        mtptData.readCacheDistributor.push = function(element) {
            mtptData.mtptReadCaches.push(element);
        };
        mtptData.readCacheDistributor.remove = function(element) {
            if ($.inArray(element, mtptData.mtptCustomRCs()) !== -1) {
                mtptData.mtptCustomRCs.remove(element);
            }
            if ($.inArray(element, mtptData.mtptReadCaches()) !== -1) {
                mtptData.mtptReadCaches.remove(element);
            }
        };
        mtptData.readCacheDistributor.isObservableArray = true;

        mtptData.allWriteMountpoints = ko.computed(function() {
            var returnValue = [];
            $.each(mtptData.mountpoints(), function(i, e) {
                returnValue.push(e);
            });
            $.each(mtptData.mtptCustomWCs(), function(i, e) {
                returnValue.push(e);
            });
            return returnValue;
        });
        mtptData.writeCacheDistributor = ko.computed(function() {
            return mtptData.mtptWriteCaches();
        });
        mtptData.writeCacheDistributor.push = function(element) {
            mtptData.mtptWriteCaches.push(element);
        };
        mtptData.writeCacheDistributor.remove = function(element) {
            if ($.inArray(element, mtptData.mtptCustomWCs()) !== -1) {
                mtptData.mtptCustomWCs.remove(element);
            }
            if ($.inArray(element, mtptData.mtptWriteCaches()) !== -1) {
                mtptData.mtptWriteCaches.remove(element);
            }
        };
        mtptData.writeCacheDistributor.isObservableArray = true;

        mtptData.accesskey.subscribe(resetAlbaBackends);
        mtptData.secretkey.subscribe(resetAlbaBackends);
        mtptData.host.subscribe(resetAlbaBackends);
        mtptData.port.subscribe(resetAlbaBackends);
        mtptData.localHost.subscribe(function() {
            mtptData.host('');
            mtptData.port(80);
            mtptData.accesskey('');
            mtptData.secretkey('');
            resetAlbaBackends();
        });

        return mtptData;
    };
    return singleton();
});

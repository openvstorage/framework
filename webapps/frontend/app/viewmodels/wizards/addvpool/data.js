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
define(['knockout'], function(ko){
    "use strict";
    var nameRgex, hostRegex, mountpointRegex, ipRegex, singleton;
    nameRgex = /^[0-9a-zA-Z]+([\\-_]+[0-9a-zA-Z]+)*$/;
    hostRegex = /^((((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|((([a-z0-9]+[\.\-])*[a-z0-9]+\.)+[a-z]{2,4}))$/;
    mountpointRegex = /^(\/[a-zA-Z0-9\-_ \.]+)+\/?$/;
    ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;
    singleton = function() {
        return {
            target:         ko.observable(),
            accesskey:      ko.observable(''),
            secretkey:      ko.observable(''),
            backend:        ko.observable('LOCAL'),
            mtptTemp:       ko.observable().extend({ regex: mountpointRegex }),
            mtptDFS:        ko.observable().extend({ regex: mountpointRegex }),
            mtptMD:         ko.observable().extend({ regex: mountpointRegex }),
            mtptCache:      ko.observable().extend({ regex: mountpointRegex }),
            storageIP:      ko.observable().extend({ regex: ipRegex }),
            name:           ko.observable('').extend({ regex: nameRgex }),
            host:           ko.observable('').extend({ regex: hostRegex }),
            port:           ko.observable(80).extend({ numeric: { min: 1, max: 65536 }}),
            timeout:        ko.observable(600).extend({ numeric: {}}),
            vRouterPort:    ko.observable(12322).extend({ numeric: { min: 1, max: 65536 }}),
            backends:       ko.observableArray(['LOCAL', 'CEPH_S3', 'AMAZON_S3', 'SWIFT_S3']),
            vsas:           ko.observableArray([]),
            vsrs:           ko.observableArray([]),
            mountpoints:    ko.observableArray([]),
            ipAddresses:    ko.observableArray([])
        };
    };
    return singleton();
});

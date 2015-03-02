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
    var nameRgex, ipRegex, singleton;
    nameRgex = /^[0-9a-zA-Z]+([\\-_]+[0-9a-zA-Z]+)*$/;
    ipRegex = /^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$/;
    singleton = function() {
        return {
            username:   ko.observable(),
            password:   ko.observable(),
            centerType: ko.observable('VCENTER', 'OPENSTACK'),
            name:       ko.observable().extend({ regex: nameRgex }),
            ipAddress:  ko.observable().extend({ regex: ipRegex }),
            port:       ko.observable(443).extend({ numeric: { min: 1, max: 65536 } }),
            types:      ko.observableArray(['VCENTER', 'OPENSTACK'])
        };
    };
    return singleton();
});

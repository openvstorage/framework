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

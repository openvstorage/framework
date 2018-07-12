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
define(['jquery', 'knockout', 'ovs/generic'], function($, ko, generic){
    "use strict";
    var singleton = function() {
        var wizardData = {
            name:                  ko.observable('').extend({ regex: generic.vdiskNameRegex }),
            sizeEntry:             ko.observable(0).extend({ numeric: { min: 1, max: 65535 }, throttle: 500}),
            storageRouter:         ko.observable(),
            storageRouters:        ko.observableArray([]),
            vPool:                 ko.observable(),
            vPools:                ko.observableArray([]),
            vPoolUsableBackendMap: ko.observable({})
        };

        // Computed
        wizardData.size = ko.computed(function () {
            return wizardData.sizeEntry() * Math.pow(1024, 3);
        });
        return wizardData;
    };
    return singleton();
});

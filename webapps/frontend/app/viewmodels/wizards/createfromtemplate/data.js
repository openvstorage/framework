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
    var singleton = function() {
        return {
            mode:                   ko.observable(),
            guid:                   ko.observable(),
            vObject:                ko.observable(),
            amount:                 ko.observable(1).extend({ numeric: { min: 1 } }),
            startnr:                ko.observable(1).extend({ numeric: { min: 0 } }),
            name:                   ko.observable(),
            description:            ko.observable(''),
            selectedStorageRouters: ko.observableArray([]),
            storageRouters:         ko.observableArray([]),
            names:                  ko.observableArray([])
        };
    };
    return singleton();
});

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
    var nameRegex, singleton;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$/;
    singleton = function() {
        return {
            backends:                ko.observableArray([]),
            backendType:             ko.observable(),
            backendTypes:            ko.observableArray([]),
            name:                    ko.observable().extend({ regex: nameRegex }),
            storageRoutersChecked:   ko.observable(false),
            validStorageRouterFound: ko.observable()
        };
    };
    return singleton();
});

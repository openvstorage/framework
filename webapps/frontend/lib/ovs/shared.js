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
        var pluginData = {};
        return {
            messaging      : undefined,
            tasks          : undefined,
            authentication : undefined,
            defaultLanguage: 'en-US',
            language       : 'en-US',
            mode           : ko.observable('full'),
            routing        : undefined,
            footerData     : ko.observable(ko.observable()),
            nodes          : undefined,
            identification : ko.observable(),
            releaseName    : '',
            pluginData     : pluginData,
            user           : {
                username: ko.observable(),
                guid    : ko.observable(),
                roles   : ko.observableArray([])
            },
            hooks          : {
                dashboards: [],
                wizards   : {},
                pages     : {}
            }
        };
    };
    return singleton();
});

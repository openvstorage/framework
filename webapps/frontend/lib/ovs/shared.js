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
define([
    'knockout',
    'ovs/routing',
    'ovs/services/messaging', 'ovs/services/authentication', 'ovs/services/tasks'],
    function(ko,
             routing,
             messaging, authentication, tasks){
    "use strict";
    var singleton = function() {
        var pluginData = {};
        return {
            messaging      : messaging,
            tasks          : tasks,
            authentication : authentication,
            defaultLanguage: 'en-US',
            language       : 'en-US',
            mode           : ko.observable('full'),
            routing        : routing,
            footerData     : ko.observable(ko.observable()),
            nodes          : [],
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

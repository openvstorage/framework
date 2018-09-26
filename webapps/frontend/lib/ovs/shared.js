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
    'knockout', 'jquery',
    'ovs/routing'],
    function(ko, $,
             routing){
    "use strict";
    var modes = Object.freeze({
        FULL: 'full'
    });

    function Shared() {
        var self = this;

        self.mode           = ko.observable(modes.FULL);
        self.routing        = routing;
        self.footerData     = ko.observable(ko.observable());
        self.identification = ko.observable();
        self.releaseName    = '';
        self.pluginData     = {};
        self.hooks          = {
            dashboards: [],
            wizards: {},
            pages: {}
        };

        self.fullMode = ko.pureComputed(function() {
            return self.mode() === modes.FULL
        })
    }
    Shared.prototype = { };
    return new Shared();
});

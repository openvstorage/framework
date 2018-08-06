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
    'jquery', 'knockout',
    'viewmodels/containers/shared/base_container', 'viewmodels/containers/backend/backendtype', 'viewmodels/containers/backend/backend',
    'viewmodels/services/backend'
], function($, ko,
            BaseContainer, BackendType, Backend,
            backendService){
    "use strict";
    var viewModelMapping  = {
        backendTypes: {
            key: function (data) {
                return ko.utils.unwrapObservable(data.guid)
            },
            create: function (options) {
                var guid = options.data.guid;
                var backendType = new BackendType(guid);
                backendType.fillData((ko.utils.unwrapObservable(options.data)));
                backendType.loaded(true);
                return backendType;
            },
            update: function (options){
                options.target.fillData(options.data);
                return options.target
            }
        },
        backends: {
            key: function (data) {
                return ko.utils.unwrapObservable(data.guid)
            },
            create: function (options) {
                var guid = options.data.guid;
                var backend = new Backend(guid);
                backend.fillData((ko.utils.unwrapObservable(options.data)));
                backend.loaded(true);
                return backend;
            },
            update: function (options){
                options.target.fillData(options.data);
                return options.target
            }
        }
    };
    function AddBackendData(data) {
        var self = this;
        BaseContainer.call(self);  // Inheritance

        self.name = ko.observable().extend({ regex: backendService.nameRegex });
        self.selectedBackendType = ko.observable();

        var vmData = $.extend({
            backends: [],
            backendTypes: [],
            name: undefined
        }, data || {});
        ko.mapping.fromJS(vmData, viewModelMapping, self);
    }
    AddBackendData.prototype = $.extend({}, BaseContainer.prototype);
    return AddBackendData
});

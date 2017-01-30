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
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid, parent) {
        var self = this;

        // Variables
        self.parent = parent;

        // Handles
        self.loadHandle = undefined;

        // Observables
        self.aliases      = ko.observable();
        self.filesystem   = ko.observable();
        self.guid         = ko.observable(guid);
        self.loaded       = ko.observable(false);
        self.loading      = ko.observable(false);
        self.mountpoint   = ko.observable();
        self.offset       = ko.observable().extend({ format: generic.formatBytes });
        self.relativeSize = ko.observable();
        self.roles        = ko.observableArray([]);
        self.size         = ko.observable().extend({ format: generic.formatBytes });
        self.state        = ko.observable();
        self.trigger      = ko.observable();
        self.usage        = ko.observable();

        // Functions
        self.fillData = function(data) {
            self.filesystem(data.filesystem);
            self.state(data.state);
            self.offset(data.offset);
            self.size(data.size);
            if (self.relativeSize() === undefined && parent.size !== undefined && parent.size.raw !== undefined && parent.size.raw() !== 0) {
                self.relativeSize(Math.round(data.size / parent.size.raw() * 100))
            }
            self.mountpoint(data.mountpoint);
            self.aliases(data.aliases);
            self.usage(generic.tryGet(data, 'usage', undefined));
            self.roles(data.roles);

            self.loaded(true);
            self.loading(false);
            self.trigger(generic.getTimestamp());
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('diskpartitions/' + self.guid())
                        .done(function(data) {
                            self.fillData(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.loading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});

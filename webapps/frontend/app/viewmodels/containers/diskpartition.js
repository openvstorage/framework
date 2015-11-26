// Copyright 2015 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle = undefined;

        // Observables
        self.trigger           = ko.observable();
        self.loading           = ko.observable(false);
        self.loaded            = ko.observable(false);
        self.guid              = ko.observable(guid);
        self.id                = ko.observable();
        self.filesystem        = ko.observable();
        self.state             = ko.observable();
        self.inode             = ko.observable();
        self.offset            = ko.observable().extend({ format: generic.formatBytes });
        self.size              = ko.observable().extend({ format: generic.formatBytes });
        self.mountpoint        = ko.observable();
        self.path              = ko.observable();
        self.usage             = ko.observable();
        self.roles             = ko.observableArray([]);

        // Functions
        self.fillData = function(data) {
            self.id(data.id);
            self.filesystem(data.filesystem);
            self.state(data.state);
            self.inode(data.inode);
            self.offset(data.offset);
            self.size(data.size);
            self.mountpoint(data.mountpoint);
            self.path(data.path);
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

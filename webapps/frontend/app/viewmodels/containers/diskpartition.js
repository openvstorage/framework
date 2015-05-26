// Copyright 2015 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
        self.offset            = ko.observable();
        self.size              = ko.observable();
        self.mountpoint        = ko.observable();
        self.path              = ko.observable();

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.id, data, 'id');
            generic.trySet(self.filesystem, data, 'filesystem');
            generic.trySet(self.state, data, 'state');
            generic.trySet(self.inode, data, 'inode');
            generic.trySet(self.offset, data, 'offset');
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.mountpoint, data, 'mountpoint');
            generic.trySet(self.path, data, 'path');

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

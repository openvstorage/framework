// Copyright 2016 iNuron NV
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
    'ovs/api', 'ovs/generic'
], function($, ko, api, generic) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.canBeDeletedHandle = undefined;

        // Observables
        self.canDelete         = ko.observable(false);
        self.clusterIP         = ko.observable();
        self.guid              = ko.observable(guid);
        self.loaded            = ko.observable(false);
        self.loading           = ko.observable(false);
        self.mountpoint        = ko.observable();
        self.name              = ko.observable();
        self.ports             = ko.observableArray([0, 0, 0]);
        self.storageDriverID   = ko.observable();
        self.storageIP         = ko.observable();
        self.storageRouterGuid = ko.observable();

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.clusterIP, data, 'cluster_ip');
            generic.trySet(self.mountpoint, data, 'mountpoint');
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.ports, data, 'ports');
            generic.trySet(self.storageDriverID, data, 'storagedriver_id');
            generic.trySet(self.storageIP, data, 'storage_ip');
            generic.trySet(self.storageRouterGuid, data, 'storagerouter_guid');
            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.get('storagedrivers/' + self.guid())
                    .done(function(data) {
                        self.fillData(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
        self.canBeDeleted = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.canBeDeletedHandle)) {
                    self.canBeDeletedHandle = api.post('storagedrivers/' + self.guid() + '/can_be_deleted')
                        .done(function (data) {
                            self.canDelete(data);
                            deferred.resolve();
                        })
                        .fail(function() {
                            self.canDelete(false);
                            deferred.reject();
                        });
                }
            }).promise();
        };
    };
});

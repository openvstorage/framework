// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define(['knockout', 'ovs/generic'], function(ko, generic){
    "use strict";
    var singleton = function() {
        var data = {
            partition:     ko.observable(),
            disk:          ko.observable(),
            storageRouter: ko.observable(),
            roles:         ko.observableArray([]),
            currentUsage:  ko.observable()
        };
        data.availableRoles = ko.computed(function() {
            if (data.currentUsage() === undefined) {
                return [];
            }
            var db = {name: 'db'},
                read = {name: 'read'},
                write = {name: 'write'},
                scrub = {name: 'scrub'},
                roles = [db, read, write, scrub],
                dictionary = {DB: db, READ: read, WRITE: write, SCRUB: scrub};
            $.each(data.currentUsage(), function(role, partitions) {
                if (role !== 'BACKEND') {
                    $.each(partitions, function (index, partition) {
                        if (partition.guid === data.partition().guid() && partition.in_use === true) {
                            dictionary[role].disabled = true;
                        }
                        if (partition.guid === data.partition().guid() && partition.in_use === false) {
                            dictionary[role].disabled = false;
                        }
                    });
                }
            });
            // @TODO: Make sure the DB and SCRUB role is disabled when there's already one on any of this storagerouter's partitions
            // @TODO: Also take into account that when not in use yet, they always have to be removable
            return roles
        });
        return data;
    };
    return singleton();
});

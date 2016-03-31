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
define(['jquery', 'knockout'], function($, ko){
    "use strict";
    var nameRegex = /^[0-9a-zA-Z][\-_a-zA-Z0-9]{1,48}[a-zA-Z0-9]$/,
        singleton = function() {
        var data = {
            name:           ko.observable('').extend({ regex: nameRegex }),
            vDisk:          ko.observable(),
            vDisks:         ko.observableArray([]),
            snapshot:       ko.observable(),
            storageRouter:  ko.observable(),
            storageRouters: ko.observableArray([])
        };
        data.availableSnapshots = ko.computed(function() {
            var snapshots = [undefined];
            if (data.vDisk() !== undefined) {
                $.each(data.vDisk().snapshots(), function(index, snapshot) {
                    snapshots.push(snapshot);
                });
            }
            return snapshots;
        });
        data.displaySnapshot = function(item) {
            if (item === undefined) {
                return $.t('ovs:wizards.clone.gather.newsnapshot');
            }
            var text = '', date = new Date(item.timestamp * 1000);
            if (item.label !== undefined && item.label !== '') {
                text += item.label
            }
            text += ' (' + date.toLocaleDateString() + ' ' + date.toLocaleTimeString() + ')';
            return text;
        };
        return data;
    };
    return singleton();
});

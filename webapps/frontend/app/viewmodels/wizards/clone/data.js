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
define(['jquery', 'knockout', 'ovs/generic'], function($, ko, generic){
    "use strict";
        var singleton = function() {
        var data = {
            name:           ko.observable('').extend({ regex: generic.vdiskNameRegex }),
            vDisk:          ko.observable(),
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

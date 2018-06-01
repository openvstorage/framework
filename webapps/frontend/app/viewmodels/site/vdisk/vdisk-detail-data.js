// Copyright (C) 2018 iNuron NV
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
define(['jquery', 'knockout',
        'viewmodels/containers/shared/base_container', 'viewmodels/containers/vdisk/vdisk',
        'viewmodels/services/subscriber'
],function($, ko,
           BaseContainer,
           VDisk,
           subscriberService){
    "use strict";
    // This data is not a singleton but a constructor

    /**
     * Constructor for shared data between the VDiskDetailPage and its plugins
     * @constructor
     */
    var viewModelMapping = {
        vdisk: {
            key: function(data){
                return ko.utils.unwrapObservable(data.guid)
            },
            create: function (options) {
                var vdisk = new VDisk(options.data.guid);
                vdisk.fillData(options.data);
                return vdisk;
            },
            update: function (options) {
                options.target.fillData(options.data);
                return options.target
            }
        }
    };
    function VDiskDetailData(data) {
        var self = this;

        BaseContainer.call(self);
        var vmData = $.extend({
            vdisk: {},
            blocked_actions: []
        }, data || {});
        ko.mapping.fromJS(vmData, viewModelMapping, self);
        subscriberService.trigger('shared_data:create', self);
    }

    var functions = {
        // Event functions
        update: function(data) {
            var self = this;
            BaseContainer.prototype.update.call(self, data);
            subscriberService.trigger('shared_data:create', self);
        }
    };


    VDiskDetailData.prototype = $.extend({}, functions, BaseContainer.prototype);
    return {
        view_id: 'vdisk_detail',
        constructor: VDiskDetailData
    }
});

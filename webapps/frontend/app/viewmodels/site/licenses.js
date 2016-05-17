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
    'knockout', 'plugins/dialog', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api',
    '../containers/license', '../wizards/addlicense/index'
], function(ko, dialog, $, shared, generic, api, License, AddLicenseWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.widgets        = [];
        self.shared         = shared;
        self.guard          = { authenticated: true };
        self.generic        = generic;
        self.licensesHandle = {};
        self.licenseHeaders = [
            { key: 'component',  value: $.t('ovs:generic.component'),  width: 250       },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: 250       },
            { key: 'metadata',   value: $.t('ovs:generic.metadata'),   width: undefined }
        ];

        // Observable
        self.licenses    = ko.observableArray([]);

        // Functions
        self.addLicense = function() {
            dialog.show(new AddLicenseWizard({
                modal: true
            }));
        };
        self.loadLicenses = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.licensesHandle[options.page])) {
                    options.sort = 'component,name,valid_until';
                    options.contents = '_dynamics';
                    self.licensesHandle[options.page] = api.get('licenses', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new License(guid);
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
        };
    };
});

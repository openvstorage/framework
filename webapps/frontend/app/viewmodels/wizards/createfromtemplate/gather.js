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
    'ovs/api', 'ovs/shared', 'ovs/generic',
    'viewmodels/containers/vdisk/vdisk', 'viewmodels/containers/storagerouter/storagerouter',
    'viewmodels/services/vdisk',
    './data'
], function($, ko, api, shared, generic,
            VDisk, StorageRouter,
            vdiskService,
            data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data   = data;
        self.shared = shared;

        // Handles
        self.loadStorageRoutersHandle = undefined;

        // Observables
        self.preValidateResult = ko.observable({ valid: true, reasons: [], fields: [] });

        // Computed
        self.namehelp = ko.computed(function() {
            if (data.name() === undefined || data.name() === '') {
                return $.t('ovs:wizards.create_ft.gather.no_name');
            }
            if (data.amount() === 1) {
                return $.t('ovs:wizards.create_ft.gather.amount_one', {
                    devicename: vdiskService.cleanDeviceName(data.name())
                });
            }
            var start = data.name() + '-' + data.startnr(),
                end = data.name() + '-' + (data.startnr() + data.amount() - 1);
            return $.t('ovs:wizards.create_ft.gather.amount_multiple', {
                start: start,
                devicestart: vdiskService.cleanDeviceName(start),
                end: end,
                deviceend: vdiskService.cleanDeviceName(end)
            });
        });
        self.canStart = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            if (self.data.vObject() === undefined) {
                valid = false;
                fields.push('vd');
                reasons.push($.t('ovs:wizards.create_ft.gather.no_object'));
            }
            if (self.data.storageRouters().length === 0) {
                valid = false;
                fields.push('storagerouters');
                reasons.push($.t('ovs:wizards.create_ft.gather.no_storagerouters'));
            }
            return { value: valid, reasons: reasons, fields: fields };
        });
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [], showErrors = false, data = self.canStart(),
                preValidation = self.preValidateResult();
            if (preValidation.valid === false) {
                showErrors = true;
                reasons = reasons.concat(preValidation.reasons);
                fields = fields.concat(preValidation.fields);
            }
            if (!data.value) {
                return data;
            }
            if (self.data.name() === undefined || self.data.name() === '') {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.create_ft.gather.no_name'));
            }
            if (self.data.selectedStorageRouters().length === 0) {
                valid = false;
                fields.push('storagerouters');
                reasons.push($.t('ovs:wizards.create_ft.gather.no_storagerouters_selected'));
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });

        // Functions
        self.preValidate = function() {
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                if (self.data.vObject() === undefined || self.data.vObject().vpoolGuid() === undefined) {
                    deferred.reject();
                    return;
                }
                var i, names = [];
                if (self.data.amount() === 1) {
                    names.push(self.data.name());
                } else {
                    for (i = self.data.startnr(); i < (self.data.startnr() + self.data.amount()); i++) {
                        names.push(self.data.name() + '-' + i);
                    }
                }
                api.get('vpools/' + self.data.vObject().vpoolGuid() + '/devicename_exists', { queryparams: { names: JSON.stringify(names) }})
                    .done(function(exists) {
                        if (exists) {
                            validationResult.valid = false;
                            validationResult.reasons.push($.t('ovs:wizards.create_ft.gather.name_exists'));
                            validationResult.fields.push('name');
                            self.preValidateResult(validationResult);
                            deferred.reject();
                        } else {
                            self.preValidateResult(validationResult);
                            deferred.resolve();
                        }
                    })
            }).promise();
        };
        self._create = function(name, description, storageRouter) {
            return $.Deferred(function(deferred) {
                api.post('vdisks/' + self.data.guid() + '/create_from_template', {
                        data: {
                            name: name,
                            storagerouter_guid: storageRouter.guid()
                        }
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        deferred.resolve(true);
                    })
                    .fail(function(error) {
                        error = generic.extractErrorMessage(error);
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:generic.messages.errorwhile', {what: $.t('ovs:wizards.create_ft.gather.creating', {
                                what: self.data.vObject().name(), error: error
                            })})
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var calls = [], i, max = self.data.startnr() + self.data.amount() - 1,
                    name, srcounter = 0;
                for (i = self.data.startnr(); i <= max; i += 1) {
                    name = self.data.name();
                    if (self.data.amount() > 1) {
                        name += ('-' + i.toString());
                    }
                    calls.push(self._create(name, self.data.description(), self.data.selectedStorageRouters()[srcounter]));
                    srcounter += 1;
                    if (srcounter >= self.data.selectedStorageRouters().length) {
                        srcounter = 0;
                    }
                }
                generic.alertInfo(
                    $.t('ovs:wizards.create_ft.gather.started'),
                    $.t('ovs:wizards.create_ft.gather.in_progress', { what: self.data.vObject().name() })
                );
                deferred.resolve();
                $.when.apply($, calls)
                    .done(function() {
                        var i, args = Array.prototype.slice.call(arguments),
                            success = 0;
                        for (i = 0; i < args.length; i += 1) {
                            success += (args[i] ? 1 : 0);
                        }
                        if (success === args.length) {
                        generic.alertSuccess(
                            $.t('ovs:wizards.create_ft.gather.complete'),
                            $.t('ovs:wizards.create_ft.gather.success', { what: self.data.vObject().name() })
                        );
                        } else if (success > 0) {
                        generic.alert(
                            $.t('ovs:wizards.create_ft.gather.complete'),
                            $.t('ovs:wizards.create_ft.gather.some_failed', { what: self.data.vObject().name() })
                        );
                        } else if (self.data.amount() > 2) {
                            generic.alertError(
                                $.t('ovs:generic.error'),
                                $.t('ovs:wizards.create_ft.gather.all_failed', { what: self.data.vObject().name() })
                            );
                        }
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            if (self.data.vObject() === undefined || self.data.vObject().guid() !== self.data.guid()) {
                self.data.vObject(new VDisk(self.data.guid()));
                self.data.vObject().load();
            }
            generic.xhrAbort(self.loadStorageRoutersHandle);
            self.loadStorageRoutersHandle = api.get('vdisks/' + self.data.guid() + '/get_target_storagerouters', {
                queryparams: {
                    contents: '',
                    sort: 'name'
                }
            })
                .done(function(data) {
                    var guids = [], srdata = {};
                    $.each(data.data, function(index, item) {
                        guids.push(item.guid);
                        srdata[item.guid] = item;
                    });
                    generic.crossFiller(
                        guids, self.data.storageRouters,
                        function(guid) {
                            var sr = new StorageRouter(guid);
                            sr.fillData(srdata[guid]);
                            return sr;
                        }, 'guid'
                    );
                });
        };
    };
});

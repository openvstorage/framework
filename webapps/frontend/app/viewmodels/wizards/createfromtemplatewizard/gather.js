// Copyright 2014 CloudFounders NV
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
    'ovs/api', 'ovs/shared', 'ovs/generic',
    '../../containers/vmachine', '../../containers/pmachine', './data'
], function($, ko, api, shared, generic, VMachine, PMachine, data) {
    "use strict";
    return function() {
        var self = this;

        self.data   = data;
        self.shared = shared;
        self.loadPMachinesHandle = undefined;

        self.namehelp = ko.computed(function() {
            if (data.name() === undefined || data.name() === '') {
                return $.t('ovs:wizards.createft.gather.noname');
            } else if (data.amount() === 1) {
                return $.t('ovs:wizards.createft.gather.amountone');
            }
            return $.t('ovs:wizards.createft.gather.amountmultiple', {
                start: data.name() + '-' + data.startnr(),
                end: data.name() + '-' + (data.startnr() + data.amount() - 1)
            });
        });

        self.canStart = ko.computed(function() {
            if (self.data.vm() === undefined) {
                return {value: false, reason: $.t('ovs:wizards.createft.gather.nomachine')};
            }
            if (self.data.pMachines().length === 0) {
                return {value: false, reason: $.t('ovs:wizards.createft.gather.nopmachines')};
            }
            return {value: true, reason: undefined};
        });
        self.canContinue = ko.computed(function() {
            var data = self.canStart();
            if (!data.value) {
                return data;
            }
            if (self.data.name() === undefined || self.data.name() === '') {
                return {value: false, reason: $.t('ovs:wizards.createft.gather.noname')};
            }
            if (self.data.selectedPMachines().length === 0) {
                return {value: false, reason: $.t('ovs:wizards.createft.gather.nopmachinesselected')};
            }
            return {value: true, reason: undefined};
        });

        self._create = function(name, description, pmachine) {
            return $.Deferred(function(deferred) {
                api.post('/vmachines/' + self.data.vm().guid() + '/create_from_template', {
                        pmachineguid: pmachine.guid(),
                        name: name,
                        description: description
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        deferred.resolve(true);
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:generic.messages.errorwhile', {
                                context: 'error',
                                what: $.t('ovs:wizards.createft.gather.creating', { what: self.data.vm().name() }),
                                error: error
                            })
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var calls = [], i, max = self.data.startnr() + self.data.amount() - 1,
                    name, pmachinecounter = 0;
                for (i = self.data.startnr(); i <= max; i += 1) {
                    name = self.data.name();
                    if (self.data.amount() > 1) {
                        name += ('-' + i.toString());
                    }
                    calls.push(self._create(name, self.data.description(), self.data.selectedPMachines()[pmachinecounter]));
                    pmachinecounter += 1;
                    if (pmachinecounter >= self.data.selectedPMachines().length) {
                        pmachinecounter = 0;
                    }
                }
                generic.alertInfo(
                    $.t('ovs:wizards.createft.gather.started'),
                    $.t('ovs:wizards.createft.gather.inprogress', { what: self.data.vm().name() })
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
                            $.t('ovs:wizards.createft.gather.complete'),
                            $.t('ovs:wizards.createft.gather.success', { what: self.data.vm().name() })
                        );
                        } else if (success > 0) {
                        generic.alert(
                            $.t('ovs:wizards.createft.gather.complete'),
                            $.t('ovs:wizards.createft.gather.somefailed', { what: self.data.vm().name() })
                        );
                        } else if (self.data.amount() > 2) {
                            generic.alertError(
                                $.t('ovs:generic.error'),
                                $.t('ovs:wizards.createft.gather.allfailed', { what: self.data.vm().name() })
                            );
                        }
                    });
            }).promise();
        };

        self.activate = function() {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.guid()) {
                self.data.vm(new VMachine(self.data.guid()));
                self.data.vm().load();
                self.data.selectedPMachines([]);
            }
            generic.xhrAbort(self.loadPMachinesHandle);
            self.loadPMachinesHandle = api.get('vmachines/' + self.data.guid() + '/get_target_pmachines')
                .done(function(data) {
                    var i, guids = [];
                    for (i = 0; i < data.length; i += 1) {
                        guids.push(data[i].guid);
                    }
                    generic.crossFiller(
                        guids, self.data.pMachines,
                        function(guid) {
                            var pm = new PMachine(guid);
                            pm.load();
                            return pm;
                        }, 'guid'
                    );
                });
        };
    };
});

// license see http://www.openvstorage.com/licenses/opensource/
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

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var i, pmachineguids = [];
                for (i = 0; i < self.data.selectedPMachines().length; i += 1) {
                    pmachineguids.push(self.data.selectedPMachines()[i].guid());
                }
                api.post('/vmachines/' + self.data.vm().guid() + '/create_multiple_from_template', {
                        pmachineguids: pmachineguids,
                        name: self.data.name(),
                        description: self.data.description(),
                        start: self.data.startnr(),
                        amount: self.data.amount()
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:wizards.createft.gather.complete'),
                            $.t('ovs:wizards.createft.gather.success', { what: self.data.vm().name() })
                        );
                    })
                    .fail(function() {
                        generic.alert(
                            $.t('ovs:wizards.createft.gather.complete'),
                            $.t('ovs:wizards.createft.gather.somefailed', { what: self.data.vm().name() })
                        );
                    });
                generic.alertInfo(
                    $.t('ovs:wizards.createft.gather.started'),
                    $.t('ovs:wizards.createft.gather.inprogress', { what: self.data.vm().name() })
                );
                deferred.resolve();
            }).promise();
        };

        self.activate = function() {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.guid()) {
                self.data.vm(new VMachine(self.data.guid()));
                self.data.vm().load();
            }
            generic.xhrAbort(self.loadPMachinesHandle);
            self.loadPMachinesHandle = api.get('pmachines')
                .done(function(data) {
                    var i, guids = [];
                    for (i = 0; i < data.length; i += 1) {
                        guids.push(data[i].guid);
                    }
                    generic.crossFiller(
                        guids, self.data.pMachineGuids, self.data.pMachines,
                        function(guid) {
                            var pm = new PMachine(guid);
                            pm.load();
                            return pm;
                        }
                    );
                });
        };
    };
});

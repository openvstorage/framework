define([
    'knockout',
    '../../containers/vmachine', './data'
], function(ko, VMachine, data) {
    "use strict";
    return function () {
        var self = this;

        self.data = data;

        self.can_continue = ko.computed(function () {
            if (self.data.vm() === undefined) {
                return {value: false, reason: 'No machine loaded'};
            }
            var i, disks = self.data.vm().vdisks();
            for(i = 0; i < disks.length; i += 1) {
                if (disks[i].snapshots().length === 0) {
                    return {value: false, reason: 'Not all disks have snapshots'};
                }
            }
            return {value: true, reason: undefined};
        });

        self.activate = function () {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.machineguid()) {
                self.data.vm(new VMachine(self.data.machineguid()));
                self.data.vm()
                    .load()
                    .done(function () {
                        self.data.name(self.data.vm().name() + '_clone');
                        var i, disks = self.data.vm().vdisks(),
                            loads = [];
                        for(i = 0; i < disks.length; i += 1) {
                            loads.push(disks[i].load());
                        }
                        $.when.apply($, loads)
                            .done(function () {
                                self.data.vm().vdisks.sort(function (a, b) {
                                   return a.order() - b.order();
                                });
                            });
                    });
            }
        };
    };
});
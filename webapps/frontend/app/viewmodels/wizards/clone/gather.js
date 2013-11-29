// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    '../../containers/vmachine', './data'
], function($, ko, VMachine, data) {
    "use strict";
    return function() {
        var self = this;

        self.data = data;

        self.canContinue = ko.computed(function() {
            if (self.data.vm() === undefined) {
                return {value: false, reason: $.t('ovs:wizards.clone.gather.nomachine')};
            }
            if (!self.data.name()) {
                return {value: false, reason: $.t('ovs:wizards.clone.gather.noname')};
            }
            var i, disks = self.data.vm().vDisks();
            for(i = 0; i < disks.length; i += 1) {
                if (disks[i].snapshots().length === 0) {
                    return {value: false, reason: $.t('ovs:wizards.clone.gather.nosnapshots')};
                }
            }
            return {value: true, reason: undefined};
        });

        self.activate = function() {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.machineGuid()) {
                self.data.vm(new VMachine(self.data.machineGuid()));
                self.data.vm()
                    .load()
                    .done(function() {
                        self.data.name(self.data.vm().name() + '-clone');
                        var i, disks = self.data.vm().vDisks(),
                            loads = [];
                        for(i = 0; i < disks.length; i += 1) {
                            loads.push(disks[i].load());
                        }
                        $.when.apply($, loads)
                            .done(function() {
                                self.data.vm().vDisks.sort(function(a, b) {
                                   return a.order() - b.order();
                                });
                            });
                    });
            }
        };
    };
});

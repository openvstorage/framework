// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/api', 'ovs/generic', 'ovs/shared',
    '../../containers/vmachine', './data'
], function($, ko, api, generic, shared, VMachine, data) {
    "use strict";
    return function() {
        var self = this;

        self.shared = shared;
        self.data = data;

        self.canContinue = ko.computed(function() {
            if (self.data.vm() === undefined) {
                return {value: false, reason: $.t('ovs:wizards.snapshot.gather.nomachine')};
            }
            if (!self.data.name()) {
                return {value: false, reason: $.t('ovs:wizards.snapshot.gather.noname')};
            }
            return {value: true, reason: undefined};
        });

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    name: self.data.name(),
                    consistent: self.data.isConsistent()
                };
                api.post('vmachines/' + self.data.vm().guid() + '/snapshot', data)
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.snapshot.confirm.snapshotstarted'),
                            $.t('ovs:wizards.snapshot.confirm.inprogress', { what: self.data.vm().name() })
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.snapshot.confirm.success', { what: self.data.vm().name() })
                        );
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.snapshot.confirm.failed', { what: self.data.vm().name() })
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };

        self.activate = function() {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.machineGuid()) {
                self.data.vm(new VMachine(self.data.machineGuid()));
                self.data.vm()
                    .load()
                    .done(function() {
                        self.data.name(self.data.vm().name() + '-snapshot');
                    });
            }
        };
    };
});

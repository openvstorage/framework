// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    'ovs/api', 'ovs/shared', 'ovs/generic',
    '../../containers/vmachine', './data'
], function($, ko, api, shared, generic, VMachine, data) {
    "use strict";
    return function() {
        var self = this;

        self.data   = data;
        self.shared = shared;

        self.canContinue = ko.computed(function() {
            if (self.data.vm() === undefined) {
                return {value: false, reason: $.t('ovs:wizards.rollback.gather.nomachine')};
            }
            if (self.data.vm().snapshots() === undefined || self.data.vm().snapshots().length === 0) {
                return {value: false, reason: $.t('ovs:wizards.rollback.gather.nosnapshots')};
            }
            return {value: true, reason: undefined};
        });

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var data = {
                    timestamp: self.data.snapshot().timestamp
                };
                api.post('vmachines/' + self.data.vm().guid() + '/rollback', data)
                    .then(function(taskID) {
                        generic.alertInfo(
                            $.t('ovs:wizards.rollback.gather.rollbackstarted'),
                            $.t('ovs:wizards.rollback.gather.inprogress', { what: self.data.vm().name() })
                        );
                        deferred.resolve();
                        return taskID;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:generic.finished'),
                            $.t('ovs:wizards.rollback.gather.success', { what: self.data.vm().name() })
                        );
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.rollback.gather.failed', { what: self.data.vm().name() })
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };

        self.activate = function() {
            if (self.data.vm() === undefined || self.data.vm().guid() !== self.data.machineGuid()) {
                self.data.vm(new VMachine(self.data.machineGuid()));
                self.data.vm().load();
            }
        };
    };
});

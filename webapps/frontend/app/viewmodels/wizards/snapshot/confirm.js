// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'knockout',
    './data', 'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, ko, data, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        self.shared = shared;
        self.data   = data;

        self.canContinue = ko.observable({value: true, reason: undefined});

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
                        generic.alertError(
                            $.t('ovs:generic.error'),
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
    };
});

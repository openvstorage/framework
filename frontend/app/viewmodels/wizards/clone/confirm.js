define([
    'knockout',
    './data', 'ovs/shared', 'ovs/authentication', 'ovs/generic', 'ovs/api'
], function(ko, data, shared, authentication, generic, api) {
    "use strict";
    return function () {
        var self = this;

        self.shared = shared;
        self.data = data;
        self.canContinue = ko.observable({value: true, reason: undefined});

        self.finish = function() {
            return $.Deferred(function (deferred) {
                var i,
                    data = {},
                    vm = self.data.vm(),
                    disks = vm.vDisks();
                data.disks = {};
                data.name = self.data.name();
                for (i = 0; i < disks.length; i += 1) {
                    data.disks[disks[i].guid()] = disks[i].selectedSnapshot();
                }
                api.post('vmachines/' + vm.guid() + '/clone', data)
                    .then(function (data) {
                        deferred.resolve(data);
                        generic.alertInfo('Machine ' + vm.name() + ' cloning in progress...');
                        return data;
                    })
                    .then(self.shared.tasks.wait)
                    .done(function () {
                        generic.alertSuccess('Machine ' + vm.name() + ' cloned successfully to ' + self.data.name());
                    })
                    .fail(function (error) {
                        generic.alertError('Error while cloning ' + vm.name() + ': ' + error);
                    });
            }).promise();
        };
    };
});
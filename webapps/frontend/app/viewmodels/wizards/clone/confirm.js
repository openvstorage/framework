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
    './data', 'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, ko, data, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data   = data;

        // Computed
        self.canContinue = ko.observable({value: true, reason: undefined});

        // Functions
        self._clone = function(i, vm, callData) {
            var currentData = $.extend({}, callData);
            currentData.name = self.data.amount() === 1 ? self.data.name() : self.data.name() + '-' + i;
            return $.Deferred(function(deferred) {
                api.post('vmachines/' + vm.guid() + '/clone', currentData)
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        deferred.resolve(true);
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:generic.messages.errorwhile', {
                                context: 'error',
                                what: $.t('ovs:wizards.clone.confirm.cloning', { what: vm.name() }),
                                error: error
                            })
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var i, clones = [],
                    callData = {snapshot: self.data.snapshot().timestamp},
                    vm = self.data.vm();
                for (i = 1; i <= self.data.amount(); i += 1) {
                    clones.push(self._clone(i, vm, callData));
                }
                generic.alertInfo($.t('ovs:wizards.clone.confirm.clonestarted'), $.t('ovs:wizards.clone.confirm.inprogress', { what: vm.name() }));
                deferred.resolve();
                $.when.apply($, clones)
                    .done(function() {
                        var i, args = Array.prototype.slice.call(arguments),
                            success = 0;
                        for (i = 0; i < args.length; i += 1) {
                            success += (args[i] ? 1 : 0);
                        }
                        if (success === args.length) {
                            generic.alertSuccess($.t('ovs:wizards.clone.confirm.complete'), $.t('ovs:wizards.clone.confirm.success', { what: vm.name() }));
                        } else if (success > 0) {
                            generic.alert($.t('ovs:wizards.clone.confirm.complete'), $.t('ovs:wizards.clone.confirm.somefailed', { what: vm.name() }));
                        } else if (clones.length > 2) {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:wizards.clone.confirm.allfailed', { what: vm.name() }));
                        }
                    });
            }).promise();
        };
    };
});

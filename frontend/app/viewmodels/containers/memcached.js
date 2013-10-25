define([
    'knockout',
    'ovs/generic', 'ovs/api'
], function(ko, generic, api) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.refreshHandle = undefined;
        self.refreshTimeout = undefined;

        // Obserables
        self.bytes        = ko.observable('');
        self.currItems    = ko.observable(0);
        self.totalItems   = ko.observable(0);
        self.getHits      = ko.observable(0);
        self.cmdGet       = ko.observable(0);
        self.hitRate      = ko.observable(0);
        self.bytesRead    = ko.observable('');
        self.bytesWritten = ko.observable('');
        self.uptime       = ko.observable(0);
        self.raw          = ko.observable('');

        // Functions
        self.refresh = function() {
            generic.xhrAbort(self.refreshHandle);
            self.refreshHandle = api.get('statistics/memcache')
            .done(function(data) {
                self.bytes(generic.getBytesHuman(data.bytes));
                self.currItems(data.curr_items);
                self.totalItems(data.total_items);
                self.getHits(data.get_hits);
                self.cmdGet(data.cmd_get);
                self.hitRate(data.cmd_get === 0 ? 0 : Math.round(data.get_hits / data.cmd_get * 1000) / 10);
                self.bytesRead(generic.getBytesHuman(data.bytes_read));
                self.bytesWritten(generic.getBytesHuman(data.bytes_written));
                self.uptime(data.uptime);

                var raw_string = '', attribute;
                for (attribute in data) {
                    if (data.hasOwnProperty(attribute)) {
                        raw_string += generic.padRight(attribute, ' ', 25) + data[attribute].toString() + '\n';
                    }
                }
                self.raw(raw_string);
            });
        };
    };
});
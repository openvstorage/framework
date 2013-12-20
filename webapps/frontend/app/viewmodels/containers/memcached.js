// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
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

        self.ovsDRhit   = ko.observable(0);
        self.ovsDRtot   = ko.observable(0);
        self.ovsDRrat   = ko.observable(0);
        self.ovsDRspeed = ko.deltaObservable(generic.formatShort);
        self.ovsOLhit   = ko.observable(0);
        self.ovsOLtot   = ko.observable(0);
        self.ovsOLrat   = ko.observable(0);
        self.ovsOLspeed = ko.deltaObservable(generic.formatShort);
        self.ovsDLhit   = ko.observable(0);
        self.ovsDLtot   = ko.observable(0);
        self.ovsDLrat   = ko.observable(0);
        self.ovsDLspeed = ko.deltaObservable(generic.formatShort);
        self.ovsRLhit   = ko.observable(0);
        self.ovsRLtot   = ko.observable(0);
        self.ovsRLrat   = ko.observable(0);
        self.ovsRLspeed = ko.deltaObservable(generic.formatShort);

        // Functions
        self.refresh = function() {
            generic.xhrAbort(self.refreshHandle);
            self.refreshHandle = api.get('statistics/memcache')
            .done(function(data) {
                self.bytes(generic.formatBytes(data.bytes));
                self.currItems(data.curr_items);
                self.totalItems(data.total_items);
                self.getHits(data.get_hits);
                self.cmdGet(data.cmd_get);
                self.hitRate(data.cmd_get === 0 ? 100 : generic.round(data.get_hits / data.cmd_get * 100, 2));
                self.bytesRead(generic.formatBytes(data.bytes_read));
                self.bytesWritten(generic.formatBytes(data.bytes_written));
                self.uptime(data.uptime);

                self.ovsDRhit(data.ovs_dal.descriptor_hit);
                self.ovsDRtot(data.ovs_dal.descriptor_hit + data.ovs_dal.descriptor_miss);
                self.ovsDRrat(self.ovsDRtot() === 0 ? 100 : generic.round(self.ovsDRhit() / self.ovsDRtot() * 100, 2));
                self.ovsDRspeed(self.ovsDRtot());
                self.ovsOLhit(data.ovs_dal.object_load_hit);
                self.ovsOLtot(data.ovs_dal.object_load_hit + data.ovs_dal.object_load_miss);
                self.ovsOLrat(self.ovsOLtot() === 0 ? 100 : generic.round(self.ovsOLhit() / self.ovsOLtot() * 100, 2));
                self.ovsOLspeed(self.ovsOLtot());
                self.ovsDLhit(data.ovs_dal.datalist_hit);
                self.ovsDLtot(data.ovs_dal.datalist_hit + data.ovs_dal.datalist_miss);
                self.ovsDLrat(self.ovsDLtot() === 0 ? 100 : generic.round(self.ovsDLhit() / self.ovsDLtot() * 100, 2));
                self.ovsDLspeed(self.ovsDLtot());
                self.ovsRLhit(data.ovs_dal.relations_hit);
                self.ovsRLtot(data.ovs_dal.relations_hit + data.ovs_dal.relations_miss);
                self.ovsRLrat(self.ovsRLtot() === 0 ? 100 : generic.round(self.ovsRLhit() / self.ovsRLtot() * 100, 2));
                self.ovsRLspeed(self.ovsRLtot());

                var rawString = '', attribute;
                for (attribute in data) {
                    if (data.hasOwnProperty(attribute)) {
                        rawString += generic.padRight(attribute, ' ', 25) + data[attribute].toString() + '\n';
                    }
                }
                self.raw(rawString);
            });
        };
    };
});

define(['jquery', 'knockout', 'ovs/generic', 'ovs/authentication'], function ($, ko, generic, authentication) {
    "use strict";

    return function () {
        var self = this;
        // Variables
        self.refresh_handle = undefined;
        self.refresh_timeout = undefined;

        // Obserables
        self.bytes         = ko.observable('');
        self.curr_items    = ko.observable(0);
        self.total_items   = ko.observable(0);
        self.get_hits      = ko.observable(0);
        self.cmd_get       = ko.observable(0);
        self.hit_rate      = ko.observable(0);
        self.bytes_read    = ko.observable('');
        self.bytes_written = ko.observable('');
        self.uptime        = ko.observable(0);
        self.raw           = ko.observable('');

        // Functions
        self.refresh = function () {
            if (self.refresh_handle !== undefined) {
                self.refresh_handle.abort();
            }
            self.refresh_handle = $.ajax('/api/internal/statistics/memcache/?timestamp=' + generic.gettimestamp(), {
                type: 'get',
                contentType: 'application/json',
                headers: {
                    'Authorization': authentication.header()
                }
            })
            .done(function (data) {
                self.bytes(generic.get_bytes_human(data.bytes));
                self.curr_items(data.curr_items);
                self.total_items(data.total_items);
                self.get_hits(data.get_hits);
                self.cmd_get(data.cmd_get);
                self.hit_rate(data.cmd_get === 0 ? 0 : Math.round(data.get_hits / data.cmd_get * 1000) / 10);
                self.bytes_read(generic.get_bytes_human(data.bytes_read));
                self.bytes_written(generic.get_bytes_human(data.bytes_written));
                self.uptime(data.uptime);

                var raw_string = '';
                for (var attribute in data) {
                    if (data.hasOwnProperty(attribute)) {
                        raw_string += generic.padright(attribute, ' ', 25) + data[attribute].toString() + '\n';
                    }
                }
                self.raw(raw_string);
            });
        };
    };
});
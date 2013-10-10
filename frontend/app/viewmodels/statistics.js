define(['plugins/router', 'durandal/app', 'models/memcached'], function (router, app, Memcached) {
    "use strict";
    var Internal = function () {
        var self = this;
        // Variables
    };
    var viewModel = {
        // Data
        displayname: 'Statistics',
        description: 'The page contains various system statistics',
        memcached: new Memcached(),

        // Internal management
        refresh_timeout: undefined,

        // Functions
        refresh: function () {
            viewModel.memcached.refresh();
        },
        start_refresh: function () {
            viewModel.refresh_timeout = window.setInterval(function () {
                viewModel.refresh();
            }, 1000);
        },
        stop_refresh: function () {
            if (viewModel.refresh_timeout !== undefined) {
                window.clearInterval(viewModel.refresh_timeout);
            }
        },

        // Durandal
        activate: function () {
            app.trigger('statistics:refresh');
            app.trigger('statistics:start_refresh');
        },
        deactivate: function () {
            app.trigger('statistics:stop_refresh');
        }
    };
    app.on('statistics:start_refresh', viewModel.start_refresh);
    app.on('statistics:stop_refresh', viewModel.stop_refresh);
    app.on('statistics:refresh', viewModel.refresh);
    return viewModel;
});
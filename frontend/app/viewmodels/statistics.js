define(['plugins/router', 'durandal/app', 'models/memcached'], function (router, app, Memcached) {
    "use strict";
    var viewModel = {
        displayname: 'Statistics',
        description: 'The page contains various system statistics',
        memcached: new Memcached(),
        activate: function () {
            app.trigger('statistics_memcache:start_refresh');
        },
        deactivate: function() {
            app.trigger('statistics_memcache:stop_refresh');
        }
    };
    app.on('statistics_memcache:start_refresh', viewModel.memcached.start_refresh);
    app.on('statistics_memcache:stop_refresh', viewModel.memcached.stop_refresh);
    return viewModel;
});
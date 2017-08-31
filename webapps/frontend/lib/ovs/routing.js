// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define */
define(['jquery'], function($) {
    "use strict";
    var mainRoutes, siteRoutes, buildSiteRoutes, loadHash, extraRoutes, routePatches;

    mainRoutes = [
       { route: '',              moduleId: 'viewmodels/redirect', nav: false },
       { route: ':mode*details', moduleId: 'viewmodels/index',    nav: false }
    ];
    siteRoutes = [
        { route: '',                     moduleId: 'dashboard/dashboard',                   title: $.t('ovs:dashboard.title'),             titlecode: 'ovs:dashboard.title',             nav: false, main: false },
        { route: 'storagerouters',       moduleId: 'storagerouter/storagerouters',          title: $.t('ovs:storagerouters.title'),        titlecode: 'ovs:storagerouters.title',        nav: true,  main: true  },
        { route: 'storagerouters/:guid', moduleId: 'storagerouter/storagerouter-detail',    title: $.t('ovs:storagerouters.detail.title'), titlecode: 'ovs:storagerouters.detail.title', nav: false, main: false },
        { route: 'vpools',               moduleId: 'vpool/vpools',                          title: $.t('ovs:vpools.title'),                titlecode: 'ovs:vpools.title',                nav: true,  main: true  },
        { route: 'vpool/:guid',          moduleId: 'vpool/vpool-detail',                    title: $.t('ovs:vpools.detail.title'),         titlecode: 'ovs:vpools.detail.title',         nav: false, main: false },
        { route: 'vdisks',               moduleId: 'vdisk/vdisks',                          title: $.t('ovs:vdisks.title'),                titlecode: 'ovs:vdisks.title',                nav: true,  main: true  },
        { route: 'vdisk/:guid',          moduleId: 'vdisk/vdisk-detail',                    title: $.t('ovs:vdisks.detail.title'),         titlecode: 'ovs:vdisks.detail.title',         nav: false, main: false },
        { route: 'vtemplates',           moduleId: 'vtemplates/vtemplates',                 title: $.t('ovs:vtemplates.title'),            titlecode: 'ovs:vtemplates.title',            nav: true,  main: true  },
        { route: 'login',                moduleId: 'login/login',                           title: $.t('ovs:login.title'),                 titlecode: 'ovs:login.title',                 nav: false, main: false },
        { route: 'domains',              moduleId: 'admin/domains',                        title: $.t('ovs:domains.title'),               titlecode: 'ovs:domains.title',               nav: true,  main: false },
        { route: 'users',                moduleId: 'admin/users',                            title: $.t('ovs:users.title'),                 titlecode: 'ovs:users.title',                 nav: true,  main: false },
        { route: 'support',              moduleId: 'admin/support',                       title: $.t('ovs:support.title'),               titlecode: 'ovs:support.title',               nav: true,  main: false },
        { route: 'updates',              moduleId: 'admin/updates',                       title: $.t('ovs:updates.title'),               titlecode: 'ovs:updates.title',               nav: true,  main: false }
    ];
    extraRoutes = [];
    routePatches = [];

    buildSiteRoutes = function(mode) {
        var i, j, k;
        for (i = 0; i < extraRoutes.length; i += 1) {
            for (j = 0; j < extraRoutes[i].length; j += 1) {
                siteRoutes.push(extraRoutes[i][j]);
            }
        }
        for (i = 0; i < siteRoutes.length; i += 1) {
            siteRoutes[i].hash = '#' + mode + '/' + siteRoutes[i].route;
        }
        for (i = 0; i < routePatches.length; i += 1) {
            for (j = 0; j < routePatches[i].length; j += 1) {
                for (k = 0; k < siteRoutes.length; k += 1) {
                    if (siteRoutes[k].moduleId === routePatches[i][j].moduleId) {
                        siteRoutes[k].nav = routePatches[i][j].nav;
                        siteRoutes[k].main = routePatches[i][j].main;
                    }
                }
            }
        }
    };
    loadHash = function(module, params) {
        var i, item, hash;
        params = params || {};
        module = '/' + module;
        for (i = 0; i < siteRoutes.length; i += 1) {
            if (siteRoutes[i].moduleId.indexOf(module, siteRoutes[i].moduleId.length - module.length) !== -1) {
                hash = siteRoutes[i].hash;
                for (item in params) {
                    if (params.hasOwnProperty(item)) {
                        hash = hash.replace(':' + item, params[item].call ? params[item]() : params[item]);
                    }
                }
                if (hash.indexOf(':') === -1) {
                    return hash;
                }
            }
        }
        return '/';
    };

    return {
        mainRoutes: mainRoutes,
        siteRoutes: siteRoutes,
        extraRoutes: extraRoutes,
        routePatches: routePatches,
        buildSiteRoutes: buildSiteRoutes,
        loadHash: loadHash
    };
});

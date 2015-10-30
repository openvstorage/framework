// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define(['jquery'], function($) {
    "use strict";
    var mainRoutes, siteRoutes, buildSiteRoutes, loadHash, extraRoutes, routePatches;

    mainRoutes = [
       { route: '',              moduleId: 'viewmodels/redirect', nav: false },
       { route: ':mode*details', moduleId: 'viewmodels/index',    nav: false }
    ];
    siteRoutes = [
        { route: '',                     moduleId: 'dashboard',            title: $.t('ovs:dashboard.title'),             titlecode: 'ovs:dashboard.title',             nav: false, main: false },
        { route: 'storagerouters',       moduleId: 'storagerouters',       title: $.t('ovs:storagerouters.title'),        titlecode: 'ovs:storagerouters.title',        nav: true,  main: true  },
        { route: 'storagerouters/:guid', moduleId: 'storagerouter-detail', title: $.t('ovs:storagerouters.detail.title'), titlecode: 'ovs:storagerouters.detail.title', nav: false, main: false },
        { route: 'vpools',               moduleId: 'vpools',               title: $.t('ovs:vpools.title'),                titlecode: 'ovs:vpools.title',                nav: true,  main: true  },
        { route: 'vpool/:guid',          moduleId: 'vpool-detail',         title: $.t('ovs:vpools.detail.title'),         titlecode: 'ovs:vpools.detail.title',         nav: false, main: false },
        { route: 'vmachines',            moduleId: 'vmachines',            title: $.t('ovs:vmachines.title'),             titlecode: 'ovs:vmachines.title',             nav: true,  main: true  },
        { route: 'vmachine/:guid',       moduleId: 'vmachine-detail',      title: $.t('ovs:vmachines.detail.title'),      titlecode: 'ovs:vmachines.detail.title',      nav: false, main: false },
        { route: 'vdisks',               moduleId: 'vdisks',               title: $.t('ovs:vdisks.title'),                titlecode: 'ovs:vdisks.title',                nav: true,  main: true  },
        { route: 'vdisk/:guid',          moduleId: 'vdisk-detail',         title: $.t('ovs:vdisks.detail.title'),         titlecode: 'ovs:vdisks.detail.title',         nav: false, main: false },
        { route: 'vtemplates',           moduleId: 'vtemplates',           title: $.t('ovs:vtemplates.title'),            titlecode: 'ovs:vtemplates.title',            nav: true,  main: true  },
        { route: 'login',                moduleId: 'login',                title: $.t('ovs:login.title'),                 titlecode: 'ovs:login.title',                 nav: false, main: false },
        { route: 'about',                moduleId: 'about',                title: $.t('ovs:about.title'),                 titlecode: 'ovs:about.title',                 nav: true,  main: false },
        { route: 'hmc',                  moduleId: 'pmachines',            title: $.t('ovs:pmachines.title'),             titlecode: 'ovs:pmachines.title',             nav: true,  main: false },
        { route: 'users',                moduleId: 'users',                title: $.t('ovs:users.title'),                 titlecode: 'ovs:users.title',                 nav: true,  main: false },
        { route: 'statistics',           moduleId: 'statistics',           title: $.t('ovs:statistics.title'),            titlecode: 'ovs:statistics.title',            nav: true,  main: false },
        { route: 'support',              moduleId: 'support',              title: $.t('ovs:support.title'),               titlecode: 'ovs:support.title',               nav: true,  main: false },
        { route: 'licenses',             moduleId: 'licenses',             title: $.t('ovs:licenses.title'),              titlecode: 'ovs:licenses.title',              nav: true,  main: false },
        { route: 'updates',              moduleId: 'updates',              title: $.t('ovs:updates.title'),               titlecode: 'ovs:updates.title',               nav: true,  main: false }
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
